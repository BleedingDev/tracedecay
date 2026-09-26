"""Unit checks for the bounded production NCM stress campaign."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import stat
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "product" / "ncm" / "production_stress.py"
SPEC = importlib.util.spec_from_file_location("ncm_production_stress", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
STRESS = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = STRESS
SPEC.loader.exec_module(STRESS)


class ProductionStressCampaignTest(unittest.TestCase):
    def test_default_campaign_is_four_bounded_25_trial_cells(self) -> None:
        config = STRESS.CampaignConfig()
        config.validate()
        self.assertEqual(
            config.counts,
            {
                "cold": 25,
                "warm": 25,
                "restart": 25,
                "concurrent": 25,
            },
        )
        self.assertEqual(config.planned_trials, 100)

    def test_campaign_rejects_unbounded_or_non_concurrent_configuration(self) -> None:
        with self.assertRaises(STRESS.StressFailure):
            STRESS.CampaignConfig(cold_trials=101).validate()
        with self.assertRaises(STRESS.StressFailure):
            STRESS.CampaignConfig(
                cold_trials=100,
                warm_trials=100,
                restart_trials=100,
                concurrent_trials=101,
            ).validate()
        with self.assertRaises(STRESS.StressFailure):
            STRESS.CampaignConfig(concurrency=1).validate()

    def test_invalid_configuration_receipt_contains_terminal_rows_for_every_phase(
        self,
    ) -> None:
        config = STRESS.CampaignConfig(
            cold_trials=0,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "receipt.json"
            result = STRESS.run_campaign(
                ROOT,
                Path(directory) / "worker",
                Path(directory) / "model",
                Path(directory) / "manifest",
                output,
                Path(directory) / "state",
                config,
            )
            receipt = json.loads(output.read_text())
        self.assertEqual(result, 2)
        self.assertEqual(receipt["status"], "blocked")
        self.assertEqual(set(receipt["phases"]), set(STRESS.PHASES))
        self.assertTrue(
            all(
                trial["status"] == "incomplete"
                for phase in receipt["phases"].values()
                for trial in phase["trials"]
            )
        )
        self.assertTrue(all(phase["trials"] for phase in receipt["phases"].values()))

    def test_concurrent_phase_uses_one_supported_owner_and_worker(self) -> None:
        topology = STRESS.concurrent_topology(4)
        self.assertEqual(topology["client_callers"], 4)
        self.assertEqual(topology["dispatch"], "bounded_single_owner_mailbox")
        self.assertEqual(topology["mailbox_capacity"], 32)

        worker = mock.Mock(timeout_ms=1000)
        worker.operations = []
        worker.process = mock.Mock(pid=12345)
        worker.process.poll.return_value = None
        worker.remaining_seconds.return_value = None
        barrier = threading.Barrier(4)
        caller_threads: set[int] = set()
        owner_threads: set[int] = set()
        namespace_id = STRESS.wire_namespace("ncm-production-stress/concurrent")

        def action(owner_worker: object) -> tuple[int, int]:
            owner_threads.add(threading.get_ident())
            worker.operations.append({"op": "recall", "wire_namespace": namespace_id})
            return threading.get_ident(), id(owner_worker)

        with STRESS.SingleOwnerDispatcher(worker) as owner:
            owner.begin_batch(4)

            def client_call() -> tuple[int, tuple[int, int]]:
                caller_id = threading.get_ident()
                caller_threads.add(caller_id)
                barrier.wait()
                return caller_id, owner.call(action, batch=True)

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(client_call) for _ in range(4)]
                results = [future.result() for future in futures]
            stats = owner.stats()

        self.assertEqual(len(owner_threads), 1)
        self.assertTrue(owner_threads.isdisjoint(caller_threads))
        self.assertEqual(
            {owner_thread for _caller, (owner_thread, _worker) in results},
            owner_threads,
        )
        self.assertEqual(
            {_worker for _caller, (_owner_thread, _worker) in results},
            {id(worker)},
        )
        self.assertEqual(stats["owner_threads"], 1)
        self.assertEqual(stats["owner_threads"], len(stats["owner_thread_ids"]))
        self.assertEqual(stats["worker_processes"], 1)
        self.assertEqual(stats["worker_pids"], [12345])
        self.assertTrue(stats["worker_alive"])
        self.assertTrue(stats["owner_alive"])
        self.assertEqual(stats["batch_expected"], 4)
        self.assertEqual(stats["batch_submitted"], 4)
        self.assertEqual(stats["batch_client_callers"], 4)
        self.assertEqual(len(stats["batch_caller_thread_ids"]), 4)
        self.assertTrue(set(stats["batch_caller_thread_ids"]).isdisjoint(owner_threads))
        self.assertTrue(stats["batch_released"])
        self.assertTrue(stats["client_overlap"])
        self.assertGreaterEqual(stats["max_queue_depth"], 2)
        self.assertEqual(
            stats["namespace_owner_bindings"],
            [
                {
                    "namespace_id": namespace_id,
                    "owner_thread_ids": sorted(owner_threads),
                    "worker_pids": [12345],
                }
            ],
        )

    def test_dispatcher_shutdown_is_bounded_when_owner_action_stalls(self) -> None:
        worker = mock.Mock(timeout_ms=120_000)
        worker.remaining_seconds.return_value = 120.0
        started = threading.Event()
        release = threading.Event()

        def stalled_action(_worker: object) -> None:
            started.set()
            release.wait(timeout=30.0)

        owner = STRESS.SingleOwnerDispatcher(worker)
        result: list[BaseException | None] = []
        call_thread = threading.Thread(
            target=lambda: self._capture_exception(
                result, lambda: owner.call(stalled_action)
            ),
            daemon=True,
        )
        call_thread.start()
        self.assertTrue(started.wait(timeout=1.0))
        began = time.monotonic()
        with self.assertRaises(STRESS.DeadlineExceeded):
            owner.close()
        self.assertLess(time.monotonic() - began, 3.0)
        release.set()
        call_thread.join(timeout=2.0)
        owner._thread.join(timeout=2.0)

    @staticmethod
    def _capture_exception(result: list[BaseException | None], action: object) -> None:
        try:
            action()  # type: ignore[operator]
        except BaseException as error:
            result.append(error)
        else:
            result.append(None)

    def test_seed_and_namespace_derivation_are_reproducible_and_disjoint(self) -> None:
        first = [
            STRESS.phase_seed(77, phase, index)
            for phase in STRESS.PHASES
            for index in range(3)
        ]
        second = [
            STRESS.phase_seed(77, phase, index)
            for phase in STRESS.PHASES
            for index in range(3)
        ]
        self.assertEqual(first, second)
        self.assertEqual(len(first), len(set(first)))
        names = [
            STRESS.trial_namespace(phase, index, seed)
            for phase, seed in zip(STRESS.PHASES, first[:4])
            for index in [0]
        ]
        self.assertEqual(
            len(names), len(set(STRESS.wire_namespace(name) for name in names))
        )
        self.assertTrue(STRESS.HEX_64.fullmatch(STRESS.wire_namespace(names[0])))

    def test_observation_payload_digest_matches_effect_fields(self) -> None:
        payload = STRESS.observation_payload(
            phase="warm", trial_index=2, seed=99, ordinal=1
        )
        effect = {
            key: payload[key]
            for key in (
                "source",
                "key_text",
                "value_text",
                "affect",
                "surprise",
                "intensity",
                "provenance",
            )
        }
        self.assertEqual(
            payload["payload_sha256"],
            STRESS.sha256_bytes(STRESS.stable_json_bytes(effect, sort_keys=False)),
        )
        self.assertNotEqual(payload["key_text"], payload["value_text"])
        self.assertIn("durable-ncm-marker", payload["value_text"])

    def test_cancellation_payload_is_a_bounded_real_workload(self) -> None:
        payload = STRESS.cancellation_payload(seed=99)
        self.assertEqual(
            len(payload["key_text"].encode("utf-8")),
            STRESS.CANCELLATION_WORKLOAD_BYTES,
        )
        self.assertEqual(
            len(payload["value_text"].encode("utf-8")),
            STRESS.CANCELLATION_WORKLOAD_BYTES,
        )
        effect = {
            key: payload[key]
            for key in (
                "source",
                "key_text",
                "value_text",
                "affect",
                "surprise",
                "intensity",
                "provenance",
            )
        }
        self.assertEqual(
            payload["payload_sha256"],
            STRESS.sha256_bytes(STRESS.stable_json_bytes(effect, sort_keys=False)),
        )
        self.assertLessEqual(
            len(STRESS.stable_json_bytes(payload)), STRESS.MAX_REQUEST_BYTES
        )

    def test_cancellation_interrupt_retains_completed_first_session(self) -> None:
        config = STRESS.CampaignConfig(
            cold_trials=1,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
        )
        first_value = {"operation": {"outcome": "deadline_exceeded"}}
        first_session = {
            "pid": 123,
            "operations": [{"op": "observe", "outcome": "deadline_exceeded"}],
        }
        with (
            mock.patch.object(
                STRESS,
                "prepare_trial_state_root",
                return_value=Path("/probe"),
            ),
            mock.patch.object(
                STRESS,
                "run_session",
                side_effect=[(first_value, first_session), STRESS.CampaignTerminated()],
            ),
        ):
            with self.assertRaises(STRESS.SessionInterrupted) as caught:
                STRESS.run_real_cancellation_probe(
                    Path("/worker"),
                    Path("/campaign"),
                    config,
                    admitted_identity={},
                    campaign_deadline=time.monotonic() + 10.0,
                )
        evidence = caught.exception.evidence
        self.assertEqual(evidence["first"], first_value)
        self.assertEqual(evidence["sessions"], [first_session])
        self.assertEqual(evidence["error"]["error_code"], "campaign_terminated")

    def test_reply_parser_rejects_duplicates_and_outcome_handles_rust_variants(
        self,
    ) -> None:
        self.assertEqual(
            STRESS.outcome_name({"outcome": {"Rejected": {"reason": "x"}}}),
            "rejected",
        )
        self.assertEqual(STRESS.outcome_name({"outcome": "Success"}), "success")
        with self.assertRaises(STRESS.StressFailure):
            STRESS.parse_reply(b'{"id":1,"id":2}')

    def test_candidate_extraction_reads_externally_tagged_payload(self) -> None:
        candidate = {"record_id": 42, "value_text": "durable"}
        payload = {
            "Candidates": {
                "candidates": [candidate],
                "truncated": False,
                "margin_satisfied": True,
            }
        }
        self.assertEqual(STRESS.candidate_list(payload), [candidate])
        self.assertEqual(STRESS.candidate_list({"Empty": {}}), [])
        with self.assertRaises(STRESS.StressFailure):
            STRESS.candidate_list({"Candidates": {"candidates": [candidate, 1]}})

    def test_candidate_scope_rejects_foreign_source_and_namespace_ids(self) -> None:
        namespace = "ncm-production-stress/cold/0/7"
        source = "ncm-production-stress/cold/0/0"
        candidate = {
            "record_id": 1,
            "source": source,
            "namespace_id": STRESS.wire_namespace(namespace),
        }
        evidence = STRESS.validate_candidate_scope(
            [candidate],
            namespace=namespace,
            expected_source_id=source,
            label="scope",
        )
        self.assertEqual(evidence["source_ids"], [source])
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_candidate_scope(
                [{**candidate, "source": "foreign-source"}],
                namespace=namespace,
                expected_source_id=source,
                label="scope",
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_candidate_scope(
                [{**candidate, "namespace_id": STRESS.wire_namespace("foreign")}],
                namespace=namespace,
                expected_source_id=source,
                label="scope",
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_candidate_scope(
                [{**candidate, "namespace": "foreign"}],
                namespace=namespace,
                expected_source_id=source,
                label="scope",
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_candidate_scope(
                [
                    {
                        **candidate,
                        "namespace": namespace,
                        "namespace_id": STRESS.wire_namespace("foreign"),
                    }
                ],
                namespace=namespace,
                expected_source_id=source,
                label="scope",
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_candidate_scope(
                [{**candidate, "namespace": None}],
                namespace=namespace,
                expected_source_id=source,
                label="scope",
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_candidate_scope(
                [{**candidate, "source": None}],
                namespace=namespace,
                expected_source_id=source,
                label="scope",
            )

    def test_identity_validation_rejects_stale_model_and_double(self) -> None:
        files = [
            {
                "path": path,
                "sha256": ("a" if ordinal == 0 else "b") * 64,
                "bytes": ordinal + 1,
            }
            for ordinal, path in enumerate(STRESS.MODEL_REQUIRED_FILES)
        ]
        encoder = {
            "model": STRESS.MODEL_NAME,
            "artifact_sha256": files[0]["sha256"],
            "repository": STRESS.MODEL_REPOSITORY,
            "revision": STRESS.MODEL_REVISION,
            "revision_provenance": STRESS.MODEL_REVISION_PROVENANCE,
            "files": files,
            "max_length": STRESS.REAL_MODEL_MAX_LENGTH,
            "pooling": STRESS.MODEL_POOLING,
            "normalize": STRESS.MODEL_NORMALIZE,
        }
        worker = {
            "sha256": "c" * 64,
            "bytes": 123,
            "target": {
                "triple": "aarch64-apple-darwin",
                "os": "macos",
                "arch": "aarch64",
                "family": "unix",
            },
        }
        algorithm = {
            "profile": STRESS.ALGORITHM_PROFILE,
            "config_sha256": STRESS.ALGORITHM_CONFIG_SHA256,
        }
        model = {
            "model": STRESS.MODEL_NAME,
            "artifact_sha256": files[0]["sha256"],
            "repository": STRESS.MODEL_REPOSITORY,
            "revision": STRESS.MODEL_REVISION,
            "revision_provenance": STRESS.MODEL_REVISION_PROVENANCE,
            "files": files,
            "manifest_sha256": "d" * 64,
            "max_length": STRESS.REAL_MODEL_MAX_LENGTH,
            "pooling": STRESS.MODEL_POOLING,
            "normalize": STRESS.MODEL_NORMALIZE,
        }
        handshake = {
            "identity_revision": STRESS.IDENTITY_REVISION_V2,
            "ready": True,
            "projection_sha256": "e" * 64,
            "epoch": 0,
            "model": STRESS.MODEL_NAME,
            "artifact_sha256": files[0]["sha256"],
            "algorithm": algorithm,
            "worker": worker,
            "encoder": encoder,
        }
        health = {
            "process_alive": True,
            "encoder_ready": True,
            "encoder": {
                "model": STRESS.MODEL_NAME,
                "artifact_sha256": files[0]["sha256"],
                "max_length": STRESS.REAL_MODEL_MAX_LENGTH,
            },
            "v2_identity": {
                "identity_revision": STRESS.IDENTITY_REVISION_V2,
                "ready": True,
                "model": STRESS.MODEL_NAME,
                "artifact_sha256": files[0]["sha256"],
                "config_sha256": STRESS.ALGORITHM_CONFIG_SHA256,
                "projection_sha256": "e" * 64,
                "epoch": 0,
                "algorithm": algorithm,
                "worker": worker,
                "encoder": encoder,
            },
        }
        result = STRESS.validate_production_identity(model, handshake, health)
        self.assertTrue(result["real_encoder"])
        self.assertFalse(result["test_double"])
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_production_identity(
                model,
                {**handshake, "model": "old-model"},
                health,
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_production_identity(
                model,
                {
                    **handshake,
                    "encoder": {
                        **encoder,
                        "model": STRESS.MODEL_ARTIFACT_MARKER,
                        "artifact_sha256": STRESS.MODEL_ARTIFACT_MARKER,
                    },
                },
                health,
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_production_identity(
                model, handshake, {"process_alive": True}
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_production_identity(
                model,
                handshake,
                {key: value for key, value in health.items() if key != "v2_identity"},
            )
        stale_files = [dict(item) for item in files]
        stale_files[1]["sha256"] = "f" * 64
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_production_identity(
                model,
                {**handshake, "encoder": {**encoder, "files": stale_files}},
                health,
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_production_identity(
                model,
                {**handshake, "encoder": {**encoder, "revision": "old"}},
                health,
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_production_identity(
                model,
                {key: value for key, value in handshake.items() if key != "ready"},
                health,
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_production_identity(
                model,
                handshake,
                {
                    **health,
                    "v2_identity": {
                        **health["v2_identity"],
                        "projection_sha256": "f" * 64,
                    },
                },
            )

    def test_real_encoder_markers_are_required_during_admission(self) -> None:
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_real_encoder_markers(
                {"required_markers": {"fastembed": True, "onnxruntime": False}}
            )
        with self.assertRaises(STRESS.StressFailure):
            STRESS.validate_real_encoder_markers({"required_markers": {}})
        STRESS.validate_real_encoder_markers(
            {"required_markers": {"fastembed": True, "onnxruntime": True}}
        )

    def test_preflight_closes_staged_worker_when_marker_admission_fails(self) -> None:
        class FakeArtifact(dict[str, object]):
            def __init__(self) -> None:
                super().__init__(
                    required_markers={"fastembed": True, "onnxruntime": False}
                )
                self.closed = False

            def close(self) -> None:
                self.closed = True

        artifact = FakeArtifact()
        checker = mock.Mock()
        checker.verify_worker_artifact.return_value = artifact
        config = STRESS.CampaignConfig(
            cold_trials=1,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
        )
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(STRESS, "_load_checker", return_value=checker):
                with self.assertRaises(STRESS.StressFailure):
                    STRESS.preflight(
                        ROOT,
                        Path("/worker"),
                        Path(directory) / "model",
                        Path(directory) / "manifest",
                        Path(directory) / "campaign",
                        config,
                    )
        self.assertTrue(artifact.closed)

    def test_required_record_miss_accounting_is_typed_and_recursive(self) -> None:
        error = STRESS.RequiredRecordMiss("required record was absent")
        self.assertTrue(STRESS.contains_required_record_miss(error))
        self.assertTrue(
            STRESS.contains_required_record_miss(
                {"nested": [{"error_code": "required_record_miss"}]}
            )
        )
        wrapper = RuntimeError("concurrent wrapper")
        wrapper.failures = [error]
        self.assertTrue(STRESS.contains_required_record_miss(wrapper))
        self.assertFalse(STRESS.contains_required_record_miss("required_record_miss"))
        config = STRESS.CampaignConfig(
            cold_trials=1,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
        )
        receipt = {
            "phases": {
                "cold": {
                    "trials": [
                        {
                            "status": "failed",
                            "error": "opaque",
                            "evidence": {
                                "concurrent_failures": [
                                    {
                                        "evidence": {
                                            "error": {
                                                "error_code": "required_record_miss"
                                            }
                                        }
                                    }
                                ]
                            },
                        }
                    ]
                }
            },
            "failures": [],
        }
        STRESS.finalize_receipt(receipt, config)
        self.assertEqual(receipt["counts"]["required_record_misses"], 1)

    def test_session_interrupt_closes_worker_and_retains_evidence(self) -> None:
        worker = mock.Mock()
        worker.operations = []
        worker.process = None
        worker.close.return_value = {"returncode": 0}

        def interrupt(_worker: object) -> None:
            raise KeyboardInterrupt("injected interrupt")

        deadline = time.monotonic() + 5.0
        with mock.patch.object(STRESS, "WorkerProcess", return_value=worker) as started:
            with self.assertRaises(STRESS.SessionInterrupted) as caught:
                STRESS.run_session(
                    Path("/worker"),
                    Path("/state"),
                    timeout_ms=1000,
                    action=interrupt,
                    deadline=deadline,
                )
        self.assertEqual(started.call_args.kwargs["deadline"], deadline)
        worker.close.assert_called_once_with(
            suppress_errors=True,
            timeout_seconds=STRESS.SESSION_CLEANUP_TIMEOUT_SECONDS,
        )
        self.assertEqual(
            caught.exception.evidence["error"]["error_type"], "KeyboardInterrupt"
        )

    def test_successful_session_uses_bounded_worker_cleanup(self) -> None:
        worker = mock.Mock()
        worker.operations = []
        worker.process = None
        worker.close.return_value = {"returncode": 0}

        with mock.patch.object(STRESS, "WorkerProcess", return_value=worker):
            value, evidence = STRESS.run_session(
                Path("/worker"),
                Path("/state"),
                timeout_ms=1000,
                action=lambda _worker: {"ok": True},
                deadline=time.monotonic() + 5.0,
            )

        self.assertEqual(value, {"ok": True})
        self.assertEqual(evidence["close"], {"returncode": 0})
        worker.close.assert_called_once_with(
            suppress_errors=False,
            timeout_seconds=STRESS.SESSION_CLEANUP_TIMEOUT_SECONDS,
        )

    def test_trial_overrun_keeps_partial_result_and_deadline(self) -> None:
        config = STRESS.CampaignConfig(
            cold_trials=1,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
            trial_timeout_seconds=0.1,
            campaign_timeout_seconds=1.0,
        )

        def slow_runner(*_args: object, **_kwargs: object) -> dict[str, object]:
            time.sleep(0.12)
            return {"partial": {"observations": ["durable"]}}

        with mock.patch.object(STRESS, "TRIAL_RUNNERS", {"cold": slow_runner}):
            trial = STRESS.run_trial(
                Path("/worker"),
                Path("/state"),
                config,
                "cold",
                0,
                1,
            )
        self.assertEqual(trial["status"], "failed")
        self.assertEqual(trial["error_type"], "DeadlineExceeded")
        self.assertEqual(
            trial["evidence"]["partial_result"]["partial"]["observations"],
            ["durable"],
        )

    def test_trial_passes_admission_and_effective_deadline_to_runner(self) -> None:
        config = STRESS.CampaignConfig(
            cold_trials=1,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
            trial_timeout_seconds=1.0,
            campaign_timeout_seconds=2.0,
        )
        admitted = {"identity_revision": STRESS.IDENTITY_REVISION_V2}
        captured: dict[str, object] = {}

        def runner(
            _binary: Path,
            _root: Path,
            _config: STRESS.CampaignConfig,
            _phase: str,
            _index: int,
            _seed: int,
            *,
            admitted_identity: dict[str, object],
            deadline: float,
        ) -> dict[str, object]:
            captured["admitted_identity"] = admitted_identity
            captured["deadline"] = deadline
            return {}

        with mock.patch.object(STRESS, "TRIAL_RUNNERS", {"cold": runner}):
            trial = STRESS.run_trial(
                Path("/worker"),
                Path("/state"),
                config,
                "cold",
                0,
                1,
                admitted_identity=admitted,
                campaign_deadline=time.monotonic() + 1.5,
            )
        self.assertEqual(trial["status"], "pass")
        self.assertIs(captured["admitted_identity"], admitted)
        self.assertIsInstance(captured["deadline"], float)

    def test_campaign_writes_terminal_interrupted_receipt(self) -> None:
        config = STRESS.CampaignConfig(
            cold_trials=1,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
            campaign_timeout_seconds=1.0,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "receipt.json"
            state = Path(directory) / "state"
            with mock.patch.object(
                STRESS, "preflight", side_effect=KeyboardInterrupt("stop now")
            ):
                result = STRESS.run_campaign(
                    ROOT,
                    Path("/worker"),
                    Path("/model"),
                    Path("/manifest"),
                    output,
                    state,
                    config,
                )
            receipt = json.loads(output.read_text())
        self.assertEqual(result, 130)
        self.assertEqual(receipt["status"], "interrupted")
        self.assertEqual(receipt["counts"]["planned"], 4)
        self.assertEqual(set(receipt["phases"]), set(STRESS.PHASES))
        self.assertEqual(
            sum(len(phase["trials"]) for phase in receipt["phases"].values()),
            receipt["counts"]["planned"],
        )
        self.assertTrue(
            all(
                trial["status"] == "incomplete"
                for phase in receipt["phases"].values()
                for trial in phase["trials"]
            )
        )
        self.assertTrue(receipt["failures"])

    def test_campaign_signal_termination_keeps_terminal_phase_rows(self) -> None:
        config = STRESS.CampaignConfig(
            cold_trials=1,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "receipt.json"
            with mock.patch.object(
                STRESS,
                "preflight",
                side_effect=STRESS.CampaignTerminated(),
            ):
                result = STRESS.run_campaign(
                    ROOT,
                    Path("/worker"),
                    Path("/model"),
                    Path("/manifest"),
                    output,
                    Path(directory) / "state",
                    config,
                )
            receipt = json.loads(output.read_text())
        self.assertEqual(result, 130)
        self.assertEqual(receipt["status"], "interrupted")
        self.assertEqual(set(receipt["phases"]), set(STRESS.PHASES))
        self.assertEqual(
            sum(len(phase["trials"]) for phase in receipt["phases"].values()),
            receipt["counts"]["planned"],
        )

    def test_campaign_writes_terminal_failed_receipt_for_base_exception(self) -> None:
        class Fatal(BaseException):
            pass

        config = STRESS.CampaignConfig(
            cold_trials=1,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
            campaign_timeout_seconds=1.0,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "receipt.json"
            with mock.patch.object(STRESS, "preflight", side_effect=Fatal("fatal")):
                result = STRESS.run_campaign(
                    ROOT,
                    Path("/worker"),
                    Path("/model"),
                    Path("/manifest"),
                    output,
                    Path(directory) / "state",
                    config,
                )
            receipt = json.loads(output.read_text())
        self.assertEqual(result, 1)
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["failures"][0]["error_type"], "Fatal")
        self.assertEqual(
            sum(len(phase["trials"]) for phase in receipt["phases"].values()),
            receipt["counts"]["planned"],
        )

    def test_phase_exception_is_fsynced_with_remaining_terminal_rows(self) -> None:
        class Fatal(BaseException):
            pass

        class Artifact:
            launch_path = Path("/worker")

            def close(self) -> None:
                return None

        config = STRESS.CampaignConfig(
            cold_trials=1,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
        )
        identity = {"admitted_wire_identity": {"identity_revision": 2}}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "receipt.json"
            with (
                mock.patch.object(
                    STRESS,
                    "preflight",
                    return_value=(Artifact(), identity),
                ),
                mock.patch.object(
                    STRESS,
                    "run_real_cancellation_probe",
                    return_value={"effect": {"outcome": "empty"}},
                ),
                mock.patch.object(STRESS, "run_phase", side_effect=Fatal("phase")),
            ):
                result = STRESS.run_campaign(
                    ROOT,
                    Path("/worker"),
                    Path("/model"),
                    Path("/manifest"),
                    output,
                    Path(directory) / "state",
                    config,
                )
            receipt = json.loads(output.read_text())
        self.assertEqual(result, 1)
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(set(receipt["phases"]), set(STRESS.PHASES))
        self.assertEqual(
            sum(len(phase["trials"]) for phase in receipt["phases"].values()),
            receipt["counts"]["planned"],
        )
        self.assertTrue(
            all(
                trial["status"] == "incomplete"
                for phase in receipt["phases"].values()
                for trial in phase["trials"]
            )
        )

    def test_scope_probe_requires_two_populated_peer_payloads(self) -> None:
        peers = STRESS.scope_peer_payloads(
            phase="cold", trial_index=0, seed=11, query_text="same query"
        )
        self.assertEqual(len(peers), 2)
        self.assertEqual(len({peer["source"] for peer in peers}), 2)
        self.assertEqual(len({peer["value_text"] for peer in peers}), 2)

    def test_trial_state_root_reuses_verified_model_bytes_in_isolated_catalog(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            campaign = Path(directory) / "campaign"
            models = campaign / "models"
            models.mkdir(parents=True)
            model_file = models / "onnx-model"
            model_file.write_bytes(b"verified")
            receipts = campaign / STRESS.MODEL_ACQUISITION_RECEIPT_RELATIVE.parent
            receipts.mkdir()
            (campaign / STRESS.MODEL_ACQUISITION_RECEIPT_RELATIVE).write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "operation_id": "1-" + "a" * 16 + "-install",
                        "operation": "install",
                        "outcome": "committed",
                        "model": STRESS.MODEL_NAME,
                        "repository": STRESS.MODEL_REPOSITORY,
                        "revision": STRESS.MODEL_REVISION,
                        "manifest_sha256": "a" * 64,
                        "revision_provenance_sha256": "b" * 64,
                        "acquisition_manifest_sha256": "c" * 64,
                        "root": str(campaign),
                        "tree_sha256": "d" * 64,
                        "files": [],
                        "created_at_unix": 1,
                    }
                ),
                encoding="utf-8",
            )
            trial = STRESS.prepare_trial_state_root(campaign, "cold", 0, 9)
            copied = trial / "models" / model_file.name
            self.assertTrue(copied.is_file())
            staged_receipt = json.loads(
                (trial / STRESS.MODEL_ACQUISITION_RECEIPT_RELATIVE).read_text()
            )
            self.assertEqual(staged_receipt["root"], str(trial))
            self.assertEqual(
                staged_receipt["receipt_path"],
                str(trial / STRESS.MODEL_ACQUISITION_RECEIPT_RELATIVE),
            )
            self.assertEqual(
                stat.S_IMODE(trial.stat().st_mode),
                0o700,
            )
            self.assertEqual(os.stat(model_file).st_ino, os.stat(copied).st_ino)

    def test_missing_acquisition_receipt_blocks_trial_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            campaign = Path(directory) / "campaign"
            models = campaign / "models"
            models.mkdir(parents=True)
            (models / "model.onnx").write_bytes(b"verified")
            with self.assertRaises(STRESS.StressFailure) as caught:
                STRESS.prepare_trial_state_root(campaign, "cold", 0, 9)
        self.assertIn("acquisition receipt", str(caught.exception))

    def test_acquisition_receipt_binds_canonical_manifest_and_tree(self) -> None:
        manifest = json.loads(
            (ROOT / STRESS.MODEL_ACQUISITION_MANIFEST_RELATIVE).read_text(
                encoding="utf-8"
            )
        )
        files = [
            {
                "path": entry["path"],
                "bytes": entry["bytes"],
                "sha256": entry["sha256"],
            }
            for entry in manifest["files"]
        ]
        model_identity = {
            "model": manifest["model"],
            "artifact_sha256": files[0]["sha256"],
            "repository": manifest["repository"],
            "revision": manifest["revision"],
            "revision_provenance": manifest["revision_provenance"],
            "files": files,
            "manifest_sha256": manifest["embedding_manifest_sha256"],
            "max_length": manifest["max_length"],
            "pooling": manifest["pooling"],
            "normalize": manifest["normalize"],
        }
        tree_digest = "d" * 64
        with tempfile.TemporaryDirectory() as directory:
            model_root = Path(directory) / "model"
            (model_root / "models").mkdir(parents=True)
            receipt = {
                "schema_version": 1,
                "operation_id": "1-" + "a" * 16 + "-install",
                "operation": "install",
                "outcome": "committed",
                "model": manifest["model"],
                "repository": manifest["repository"],
                "revision": manifest["revision"],
                "manifest_sha256": manifest["embedding_manifest_sha256"],
                "revision_provenance_sha256": manifest["revision_provenance_sha256"],
                "acquisition_manifest_sha256": STRESS.sha256_bytes(
                    STRESS.stable_json_bytes(manifest)
                ),
                "root": str(model_root),
                "tree_sha256": tree_digest,
                "files": files,
                "created_at_unix": 1,
                "receipt_path": str(
                    model_root / STRESS.MODEL_ACQUISITION_RECEIPT_RELATIVE
                ),
            }
            receipt_path = model_root / STRESS.MODEL_ACQUISITION_RECEIPT_RELATIVE
            receipt_path.parent.mkdir()
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            with mock.patch.object(
                STRESS, "model_tree_digest", return_value=tree_digest
            ):
                validated = STRESS.validate_model_acquisition_receipt(
                    ROOT,
                    model_root,
                    model_identity,
                )
                # Model acquisition is target-independent; a receipt that
                # still binds a worker target is not the Rust owner's shape.
                receipt_path.write_text(
                    json.dumps(dict(receipt, target="aarch64-apple-darwin")),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(
                    STRESS.StressFailure, "unexpected or missing fields"
                ):
                    STRESS.validate_model_acquisition_receipt(
                        ROOT, model_root, model_identity
                    )
        self.assertEqual(
            validated["manifest_sha256"], receipt["acquisition_manifest_sha256"]
        )
        self.assertEqual(validated["tree_sha256"], tree_digest)
        self.assertEqual(validated["receipt"]["root"], str(model_root))

    def test_opt_in_real_worker_smoke(self) -> None:
        if os.environ.get("TRACEDECAY_NCM_RUN_REAL_SMOKE") != "1":
            self.skipTest(
                "set TRACEDECAY_NCM_RUN_REAL_SMOKE=1 to run real worker smoke"
            )
        worker = Path(
            os.environ.get(
                "TRACEDECAY_NCM_WORKER",
                str(ROOT / "target/debug/tracedecay-ncm-worker"),
            )
        )
        model_root = Path(
            os.environ.get("TRACEDECAY_NCM_MODEL_ROOT")
            or os.environ.get("TRACEDECAY_NCM_REAL_MODEL_ROOT")
            or str(ROOT / "target/ncm-backend-model-root")
        )
        embedding_manifest = (
            ROOT / "product" / "ncm" / "reference" / "embedding-manifest.json"
        )
        acquisition_manifest = ROOT / STRESS.MODEL_ACQUISITION_MANIFEST_RELATIVE
        acquisition_receipt = model_root / STRESS.MODEL_ACQUISITION_RECEIPT_RELATIVE
        required_model_files = [
            model_root / "models" / relative for relative in STRESS.MODEL_REQUIRED_FILES
        ]
        if (
            not worker.is_file()
            or not (model_root / "models").is_dir()
            or not (model_root / "models" / "ncm-encoder-manifest.json").is_file()
            or not embedding_manifest.is_file()
            or not acquisition_manifest.is_file()
            or not acquisition_receipt.is_file()
            or not all(path.is_file() for path in required_model_files)
        ):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                output = root / "receipt.json"
                completed = subprocess.run(
                    [
                        sys.executable,
                        str(SCRIPT),
                        "--worker",
                        str(worker),
                        "--model-root",
                        str(model_root),
                        "--embedding-manifest",
                        str(embedding_manifest),
                        "--state-root",
                        str(root / "state"),
                        "--output",
                        str(output),
                        "--cold-trials",
                        "1",
                        "--warm-trials",
                        "1",
                        "--restart-trials",
                        "1",
                        "--concurrent-trials",
                        "1",
                        "--campaign-timeout-seconds",
                        "5",
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    env={**os.environ, "PYTHONNOUSERSITE": "1"},
                    timeout=30,
                )
                self.assertEqual(
                    completed.returncode,
                    2,
                    completed.stdout + completed.stderr,
                )
                receipt = json.loads(output.read_text())
            self.assertEqual(receipt["status"], "blocked")
            self.assertTrue(receipt["blockers"])
            self.assertEqual(set(receipt["phases"]), set(STRESS.PHASES))
            self.assertTrue(
                all(
                    trial["status"] == "incomplete"
                    for phase in receipt["phases"].values()
                    for trial in phase["trials"]
                )
            )
            return
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "receipt.json"
            state = root / "state"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--worker",
                    str(worker),
                    "--model-root",
                    str(model_root),
                    "--embedding-manifest",
                    str(embedding_manifest),
                    "--state-root",
                    str(state),
                    "--output",
                    str(output),
                    "--cold-trials",
                    "1",
                    "--warm-trials",
                    "1",
                    "--restart-trials",
                    "1",
                    "--concurrent-trials",
                    "1",
                    "--concurrency",
                    "2",
                    "--campaign-timeout-seconds",
                    "180",
                ],
                check=False,
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONNOUSERSITE": "1"},
                timeout=240,
            )
            self.assertEqual(
                completed.returncode, 0, completed.stdout + completed.stderr
            )
            receipt = json.loads(output.read_text())
        self.assertEqual(receipt["status"], "pass")
        staged_acquisition = receipt["identity"]["model_acquisition"]["staged"]
        self.assertEqual(staged_acquisition["receipt"]["root"], str(state))
        self.assertEqual(
            staged_acquisition["receipt"]["receipt_path"],
            str(state / STRESS.MODEL_ACQUISITION_RECEIPT_RELATIVE),
        )
        self.assertIn("cancellation", receipt["runtime_evidence"])
        self.assertIn("effect", receipt["runtime_evidence"]["cancellation"])

    def test_phase_scheduler_retains_every_incomplete_trial(self) -> None:
        config = STRESS.CampaignConfig(
            cold_trials=3,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
        )
        config.validate()
        with mock.patch.object(
            STRESS,
            "TRIAL_RUNNERS",
            {"cold": mock.Mock(side_effect=AssertionError("must not run"))},
        ):
            phase = STRESS.run_phase(
                Path("/worker"),
                Path("/state"),
                config,
                "cold",
                campaign_deadline=0.0,
            )
        self.assertEqual(phase["planned"], 3)
        self.assertEqual(phase["completed"], 0)
        self.assertEqual(phase["incomplete"], 3)
        self.assertTrue(
            all(trial["status"] == "incomplete" for trial in phase["trials"])
        )

    def test_phase_scheduler_persists_each_trial_snapshot(self) -> None:
        config = STRESS.CampaignConfig(
            cold_trials=2,
            warm_trials=1,
            restart_trials=1,
            concurrent_trials=1,
        )
        receipt = STRESS.new_receipt(
            config,
            ROOT,
            Path("/receipt.json"),
            Path("/state"),
        )
        snapshots: list[dict[str, object]] = []

        def persist() -> None:
            snapshots.append(json.loads(json.dumps(receipt)))

        with mock.patch.object(
            STRESS,
            "run_trial",
            side_effect=lambda *_args, **_kwargs: {
                "status": "pass",
                "phase": "cold",
                "index": len(receipt["phases"].get("cold", {}).get("trials", [])),
                "seed": 1,
                "evidence": {"observations": [], "recalls": [], "scope_probes": []},
            },
        ):
            phase = STRESS.run_phase(
                Path("/worker"),
                Path("/state"),
                config,
                "cold",
                campaign_deadline=time.monotonic() + 5.0,
                receipt=receipt,
                persist=persist,
            )
        self.assertEqual(phase["completed"], 2)
        self.assertGreaterEqual(len(snapshots), 3)
        self.assertEqual(
            len(snapshots[-1]["phases"]["cold"]["trials"]),
            2,
        )

    def test_receipt_write_is_atomic_and_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            receipt = {"status": "failed", "counts": {"planned": 1, "completed": 1}}
            STRESS.write_receipt(path, receipt)
            self.assertEqual(json.loads(path.read_text()), receipt)
            self.assertFalse(list(Path(directory).glob("*.tmp")))

    def test_receipt_write_cleans_partial_temp_after_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            with mock.patch.object(STRESS.os, "fsync", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    STRESS.write_receipt(path, {"status": "running"})
            self.assertFalse(path.exists())
            self.assertFalse(list(Path(directory).glob("*.tmp")))


if __name__ == "__main__":
    unittest.main()
