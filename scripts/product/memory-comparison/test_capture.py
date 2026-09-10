"""Deterministic collector fixtures; no daemon, model, Cargo or benchmark runs."""

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from capture import (
    AttemptTiming,
    ProcessIdentity,
    ProcessTreeCapture,
    capture_directories,
    paired_delta,
    parse_ps,
    parse_time_l,
    read_process_identity,
)


START = "Thu Sep 10 01:02:03 2026"
REUSED = "Thu Sep 10 01:02:04 2026"


def row(pid, parent, rss=100, cpu="0:00.01", start=START):
    return f"{pid} {parent} {start} {rss} {cpu}\n"


class TickClock:
    def __init__(self):
        self.value = 0

    def __call__(self):
        self.value += 1_000_000
        return self.value


class AttemptTimingTests(unittest.TestCase):
    def test_failure_and_deadline_are_observed_terminals(self):
        for terminal in ("success", "failure", "deadline", "cancelled"):
            attempt = AttemptTiming("warm_recall")
            attempt.start(at_ns=200)
            attempt.finish(terminal, at_ns=800)
            report = attempt.report()
            self.assertEqual(report["elapsed_ns"], 600)
            self.assertEqual(report["terminal"], terminal)
            self.assertEqual(report["status"], "measured")
            self.assertIsNone(report["elapsed_at_cutoff_ns"])
            with self.assertRaises(ValueError):
                attempt.finish("success", at_ns=900)

    def test_cutoff_does_not_fabricate_completion(self):
        attempt = AttemptTiming("correction_verification")
        attempt.start(at_ns=50)
        attempt.censor("verification deadline", at_ns=175)
        report = attempt.report()
        self.assertEqual(report["status"], "censored")
        self.assertEqual(report["elapsed_at_cutoff_ns"], 125)
        self.assertIsNone(report["elapsed_ns"])
        self.assertIsNone(report["end_monotonic_ns"])
        self.assertIsNone(report["terminal"])

    def test_unexecuted_and_instrumentation_failure_keep_missing_timing(self):
        attempt = AttemptTiming("observe")
        self.assertIsNone(attempt.report()["elapsed_ns"])
        attempt.unmeasured("environment unavailable")
        self.assertEqual(attempt.report()["reason"], "environment unavailable")
        attempt.start(at_ns=0)
        with self.assertRaises(ValueError):
            attempt.finish("success", at_ns=-1)
        attempt.finish("failure", at_ns=10)
        attempt.unmeasured("clock source invalidated")
        self.assertEqual(attempt.report()["terminal"], "failure")
        self.assertIsNone(attempt.report()["elapsed_ns"])


class ParsingTests(unittest.TestCase):
    def test_ps_units_start_identity_and_cpu_formats(self):
        readings, errors = parse_ps(row(10, 1, 12, "2-01:02:03.50") + row(11, 10, 0, "01:05.25"))
        self.assertEqual(errors, [])
        self.assertEqual(readings[10].identity, ProcessIdentity(10, START))
        self.assertEqual(readings[10].rss_bytes, 12 * 1024)
        self.assertEqual(readings[10].cpu_seconds, 176523.5)
        self.assertEqual(readings[11].rss_bytes, 0)
        self.assertEqual(readings[11].cpu_seconds, 65.25)

    def test_partial_and_duplicate_rows_are_not_zero_or_chosen_arbitrarily(self):
        readings, errors = parse_ps(row(10, 1, "-", "-") + "truncated row\n" + row(11, 10) + row(11, 1, start=REUSED))
        self.assertIsNone(readings[10].rss_bytes)
        self.assertIsNone(readings[10].cpu_seconds)
        self.assertNotIn(11, readings)
        self.assertEqual(len(errors), 2)

    def test_time_l_is_command_rusage_and_missing_fields_are_null(self):
        measured = parse_time_l("application diagnostic\n  0.31 real 0.10 user 0.02 sys\n  40960 maximum resident set size\n")
        self.assertEqual(measured["peak_rss_bytes"], 40960)
        self.assertEqual(measured["user_cpu_seconds"], 0.10)
        self.assertEqual(measured["scope"], "timed_command_rusage")
        self.assertEqual(measured["status"], "measured")
        self.assertEqual(parse_time_l(" 99 maximum resident set size\n")["status"], "partial")
        missing = parse_time_l("application failed\n")
        self.assertIsNone(missing["peak_rss_bytes"])
        self.assertIsNone(missing["system_cpu_seconds"])
        self.assertEqual(missing["status"], "unmeasured")
        self.assertIsNone(parse_time_l("1..0 real 0.1 user 0.2 sys\n")["user_cpu_seconds"])

    def test_identity_command_is_scoped_to_supplied_pid_and_uses_c_locale(self):
        def runner(command, **kwargs):
            self.assertEqual(command[:3], ["/bin/ps", "-p", "10"])
            self.assertEqual(kwargs["env"]["LC_ALL"], "C")
            return subprocess.CompletedProcess(command, 0, row(10, 1), "")

        self.assertEqual(read_process_identity(10, runner=runner), ProcessIdentity(10, START))
        with self.assertRaises(ValueError):
            read_process_identity(0, runner=runner)


class ProcessTreeTests(unittest.TestCase):
    def capture(self, **kwargs):
        return ProcessTreeCapture(ProcessIdentity(10, START), "warm_recall", clock=TickClock(), **kwargs)

    def test_simultaneous_peak_is_not_sum_of_process_peaks(self):
        capture = self.capture()
        capture.sample(ps_output=row(10, 1, 100, "0:01.00") + row(11, 10, 900) + row(12, 11, 50) + row(99, 1, 999999))
        capture.sample(ps_output=row(10, 1, 1000, "0:02.50") + row(11, 10, 50) + row(12, 11, 50))
        report = capture.report()
        self.assertEqual(report["start_tree_rss_bytes"], 1050 * 1024)
        self.assertEqual(report["end_tree_rss_bytes"], 1100 * 1024)
        self.assertEqual(report["sampled_peak_tree_rss_bytes"], 1100 * 1024)
        self.assertEqual(sum(process["sampled_peak_rss_bytes"] for process in report["processes"]), 1950 * 1024)
        self.assertEqual(report["peak_child_count"], 2)
        self.assertEqual(report["processes"][0]["cpu_delta_seconds"], 1.5)
        self.assertTrue(all(process["os_high_water"] is None for process in report["processes"]))
        self.assertEqual({process["identity"]["pid"] for process in report["processes"]}, {10, 11, 12})
        json.dumps(report, allow_nan=False)

    def test_pid_reuse_does_not_enroll_new_tree_and_orphan_stays_owned(self):
        capture = self.capture()
        capture.sample(ps_output=row(10, 1) + row(11, 10))
        sample = capture.sample(ps_output=row(10, 1, 999999, start=REUSED) + row(12, 10, 999999) + row(11, 1) + row(13, 11))
        self.assertEqual(sample["root_status"], "replaced")
        self.assertEqual({process["identity"]["pid"] for process in sample["processes"]}, {11, 13})
        self.assertIsNone(sample["tree_rss_bytes"])
        self.assertEqual(sample["observed_tree_rss_bytes"], 200 * 1024)
        self.assertEqual(sample["status"], "partial")
        sample = capture.sample(ps_output=row(11, 1, 999999, start=REUSED) + row(13, 1))
        self.assertEqual([process["identity"]["pid"] for process in sample["processes"]], [13])

    def test_missing_sample_preserves_gap_and_boundary_absence(self):
        moments = iter([0, 1_000_000, 70_000_000, 71_000_000, 90_000_000, 91_000_000])
        capture = ProcessTreeCapture(ProcessIdentity(10, START), "observe", clock=lambda: next(moments))
        capture.sample(ps_output=row(10, 1))
        missing = capture.sample(ps_output="")
        capture.sample(ps_output=row(10, 1, 150))
        self.assertEqual(missing["gap_ns"], 70_000_000)
        self.assertEqual(missing["overrun_ns"], 50_000_000)
        self.assertIsNone(missing["tree_rss_bytes"])
        self.assertIsNone(missing["tree_cpu_seconds"])
        self.assertIsNone(missing["process_count"])
        report = capture.report()
        self.assertEqual(report["status"], "partial")
        self.assertEqual(report["maximum_gap_ns"], 70_000_000)
        self.assertEqual(report["sample_count"], 3)
        self.assertEqual(report["sampled_peak_tree_rss_bytes"], 150 * 1024)

    def test_partial_metric_retains_other_process_evidence(self):
        capture = self.capture()
        sample = capture.sample(ps_output=row(10, 1, 100) + row(11, 10, "-"))
        self.assertEqual(sample["observed_process_count"], 2)
        self.assertEqual(sample["observed_tree_rss_bytes"], 100 * 1024)
        self.assertIsNone(sample["tree_rss_bytes"])
        self.assertEqual(sample["processes"][1]["cpu_seconds"], 0.01)
        self.assertEqual(sample["child_count"], 1)
        self.assertEqual(sample["tree_cpu_seconds"], 0.02)
        sample = capture.sample(ps_output=row(10, 1, 100, "-") + row(11, 10, 100))
        self.assertEqual(sample["tree_rss_bytes"], 200 * 1024)
        self.assertIsNone(sample["tree_cpu_seconds"])

    def test_command_failure_and_timeout_are_unmeasured(self):
        responses = [subprocess.CompletedProcess([], 1, "", "denied"), subprocess.TimeoutExpired("ps", 1)]
        for response in responses:
            def runner(*args, **kwargs):
                if isinstance(response, Exception):
                    raise response
                return response

            capture = self.capture(runner=runner)
            sample = capture.sample()
            self.assertEqual(sample["status"], "unmeasured")
            self.assertIsNone(sample["tree_rss_bytes"])
            self.assertIsNone(capture.report()["sampled_peak_tree_rss_bytes"])
            self.assertIn("ps collection failed", sample["errors"][0])

    def test_explicit_exit_and_high_water_are_identity_attributed(self):
        capture = self.capture()
        capture.sample(ps_output=row(10, 1) + row(11, 10))
        worker = ProcessIdentity(11, START)
        capture.record_os_high_water(worker, 500000, source="fixture per-process OS counter")
        capture.sample(ps_output=row(10, 1))
        process = capture.report()["processes"][1]
        self.assertIsNone(process["exit"])
        self.assertIsNone(process["end_rss_bytes"])
        self.assertEqual(process["os_high_water"]["rss_bytes"], 500000)
        capture.record_exit(worker, -15, at_ns=100)
        self.assertEqual(capture.report()["processes"][1]["exit"]["returncode"], -15)
        with self.assertRaises(ValueError):
            capture.record_os_high_water(ProcessIdentity(11, REUSED), 999999, source="other process")
        self.assertEqual(capture.report()["sampled_peak_tree_rss_bytes"], 200 * 1024)

    def test_no_samples_and_missing_endpoint_stay_unmeasured(self):
        capture = self.capture()
        self.assertIsNone(capture.report()["peak_child_count"])
        capture.sample(ps_output=row(10, 1) + row(11, 10))
        capture.sample(ps_output="")
        report = capture.report()
        self.assertIsNone(report["end_tree_rss_bytes"])
        self.assertIsNone(report["end_child_count"])
        self.assertIsNone(report["processes"][1]["cpu_delta_seconds"])

    def test_background_sampling_continues_until_caller_stops(self):
        sampled = threading.Event()
        calls = []

        def runner(command, **kwargs):
            calls.append(command)
            if len(calls) >= 3:
                sampled.set()
            return subprocess.CompletedProcess(command, 0, row(10, 1), "")

        capture = ProcessTreeCapture(ProcessIdentity(10, START), "warm_recall", runner=runner)
        try:
            capture.start()
            self.assertTrue(sampled.wait(1), "periodic sampler did not produce fixture samples")
        finally:
            report = capture.stop()
        self.assertGreaterEqual(report["sample_count"], 4)
        self.assertEqual(report["nominal_interval_seconds"], 0.020)
        self.assertGreater(report["maximum_gap_ns"], 0)


class DiskTests(unittest.TestCase):
    def test_sparse_hardlink_symlink_shared_category_and_missing_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "provider"
            database.mkdir()
            sparse = database / "provider.db"
            with sparse.open("wb") as stream:
                stream.truncate(1 << 20)
            hardlink = database / "alias.db"
            os.link(sparse, hardlink)
            symlink = database / "external-link"
            symlink.symlink_to("/does/not/exist")
            model = root / "model.bin"
            model.write_bytes(b"model")
            report = capture_directories({"provider_db": [database], "model_cache": [model], "shared_model": [model], "missing": [root / "absent"]})
            provider = report["categories"]["provider_db"]
            self.assertEqual(provider["logical_bytes"], (1 << 20) + symlink.lstat().st_size)
            self.assertEqual(provider["allocated_bytes"], (sparse.stat().st_blocks + symlink.lstat().st_blocks) * 512)
            self.assertEqual(provider["file_count"], 2)
            self.assertEqual(len(provider["shared_entries"]), 1)
            self.assertEqual(report["categories"]["model_cache"]["logical_bytes"], 5)
            self.assertEqual(report["categories"]["shared_model"]["logical_bytes"], 0)
            self.assertEqual(report["categories"]["shared_model"]["shared_entries"][0]["counted_in"], "model_cache")
            self.assertIsNone(report["categories"]["missing"]["logical_bytes"])
            self.assertIsNone(report["categories"]["missing"]["allocated_bytes"])
            json.dumps(report, allow_nan=False)

    def test_scan_failure_and_unavailable_allocated_blocks_preserve_partial_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch("capture.os.scandir", side_effect=PermissionError("fixture denial")):
                category = capture_directories({"canonical": [Path(temporary)]})["categories"]["canonical"]
            self.assertIsNone(category["logical_bytes"])
            self.assertIn("fixture denial", category["errors"][0])
            path = Path(temporary) / "data"
            path.write_bytes(b"abc")
            no_blocks = os.stat_result(tuple(path.stat()))
            with patch.object(Path, "lstat", return_value=no_blocks):
                category = capture_directories({"canonical": [path]})["categories"]["canonical"]
            self.assertEqual(category["logical_bytes"], 3)
            self.assertIsNone(category["allocated_bytes"])
            self.assertTrue(category["allocation_missing"])

    def test_empty_existing_directory_is_measured_zero_but_missing_input_is_not(self):
        with tempfile.TemporaryDirectory() as temporary:
            report = capture_directories({"empty": [Path(temporary)], "unspecified": []})
        self.assertEqual(report["categories"]["empty"]["logical_bytes"], 0)
        self.assertIsNone(report["categories"]["unspecified"]["logical_bytes"])

    def test_matched_deltas_remain_signed_and_missing(self):
        self.assertEqual(paired_delta(4, 9), -5)
        self.assertEqual(paired_delta(0.1, 0.5), -0.4)
        self.assertIsNone(paired_delta(None, 9))
        self.assertIsNone(paired_delta(4, None))


class OwnedChildSmoke(unittest.TestCase):
    def test_sampler_stop_does_not_terminate_owned_child(self):
        child = subprocess.Popen([sys.executable, "-S", "-c", "import sys; sys.stdin.buffer.read()"], stdin=subprocess.PIPE)
        try:
            identity = read_process_identity(child.pid)
            capture = ProcessTreeCapture(identity, "owned_child_smoke")
            capture.start()
            report = capture.stop()
            self.assertEqual(capture.stop()["sample_count"], report["sample_count"])
            with self.assertRaises(ValueError):
                capture.start()
            self.assertIsNone(child.poll())
            self.assertGreaterEqual(report["sample_count"], 2)
            self.assertEqual(report["root"], {"pid": child.pid, "start_identity": identity.start_identity})
            self.assertGreater(report["processes"][0]["sampled_peak_rss_bytes"], 0)
            child.communicate(timeout=3)
            capture.record_exit(identity, child.returncode)
            self.assertEqual(capture.report()["processes"][0]["exit"]["returncode"], 0)
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=3)


if __name__ == "__main__":
    unittest.main()
