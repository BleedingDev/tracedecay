#!/usr/bin/env python3
"""Focused, dependency-free checks for the direct-original runner."""

from __future__ import annotations

import importlib.util
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


RUNNER_PATH = Path(__file__).with_name("runner.py")
SPEC = importlib.util.spec_from_file_location("native_original_runner", RUNNER_PATH)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


def expect_raises(error_type, callback) -> None:
    try:
        callback()
    except error_type:
        return
    raise AssertionError(f"expected {error_type.__name__}")


def test_semantic_mismatch_preserves_observables() -> None:
    original = {
        "status": "completed",
        "response": {
            "score": 900,
            "order": ["fact-a", "fact-b"],
            "receipt": {"id": "receipt-a"},
            "omissions": ["cursor"],
            "nonce": "original",
        },
    }
    product = {
        "status": "completed",
        "response": {
            "score": 899,
            "order": ["fact-a", "fact-b"],
            "receipt": {"id": "receipt-a"},
            "omissions": ["cursor"],
            "nonce": "product",
        },
    }
    result = runner.compare_results(original, product, {"ignore_json_pointers": ["/nonce"]})
    assert result["status"] == "fail"
    assert "original_semantic" in result and "product_semantic" in result
    assert result["original_semantic"]["score"] == 900


def test_missing_required_observable_is_unknown() -> None:
    side = {"status": "completed", "response": {"items": []}}
    result = runner.compare_results(
        side,
        side,
        {
            "semantic_json_pointers": ["/items", "/receipt"],
            "required_json_pointers": ["/receipt"],
        },
    )
    assert result["status"] == "unknown"
    assert result["missing_original"] == ["/receipt"]
    assert result["missing_product"] == ["/receipt"]


def test_status_priority_materializes_iterable() -> None:
    assert runner._status_priority(iter(["pass", "pass"])) == "pass"
    assert runner._status_priority(iter(["pass", "unknown"])) == "unknown"
    assert runner._status_priority(iter(["pass", "fail"])) == "fail"


def test_missing_original_binary() -> None:
    expect_raises(
        runner.BinaryError,
        lambda: runner.verify_binary(Path("/definitely/missing/native-original"), "original"),
    )


def test_shared_binary_is_refused() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-binaries-") as temporary:
        binary = Path(temporary) / "tracedecay"
        binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        binary.chmod(0o755)
        expect_raises(
            runner.BinaryError,
            lambda: runner.verify_distinct_binaries(binary, binary),
        )


def test_modified_reference_checkout_is_refused() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-reference-") as temporary:
        checkout = Path(temporary)
        subprocess.run(["git", "init", "-q", str(checkout)], check=True)
        subprocess.run(["git", "-C", str(checkout), "config", "user.email", "runner@example.invalid"], check=True)
        subprocess.run(["git", "-C", str(checkout), "config", "user.name", "runner"], check=True)
        (checkout / "source.txt").write_text("pristine\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(checkout), "add", "source.txt"], check=True)
        subprocess.run(["git", "-C", str(checkout), "commit", "-q", "-m", "reference"], check=True)
        revision = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
        verified = runner.verify_reference_checkout(checkout, revision)
        assert verified["clean"] is True
        (checkout / "source.txt").write_text("modified\n", encoding="utf-8")
        expect_raises(runner.ReferenceError, lambda: runner.verify_reference_checkout(checkout, revision))


def test_zero_relevant_cases_and_missing_operation_coverage() -> None:
    expect_raises(runner.RunnerError, lambda: runner.select_relevant_cases([], ["fact_store_search"]))
    cases = [{"id": "seed", "actions": [{"route": "fact_store_add"}]}]
    expect_raises(
        runner.RunnerError,
        lambda: runner.select_relevant_cases(cases, ["fact_store_add", "fact_store_search"]),
    )


def test_child_process_is_bounded_and_group_is_cleaned() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-process-") as temporary:
        root = Path(temporary)
        marker = root / "child.pid"
        script = root / "spawn_child.py"
        script.write_text(
            "import os, signal, sys, time\n"
            "marker = sys.argv[1]\n"
            "pid = os.fork()\n"
            "if pid == 0:\n"
            "    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))\n"
            "    open(marker, 'w', encoding='utf-8').write(str(os.getpid()))\n"
            "    while True: time.sleep(1)\n"
            "time.sleep(30)\n",
            encoding="utf-8",
        )
        artifact_dir = root / "artifacts"
        result = runner.run_process(
            [sys.executable, str(script), str(marker)],
            cwd=root,
            environment={**os.environ, "PYTHONUNBUFFERED": "1"},
            artifact_dir=artifact_dir,
            timeout=runner.ProcessTimeouts(0.1, 0.5, 0.5),
        )
        assert result["status"] == "censored"
        assert result["process"]["process_group_id"] is not None
        assert result["cleanup"]["status"] == "completed", result
        if marker.exists():
            child_pid = int(marker.read_text(encoding="utf-8"))
            for _ in range(20):
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.02)
            else:
                raise AssertionError(f"owned child survived cleanup: {child_pid}")


def test_owned_daemon_publishes_and_cleans_identity() -> None:
    if os.name == "nt":
        return
    with tempfile.TemporaryDirectory(prefix="native-original-daemon-") as temporary:
        root = Path(temporary)
        binary = root / "fake-tracedecay.py"
        binary.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, signal, socket, sys, time\n"
            "if sys.argv[1:3] != ['daemon', 'run']:\n"
            "    raise SystemExit(2)\n"
            "socket_path = sys.argv[sys.argv.index('--socket') + 1]\n"
            "profile = sys.argv[sys.argv.index('--profile-root') + 1]\n"
            "os.makedirs(profile, exist_ok=True)\n"
            "listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)\n"
            "listener.bind(socket_path)\n"
            "listener.listen(4)\n"
            "listener.settimeout(0.05)\n"
            "record = {'pid': os.getpid(), 'process_run_id': 'fake-run', 'epoch': 1,\n"
            " 'version': 'fake', 'endpoint': {'kind': 'unix', 'address': socket_path},\n"
            " 'auth_token': 'a' * 64, 'profile_root': os.path.realpath(profile)}\n"
            "with open(os.path.join(profile, 'daemon-authority.json'), 'w') as stream:\n"
            "    json.dump(record, stream)\n"
            "running = True\n"
            "def stop(*_):\n"
            "    global running\n"
            "    running = False\n"
            "signal.signal(signal.SIGTERM, stop)\n"
            "while running:\n"
            "    try:\n"
            "        connection, _ = listener.accept()\n"
            "        connection.close()\n"
            "    except socket.timeout:\n"
            "        pass\n"
            "listener.close()\n",
            encoding="utf-8",
        )
        binary.chmod(0o755)
        side_dir = root / "side"
        side_dir.mkdir()
        daemon = runner._OwnedDaemon.start(
            side="original",
            binary=binary,
            side_dir=side_dir,
            profile_root=side_dir / "profile",
            artifact_dir=root / "artifacts",
            timeouts=runner.ProcessTimeouts(2.0, 0.5, 0.5),
        )
        assert daemon.identity is not None
        assert daemon.identity["pid"] == daemon.process.pid
        assert daemon.current_identity() == daemon.identity
        cleanup = daemon.stop(runner.ProcessTimeouts(2.0, 0.5, 0.5), reason="test")
        assert cleanup["status"] == "completed", cleanup
        assert not daemon.socket_path.exists()
        assert (root / "artifacts" / "process.json").is_file()
        assert (root / "artifacts" / "cleanup.json").is_file()


def main() -> int:
    tests = [
        test_semantic_mismatch_preserves_observables,
        test_missing_required_observable_is_unknown,
        test_status_priority_materializes_iterable,
        test_missing_original_binary,
        test_shared_binary_is_refused,
        test_modified_reference_checkout_is_refused,
        test_zero_relevant_cases_and_missing_operation_coverage,
        test_child_process_is_bounded_and_group_is_cleaned,
        test_owned_daemon_publishes_and_cleans_identity,
    ]
    for test in tests:
        test()
        print(f"ok {test.__name__}")
    print(f"{len(tests)} native-original runner checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
