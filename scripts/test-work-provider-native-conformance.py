#!/usr/bin/env python3
"""Run scripts/work_provider_native_conformance.sh against stand-in provider executables.

The stand-ins speak the stock wire formats, so no provider login, model request,
or network access is involved.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("work_provider_native_conformance.sh")

FAKE_PROVIDER = r'''
import json
import sys
import time

TURN_START_REPLY = {turn_start_reply}
STDIO_STARTUP = {stdio_startup}


def emit(message):
    sys.stdout.write((message if isinstance(message, str) else json.dumps(message)) + "\n")
    sys.stdout.flush()


def idle():
    try:
        while True:
            time.sleep(0.05)
    except KeyboardInterrupt:
        sys.exit(130)


args = sys.argv[1:]
if args == ["--version"]:
    emit("stand-in 0.0.0")
elif args == ["app-server"]:
    for line in sys.stdin:
        request = json.loads(line)
        method = request.get("method")
        if method == "initialize":
            emit({{"id": 0, "result": {{"userAgent": "stand-in"}}}})
        elif method == "thread/start":
            emit({{"id": 1, "result": {{"thread": {{"id": "thread-1"}}}}}})
        elif method == "turn/start":
            for reply in TURN_START_REPLY:
                emit(reply)
            idle()
else:
    sys.stdin.read()
    if STDIO_STARTUP is not None:
        emit(STDIO_STARTUP[args[0]])
    idle()
'''

VALID_STDIO_STARTUP = {
    "--print": {"type": "system", "subtype": "init", "session_id": "session-1"},
    "exec": {"type": "thread.started", "thread_id": "thread-1"},
}
VALID_TURN_START = [{"id": 2, "result": {"turn": {"id": "turn-1", "status": "inProgress"}}}]


@unittest.skipUnless(shutil.which("setsid"), "the probe needs util-linux setsid")
class NativeConformanceProbeTest(unittest.TestCase):
    def run_probe(self, turn_start_reply=VALID_TURN_START, stdio_startup=VALID_STDIO_STARTUP):
        with tempfile.TemporaryDirectory() as stage:
            provider = Path(stage, "provider")
            body = FAKE_PROVIDER.format(turn_start_reply=repr(turn_start_reply), stdio_startup=repr(stdio_startup))
            provider.write_text(f"#!{sys.executable}\n{body}")
            provider.chmod(0o755)
            env = {
                "HOME": stage,
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "TMPDIR": stage,
                "TRACEDECAY_WORK_NATIVE_CONFORMANCE": "1",
                "CLAUDE_BIN": str(provider),
                "CODEX_BIN": str(provider),
            }
            return subprocess.run(
                ["bash", str(SCRIPT)], cwd=stage, env=env, capture_output=True, text=True, timeout=120
            )

    def assert_rejected(self, result, reason):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertNotIn("PASS", result.stdout)
        self.assertIn(reason, result.stderr)

    def test_correlated_startup_passes(self):
        result = self.run_probe()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("provider=codex-app-server availability=available start=observed turn_id=turn-1", result.stdout)
        self.assertIn("provider=claude-code-cli availability=available start=observed session_id=session-1", result.stdout)
        self.assertIn("provider=codex-cli availability=available start=observed session_id=thread-1", result.stdout)
        self.assertTrue(result.stdout.rstrip().endswith("PASS"), result.stdout)

    def test_turn_start_rpc_error_is_rejected(self):
        result = self.run_probe(turn_start_reply=[{"id": 2, "error": {"code": -32600, "message": "turn rejected"}}])
        self.assert_rejected(result, "turn rejected")
        self.assertNotIn("provider=codex-app-server", result.stdout)

    def test_empty_turn_start_response_is_rejected(self):
        result = self.run_probe(turn_start_reply=[{"id": 2}])
        self.assert_rejected(result, "turn/start lacked a result")

    def test_turn_start_without_turn_id_is_rejected(self):
        result = self.run_probe(turn_start_reply=[{"id": 2, "result": {}}])
        self.assert_rejected(result, "turn/start response lacked a turn id")

    def test_failed_turn_is_rejected(self):
        result = self.run_probe(turn_start_reply=[{"id": 2, "result": {"turn": {"id": "turn-1", "status": "failed"}}}])
        self.assert_rejected(result, "turn/start reported a failed turn")

    def test_uncorrelated_notifications_do_not_count_as_startup(self):
        result = self.run_probe(
            turn_start_reply=[
                {"method": "turn/started", "params": {"threadId": "other-thread", "turn": {"id": "turn-9"}}},
                {"method": "item/agentMessage/delta", "params": {"threadId": "thread-1", "delta": "READY"}},
                {"id": 2, "error": {"message": "turn rejected"}},
            ]
        )
        self.assert_rejected(result, "turn rejected")

    def test_malformed_frame_is_rejected(self):
        result = self.run_probe(turn_start_reply=["not json"])
        self.assert_rejected(result, "codex app-server conformance failed")

    def test_stdio_provider_without_startup_event_is_rejected(self):
        result = self.run_probe(stdio_startup=None)
        self.assert_rejected(result, "claude-code-cli did not report startup")
        self.assertNotIn("start=observed", result.stdout)


if __name__ == "__main__":
    unittest.main()
