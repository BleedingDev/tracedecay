#!/usr/bin/env python3
"""Focused checks for the native-original-runner executable wrapper."""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path


WRAPPER = Path(__file__).with_name("native-original-runner")
RUNNER = WRAPPER.with_name("runner.py")


def test_wrapper_preserves_argv_cwd_and_exit_status() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-wrapper-") as temporary:
        root = Path(temporary)
        fake_bin = root / "bin"
        fake_bin.mkdir()
        caller_cwd = root / "caller-cwd"
        caller_cwd.mkdir()
        argv_file = root / "argv.bin"
        fake_python = fake_bin / "python3"
        fake_python.write_text(
            "#!/bin/sh\n"
            "printf '%s\\0' \"$@\" > \"$NATIVE_ORIGINAL_RUNNER_ARGS\"\n"
            "exit 37\n",
            encoding="utf-8",
        )
        fake_python.chmod(0o755)

        environment = os.environ.copy()
        environment["PATH"] = os.pathsep.join(
            (str(fake_bin), environment.get("PATH", ""))
        )
        environment["NATIVE_ORIGINAL_RUNNER_ARGS"] = str(argv_file)
        forwarded = (
            "--case",
            "row with spaces",
            "--ledger",
            str(root / "ledger with spaces.jsonl"),
        )

        completed = subprocess.run(
            [str(WRAPPER), *forwarded],
            cwd=caller_cwd,
            env=environment,
            capture_output=True,
            check=False,
        )

        assert completed.returncode == 37
        assert completed.stdout == b""
        assert completed.stderr == b""
        captured = argv_file.read_bytes().split(b"\0")[:-1]
        expected = [
            b"-S",
            str(RUNNER.resolve()).encode(),
            *(argument.encode() for argument in forwarded),
        ]
        assert captured == expected


def main() -> int:
    test_wrapper_preserves_argv_cwd_and_exit_status()
    print("native-original-runner wrapper check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
