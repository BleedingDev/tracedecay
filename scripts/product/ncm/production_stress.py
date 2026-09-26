#!/usr/bin/env python3
"""Run a bounded, reproducible production NCM reliability campaign.

The campaign deliberately talks to the native worker over its production wire
protocol. It never enables the test-double flag for a campaign session and it
does not retry a missing recall. Every planned trial gets a durable receipt,
including trials that cannot start or that fail part way through.

The worker and model are admission controlled by check-backend.py before any
workload is run. This keeps this runner focused on reliability evidence: the
checker owns the trusted worker/model pins, while this file records the exact
identities observed by the worker and the durable state digests returned after
each observation and recall.
"""

from __future__ import annotations

import argparse
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
    as_completed,
)
from dataclasses import dataclass
import hashlib
import importlib.util
import json
import os
from pathlib import Path
from queue import Empty, Full, Queue
import re
import select
import signal
import shutil
import stat
import struct
import subprocess
import threading
import time
from typing import Any, Callable, Sequence


SCRIPT_PATH = Path(__file__).resolve()
REPOSITORY_ROOT = SCRIPT_PATH.parents[3]
CHECK_BACKEND_PATH = SCRIPT_PATH.parent / "check-backend.py"

CAMPAIGN_NAME = "ncm.production-stress.v1"
CAMPAIGN_SCHEMA_VERSION = 1
PROTOCOL_VERSION = 1
PROTOCOL_IDENTITY = "tracedecay.ncm.worker.v1"
ALGORITHM_PROFILE = "ncm-biomem-rs.v1"
ALGORITHM_CONFIG_SHA256 = (
    "b621b06c0e79b0d34e65ff592fc0f420ae7f478a3c01db6fb32bff6016b6d8c0"
)
MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
MODEL_REPOSITORY = "Xenova/paraphrase-multilingual-MiniLM-L12-v2"
MODEL_REVISION = "2c4055b12046f11709e9df2c122e59ffbdc2f900"
MODEL_REVISION_PROVENANCE = (
    "product/ncm/receipts/backend/2fc72f1d81f543224d8e7d8ef19195b026ba855f.json"
    "#/identities/model/revision"
)
MODEL_POOLING = "mean"
MODEL_NORMALIZE = True
MODEL_REQUIRED_FILES = (
    "onnx/model.onnx",
    "tokenizer.json",
    "config.json",
    "special_tokens_map.json",
    "tokenizer_config.json",
)
MODEL_CACHE_REPOSITORY = "models--Xenova--paraphrase-multilingual-MiniLM-L12-v2"
MODEL_BASE_URL = (
    "https://huggingface.co/Xenova/paraphrase-multilingual-MiniLM-L12-v2/"
    "resolve/2c4055b12046f11709e9df2c122e59ffbdc2f900/"
)
MODEL_ACQUISITION_MANIFEST_RELATIVE = Path(
    "product/ncm/release/model-acquisition-manifest.json"
)
MODEL_ACQUISITION_RECEIPT_RELATIVE = Path("receipts/ncm-model-acquisition-v1.json")
MODEL_ARTIFACT_MARKER = "test-double/hash"
REAL_MODEL_MAX_LENGTH = 128
IDENTITY_REVISION_V2 = 2
REAL_ENCODER_MARKERS = ("fastembed", "onnxruntime")
ACQUISITION_MANIFEST_FIELDS = {
    "schema_version",
    "manifest_type",
    "provider_id",
    "worker",
    "embedding_manifest",
    "embedding_manifest_sha256",
    "model_root",
    "cache_repository",
    "model",
    "repository",
    "revision",
    "revision_provenance",
    "revision_provenance_sha256",
    "max_length",
    "pooling",
    "normalize",
    "transport",
    "base_url",
    "files",
    "transaction",
    "receipt",
}
ACQUISITION_RECEIPT_FIELDS = {
    "schema_version",
    "operation_id",
    "operation",
    "outcome",
    "model",
    "repository",
    "revision",
    "manifest_sha256",
    "revision_provenance_sha256",
    "acquisition_manifest_sha256",
    "root",
    "tree_sha256",
    "files",
    "created_at_unix",
}
ACQUISITION_RECEIPT_OPTIONAL_FIELDS = {"receipt_path"}
OPERATION_ID = re.compile(r"^[0-9a-f]+-[0-9a-f]{16}-(?:install|update)$")
MAX_REQUEST_BYTES = 256 * 1024
MAX_REPLY_BYTES = 1024 * 1024
DEFAULT_TRIALS_PER_PHASE = 25
DEFAULT_CONCURRENCY = 4
MAX_TRIALS_PER_PHASE = 100
MAX_TOTAL_TRIALS = 400
DEFAULT_OPERATION_TIMEOUT_MS = 120_000
DEFAULT_TRIAL_TIMEOUT_SECONDS = 600.0
DEFAULT_CAMPAIGN_TIMEOUT_SECONDS = 7_200.0
OWNER_MAILBOX_MAX_REQUESTS = 32
DISPATCHER_CLOSE_TIMEOUT_SECONDS = 2.0
REAL_CANCELLATION_TIMEOUT_MS = 5
CANCELLATION_WORKLOAD_BYTES = 8 * 1024
SESSION_CLEANUP_TIMEOUT_SECONDS = 2.0
PHASES = ("cold", "warm", "restart", "concurrent")
PHASE_OFFSETS = {
    "cold": 0,
    "warm": 1_000_003,
    "restart": 2_000_003,
    "concurrent": 3_000_003,
}
HEX_64 = re.compile(r"^[0-9a-f]{64}$")


class StressFailure(RuntimeError):
    """A bounded campaign or admission assertion failed."""


class SessionFailure(StressFailure):
    """A worker session failed while retaining its operation evidence."""

    def __init__(
        self,
        message: str,
        evidence: dict[str, Any],
        *,
        error_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.evidence = evidence
        self.error_code = error_code


class EvidenceFailure(StressFailure):
    """A trial failed while retaining all sessions it could start."""

    def __init__(
        self,
        message: str,
        evidence: dict[str, Any],
        *,
        error_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.evidence = evidence
        self.error_code = error_code


class RequiredRecordMiss(StressFailure):
    """A recall did not return the trial's admitted required record."""

    def __init__(self, message: str) -> None:
        self.error_code = "required_record_miss"
        self.required_record_miss = True
        super().__init__(f"required_record_miss: {message}")


class SessionInterrupted(KeyboardInterrupt):
    """A worker session was interrupted after retaining its evidence."""

    def __init__(
        self,
        message: str,
        evidence: dict[str, Any],
        *,
        error_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.evidence = evidence
        self.error_code = error_code


class CampaignTerminated(KeyboardInterrupt):
    """SIGTERM translated into a receipt-producing campaign interruption."""

    def __init__(self, message: str = "campaign received SIGTERM") -> None:
        self.error_code = "campaign_terminated"
        super().__init__(message)


class DeadlineExceeded(StressFailure):
    """A trial or campaign deadline expired before a worker action completed."""

    def __init__(self, message: str) -> None:
        self.error_code = "deadline_exceeded"
        super().__init__(message)


def contains_required_record_miss(value: Any, _seen: set[int] | None = None) -> bool:
    """Find a typed required-record miss anywhere in retained failure evidence."""

    seen = _seen if _seen is not None else set()
    if isinstance(value, RequiredRecordMiss):
        return True
    if isinstance(value, BaseException):
        if getattr(value, "error_code", None) == "required_record_miss":
            return True
        if getattr(value, "required_record_miss", False) is True:
            return True
        value_id = id(value)
        if value_id in seen:
            return False
        seen.add(value_id)
        attributes = getattr(value, "__dict__", {})
        return any(
            contains_required_record_miss(nested, seen)
            for nested in (
                getattr(value, "evidence", None),
                value.__cause__,
                value.__context__,
                value.args,
                *(attributes.values() if isinstance(attributes, dict) else ()),
            )
        )
    if isinstance(value, dict):
        value_id = id(value)
        if value_id in seen:
            return False
        seen.add(value_id)
        if value.get("error_code") == "required_record_miss":
            return True
        if value.get("required_record_miss") is True:
            return True
        return any(
            contains_required_record_miss(nested, seen)
            for nested in (*value.keys(), *value.values())
        )
    if isinstance(value, (list, tuple, set, frozenset)):
        value_id = id(value)
        if value_id in seen:
            return False
        seen.add(value_id)
        return any(contains_required_record_miss(nested, seen) for nested in value)
    return False


def failure_evidence(error: BaseException) -> dict[str, Any]:
    """Normalize typed failure metadata into JSON-safe retained evidence."""

    evidence = getattr(error, "evidence", {})
    normalized = dict(evidence) if isinstance(evidence, dict) else {}
    normalized.update(
        {
            "error_type": type(error).__name__,
            "error_code": getattr(error, "error_code", None),
            "required_record_miss": contains_required_record_miss(error),
            "message": str(error),
        }
    )
    return normalized


def failure_error_code(error: BaseException, default: str | None = None) -> str | None:
    """Preserve a typed cause code when an evidence wrapper changes the class."""

    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        code = getattr(current, "error_code", None)
        if isinstance(code, str) and code:
            return code
        cause = current.__cause__
        if isinstance(cause, BaseException):
            current = cause
            continue
        context = current.__context__
        current = context if isinstance(context, BaseException) else None
    return default


def require(condition: bool, message: str) -> None:
    """Raise a typed failure for one reliability assertion."""

    if not condition:
        raise StressFailure(message)


def sha256_bytes(value: bytes) -> str:
    """Return a lowercase SHA-256 digest."""

    return hashlib.sha256(value).hexdigest()


def stable_json_bytes(value: Any, *, sort_keys: bool = True) -> bytes:
    """Encode bounded JSON without whitespace or non-finite numbers."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=sort_keys,
        separators=(",", ":"),
    ).encode("utf-8")


def _reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> Any:
    raise ValueError(f"non-finite JSON constant: {value}")


def parse_reply(body: bytes) -> dict[str, Any]:
    """Decode one strict worker reply object."""

    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise StressFailure(f"worker reply was not strict JSON: {error}") from error
    require(isinstance(value, dict), "worker reply was not a JSON object")
    return value


def outcome_name(reply: dict[str, Any]) -> str:
    """Return the stable snake-case name of a worker outcome."""

    outcome = reply.get("outcome")
    if isinstance(outcome, str):
        return outcome.lower()
    if isinstance(outcome, dict) and len(outcome) == 1:
        return str(next(iter(outcome))).lower()
    return "unknown"


def operation_digest(operation: dict[str, Any]) -> str:
    """Hash one operation evidence object for receipt-level correlation."""

    return sha256_bytes(stable_json_bytes(operation))


def wire_namespace(namespace: str) -> str:
    """Convert a human trial namespace to the worker's opaque 64-hex handle."""

    if HEX_64.fullmatch(namespace):
        return namespace
    return sha256_bytes(namespace.encode("utf-8"))


def phase_seed(seed: int, phase: str, index: int) -> int:
    """Derive a stable, non-overlapping seed for one phase/index pair."""

    require(phase in PHASE_OFFSETS, f"unknown phase: {phase}")
    return seed + PHASE_OFFSETS[phase] + index


def _target_dir(repository: Path) -> Path:
    configured = os.environ.get("CARGO_TARGET_DIR")
    if not configured:
        return repository / "target"
    path = Path(configured)
    return path if path.is_absolute() else repository / path


def default_worker_path(repository: Path) -> Path:
    filename = (
        "tracedecay-ncm-worker.exe" if os.name == "nt" else "tracedecay-ncm-worker"
    )
    return _target_dir(repository) / "debug" / filename


def default_output_path(repository: Path) -> Path:
    return (
        _target_dir(repository)
        / "test-profile"
        / "ncm-production-stress"
        / "receipt.json"
    )


def default_state_root(repository: Path) -> Path:
    return (
        _target_dir(repository)
        / "test-profile"
        / "ncm-production-stress"
        / f"state-{os.getpid()}"
    )


@dataclass(frozen=True)
class CampaignConfig:
    """Validated, bounded campaign configuration."""

    cold_trials: int = DEFAULT_TRIALS_PER_PHASE
    warm_trials: int = DEFAULT_TRIALS_PER_PHASE
    restart_trials: int = DEFAULT_TRIALS_PER_PHASE
    concurrent_trials: int = DEFAULT_TRIALS_PER_PHASE
    seed: int = 20260916
    concurrency: int = DEFAULT_CONCURRENCY
    operation_timeout_ms: int = DEFAULT_OPERATION_TIMEOUT_MS
    trial_timeout_seconds: float = DEFAULT_TRIAL_TIMEOUT_SECONDS
    campaign_timeout_seconds: float = DEFAULT_CAMPAIGN_TIMEOUT_SECONDS

    @property
    def counts(self) -> dict[str, int]:
        return {
            "cold": self.cold_trials,
            "warm": self.warm_trials,
            "restart": self.restart_trials,
            "concurrent": self.concurrent_trials,
        }

    @property
    def planned_trials(self) -> int:
        return sum(self.counts.values())

    def validate(self) -> None:
        for phase, count in self.counts.items():
            require(
                isinstance(count, int) and not isinstance(count, bool),
                f"{phase} trial count is not an integer",
            )
            require(
                0 < count <= MAX_TRIALS_PER_PHASE,
                f"{phase} trial count must be in 1..{MAX_TRIALS_PER_PHASE}",
            )
        require(
            self.planned_trials <= MAX_TOTAL_TRIALS,
            f"total trial count exceeds {MAX_TOTAL_TRIALS}",
        )
        require(
            isinstance(self.seed, int) and not isinstance(self.seed, bool),
            "seed is not an integer",
        )
        require(
            0 <= self.seed <= 2**63 - 1,
            "seed must be a non-negative signed 64-bit integer",
        )
        require(2 <= self.concurrency <= 8, "concurrency must be in 2..8")
        require(
            1 <= self.operation_timeout_ms <= 120_000,
            "operation timeout must be in 1..120000 ms",
        )
        require(
            0.1 <= self.trial_timeout_seconds <= 3_600.0,
            "trial timeout must be in 0.1..3600 seconds",
        )
        require(
            1.0 <= self.campaign_timeout_seconds <= 86_400.0,
            "campaign timeout must be in 1..86400 seconds",
        )


class WorkerProcess:
    """Bounded framed-protocol client for one real native worker process."""

    def __init__(
        self,
        binary: Path,
        state_root: Path,
        *,
        timeout_ms: int,
        test_double: bool = False,
        deadline: float | None = None,
    ) -> None:
        self.binary = binary
        self.state_root = state_root
        self.timeout_ms = timeout_ms
        self.test_double = test_double
        self.deadline = deadline
        self.process: subprocess.Popen[bytes] | None = None
        self.next_id = 1
        self.operations: list[dict[str, Any]] = []
        self.started_at_unix_ns = time.time_ns()
        self.started_at_monotonic = time.monotonic()
        self.closed = False
        if deadline is not None:
            self._remaining(deadline)
        self.start()

    def start(self) -> None:
        """Start the worker with only the state-root argument."""

        require(
            self.binary.is_absolute(), f"worker path must be absolute: {self.binary}"
        )
        require(
            self.state_root.is_absolute(),
            f"state root must be absolute: {self.state_root}",
        )
        arguments = [str(self.binary), "--state-root", str(self.state_root)]
        if self.test_double:
            arguments.append("--test-double")
        environment = os.environ.copy()
        environment["PATH"] = ""
        try:
            self.process = subprocess.Popen(
                arguments,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
                env=environment,
            )
        except OSError as error:
            raise StressFailure(
                f"could not start worker {self.binary}: {error}"
            ) from error

    def _require_pipes(self) -> tuple[subprocess.Popen[bytes], int, int]:
        process = self.process
        require(process is not None, "worker process was not started")
        require(
            process.stdin is not None and process.stdout is not None,
            "worker pipes are unavailable",
        )
        return process, process.stdin.fileno(), process.stdout.fileno()

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DeadlineExceeded("worker operation deadline expired")
        return remaining

    def remaining_seconds(self) -> float | None:
        """Return remaining session budget, if this worker has one."""

        if self.deadline is None:
            return None
        return self._remaining(self.deadline)

    def _write_frame(self, frame: bytes, deadline: float) -> None:
        process, input_fd, _ = self._require_pipes()
        del process
        offset = 0
        while offset < len(frame):
            try:
                _readable, writable, _exceptional = select.select(
                    [], [input_fd], [], self._remaining(deadline)
                )
                if not writable:
                    raise DeadlineExceeded(
                        "worker request pipe did not become writable before deadline"
                    )
                written = os.write(input_fd, frame[offset:])
            except BrokenPipeError as error:
                raise StressFailure(
                    f"worker closed request pipe: {self._stderr_tail()}"
                ) from error
            except OSError as error:
                raise StressFailure(
                    f"worker request write failed: {error}; {self._stderr_tail()}"
                ) from error
            require(written > 0, "worker request write made no progress")
            offset += written

    def _read_exact(self, size: int, deadline: float) -> bytes:
        _process, _input_fd, output_fd = self._require_pipes()
        chunks: list[bytes] = []
        remaining_bytes = size
        while remaining_bytes:
            try:
                readable, _writable, _exceptional = select.select(
                    [output_fd], [], [], self._remaining(deadline)
                )
                if not readable:
                    raise DeadlineExceeded(
                        "worker reply pipe did not become readable before deadline"
                    )
                chunk = os.read(output_fd, remaining_bytes)
            except OSError as error:
                raise StressFailure(
                    f"worker reply read failed: {error}; {self._stderr_tail()}"
                ) from error
            if not chunk:
                raise StressFailure(
                    f"worker closed before a complete reply: {self._stderr_tail()}"
                )
            chunks.append(chunk)
            remaining_bytes -= len(chunk)
        return b"".join(chunks)

    def _stderr_tail(self) -> str:
        process = self.process
        if process is None or process.stderr is None:
            return ""
        if process.poll() is None:
            return ""
        try:
            return process.stderr.read().decode("utf-8", errors="replace")[-2_000:]
        except OSError:
            return ""

    def call(
        self,
        operation: str,
        namespace: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """Send one request and retain exact request/reply hashes.

        A request is recorded before its frame is written.  If the caller's
        deadline expires while the native worker is still processing, that
        in-flight row remains in the session receipt with its exact request
        hash and the observed partial effect metadata.
        """

        require(not self.closed, "worker session is already closed")
        require(
            operation in {"handshake", "health", "observe", "recall", "inspection"},
            f"unsupported stress operation: {operation}",
        )
        effective_timeout_ms = self.timeout_ms if timeout_ms is None else timeout_ms
        require(
            isinstance(effective_timeout_ms, int)
            and not isinstance(effective_timeout_ms, bool)
            and 1 <= effective_timeout_ms <= 120_000,
            "worker operation timeout was outside its bounded range",
        )
        request_id = self.next_id
        self.next_id += 1
        wire_name = wire_namespace(namespace)
        request = {
            "protocol_version": PROTOCOL_VERSION,
            "id": request_id,
            "deadline_ms": effective_timeout_ms,
            "op": operation,
            "namespace": wire_name,
            "payload": payload or {},
        }
        request_body = stable_json_bytes(request)
        require(
            len(request_body) <= MAX_REQUEST_BYTES,
            "worker request exceeds the protocol byte bound",
        )
        operation_deadline = time.monotonic() + effective_timeout_ms / 1000.0
        if self.deadline is not None:
            operation_deadline = min(operation_deadline, self.deadline)
        remaining = self._remaining(operation_deadline)
        request["deadline_ms"] = max(
            1,
            min(effective_timeout_ms, int(remaining * 1000)),
        )
        request_body = stable_json_bytes(request)
        require(
            len(request_body) <= MAX_REQUEST_BYTES,
            "worker request exceeds the protocol byte bound",
        )
        started_at = time.time_ns()
        operation_evidence = {
            "id": request_id,
            "op": operation,
            "namespace": namespace,
            "wire_namespace": wire_name,
            "deadline_ms": request["deadline_ms"],
            "request_sha256": sha256_bytes(request_body),
            "request_bytes": len(request_body),
            "completed": False,
            "outcome": "in_flight",
            "started_at_unix_ns": started_at,
            "finished_at_unix_ns": None,
        }
        self.operations.append(operation_evidence)
        try:
            self._write_frame(
                struct.pack(">I", len(request_body)) + request_body,
                operation_deadline,
            )
            header = self._read_exact(4, operation_deadline)
            length = struct.unpack(">I", header)[0]
            require(
                0 < length <= MAX_REPLY_BYTES,
                f"worker reply length {length} exceeds the protocol bound",
            )
            body = self._read_exact(length, operation_deadline)
            reply = parse_reply(body)
            require(
                reply.get("id") == request_id,
                f"worker reply id {reply.get('id')} did not echo {request_id}",
            )
        except BaseException as error:
            operation_evidence.update(
                {
                    "completed": False,
                    "outcome": (
                        "deadline_exceeded"
                        if isinstance(error, DeadlineExceeded)
                        else "failed"
                    ),
                    "error_type": type(error).__name__,
                    "error_code": getattr(error, "error_code", None),
                    "error": str(error),
                    "finished_at_unix_ns": time.time_ns(),
                }
            )
            operation_evidence["duration_ms"] = int(
                (time.time_ns() - started_at) / 1_000_000
            )
            operation_evidence["evidence_sha256"] = operation_digest(operation_evidence)
            raise
        operation_evidence.update(
            {
                "completed": True,
                "reply_sha256": sha256_bytes(body),
                "reply_bytes": len(body),
                "outcome": outcome_name(reply),
                "state_generation": reply.get("state_generation"),
                "finished_at_unix_ns": time.time_ns(),
            }
        )
        operation_evidence["duration_ms"] = int(
            (operation_evidence["finished_at_unix_ns"] - started_at) / 1_000_000
        )
        operation_evidence["evidence_sha256"] = operation_digest(operation_evidence)
        return reply

    def close(
        self,
        *,
        suppress_errors: bool = False,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Close the pipe, reap the child, and retain bounded stderr evidence."""

        if self.closed:
            return getattr(self, "close_evidence", {})
        self.closed = True
        process = self.process
        if process is None:
            self.close_evidence = {"returncode": None}
            return self.close_evidence
        if timeout_seconds is None:
            timeout_seconds = SESSION_CLEANUP_TIMEOUT_SECONDS
        if timeout_seconds is not None:
            require(
                isinstance(timeout_seconds, (int, float))
                and not isinstance(timeout_seconds, bool)
                and timeout_seconds > 0,
                "worker cleanup timeout must be positive",
            )
        if process.stdin is not None:
            try:
                process.stdin.close()
            except BaseException as error:
                close_error = error
            else:
                close_error = None
        else:
            close_error = None
        forced = False
        cleanup_deadline = (
            time.monotonic() + timeout_seconds if timeout_seconds is not None else None
        )

        def bounded_timeout(default: float) -> float:
            if cleanup_deadline is None:
                return default
            return max(0.0, min(default, cleanup_deadline - time.monotonic()))

        returncode = process.poll()

        def force_reap() -> int | None:
            nonlocal forced, close_error
            forced = True
            try:
                process.terminate()
            except BaseException as error:
                close_error = close_error or error
            try:
                wait_timeout = bounded_timeout(5.0)
                if wait_timeout <= 0:
                    return process.poll()
                return process.wait(timeout=wait_timeout)
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                except BaseException as error:
                    close_error = close_error or error
                try:
                    wait_timeout = bounded_timeout(5.0)
                    if wait_timeout <= 0:
                        return process.poll()
                    return process.wait(timeout=wait_timeout)
                except BaseException as error:
                    close_error = close_error or error
                    return process.poll()
            except BaseException as error:
                close_error = close_error or error
                return process.poll()

        if returncode is None:
            try:
                wait_timeout = bounded_timeout(
                    min(10.0, max(1.0, self.timeout_ms / 1000.0))
                )
                if wait_timeout <= 0:
                    returncode = force_reap()
                else:
                    returncode = process.wait(timeout=wait_timeout)
            except subprocess.TimeoutExpired:
                returncode = force_reap()
            except BaseException as error:
                close_error = close_error or error
                returncode = force_reap()
        if returncode is None:
            returncode = process.poll()
        if returncode is None:
            try:
                process.kill()
            except BaseException as error:
                close_error = close_error or error
            try:
                wait_timeout = bounded_timeout(5.0)
                if wait_timeout <= 0:
                    returncode = process.poll()
                else:
                    returncode = process.wait(timeout=wait_timeout)
            except BaseException as error:
                close_error = close_error or error
                returncode = process.poll()
        if returncode is None:
            close_error = close_error or DeadlineExceeded(
                "worker process could not be reaped before cleanup completed"
            )
        stderr = b""
        if process.stderr is not None and returncode is not None:
            try:
                if cleanup_deadline is None:
                    stderr = process.stderr.read()
                else:
                    fd = process.stderr.fileno()
                    os.set_blocking(fd, False)
                    chunks: list[bytes] = []
                    while bounded_timeout(0.05) > 0:
                        readable, _writable, _exceptional = select.select(
                            [fd], [], [], bounded_timeout(0.05)
                        )
                        if not readable:
                            break
                        chunk = os.read(fd, 8_192)
                        if not chunk:
                            break
                        chunks.append(chunk)
                    stderr = b"".join(chunks)
            except BaseException as error:
                close_error = close_error or error
                stderr = b""
        self.close_evidence = {
            "pid": process.pid,
            "returncode": returncode,
            "forced_termination": forced,
            "stderr_tail": stderr.decode("utf-8", errors="replace")[-2_000:],
            "duration_ms": int((time.monotonic() - self.started_at_monotonic) * 1000),
        }
        if close_error is not None and not suppress_errors:
            raise StressFailure(
                f"worker close failed: {close_error}; {self.close_evidence['stderr_tail']}"
            ) from close_error
        if returncode != 0 and not suppress_errors:
            raise StressFailure(
                f"worker exited {returncode}: {self.close_evidence['stderr_tail']}"
            )
        return self.close_evidence


class SingleOwnerDispatcher:
    """Dispatch concurrent callers through one bounded worker owner.

    The production Rust NCM client exposes one bounded owner mailbox and one
    worker process per state root. The concurrent phase mirrors that contract:
    client threads submit recall actions here, while this one owner thread is
    the only thread that touches the worker pipe.
    """

    def __init__(self, worker: WorkerProcess) -> None:
        self.worker = worker
        self._commands: Queue[Any] = Queue(maxsize=OWNER_MAILBOX_MAX_REQUESTS)
        self._state_lock = threading.Lock()
        self._closed = False
        self._stop = object()
        self._batch_event: threading.Event | None = None
        self._batch_expected = 0
        self._batch_submitted = 0
        self._batch_released_at_monotonic: float | None = None
        self._max_queue_depth = 0
        self._owner_thread_ids: set[int] = set()
        self._batch_caller_thread_ids: set[int] = set()
        self._namespace_owner_thread_ids: dict[str, set[int]] = {}
        self._thread = threading.Thread(
            target=self._run,
            name="ncm-stress-worker-owner",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        with self._state_lock:
            self._owner_thread_ids.add(threading.get_ident())
        while True:
            action, future, is_batch = self._commands.get()
            operation_start = (
                len(self.worker.operations)
                if isinstance(getattr(self.worker, "operations", None), list)
                else None
            )
            owner_thread_id = threading.get_ident()
            try:
                if action is self._stop:
                    return
                if future.cancelled():
                    continue
                try:
                    if is_batch and self._batch_event is not None:
                        timeout = self.worker.remaining_seconds()
                        if timeout is None:
                            self._batch_event.wait()
                        elif not self._batch_event.wait(timeout=timeout):
                            raise DeadlineExceeded(
                                "concurrent client batch exceeded its worker deadline"
                            )
                    future.set_result(action(self.worker))
                except BaseException as error:
                    if not future.done():
                        future.set_exception(error)
            finally:
                if operation_start is not None:
                    operations = self.worker.operations
                    if isinstance(operations, list):
                        with self._state_lock:
                            for operation in operations[operation_start:]:
                                namespace_id = (
                                    operation.get("wire_namespace")
                                    if isinstance(operation, dict)
                                    and operation.get("op")
                                    in {"observe", "recall", "inspection"}
                                    else None
                                )
                                if isinstance(namespace_id, str):
                                    self._namespace_owner_thread_ids.setdefault(
                                        namespace_id, set()
                                    ).add(owner_thread_id)
                self._commands.task_done()

    def begin_batch(self, expected_calls: int) -> None:
        """Gate the next batch until every concurrent client has submitted."""

        require(
            2 <= expected_calls <= 8,
            "concurrent owner batch requires 2..8 callers",
        )
        with self._state_lock:
            require(not self._closed, "worker owner dispatcher is closed")
            require(
                self._batch_event is None,
                "worker owner already has an active concurrent batch",
            )
            self._batch_expected = expected_calls
            self._batch_submitted = 0
            self._batch_event = threading.Event()

    def call(
        self,
        action: Callable[[WorkerProcess], Any],
        *,
        batch: bool = False,
    ) -> Any:
        """Queue one client action and wait for its serialized result."""

        future: Future[Any] = Future()
        with self._state_lock:
            require(not self._closed, "worker owner dispatcher is closed")
            if batch:
                require(
                    self._batch_event is not None,
                    "worker owner batch was not started",
                )
                self._batch_caller_thread_ids.add(threading.get_ident())
                self._batch_submitted += 1
                if self._batch_submitted >= self._batch_expected:
                    self._batch_released_at_monotonic = time.monotonic()
                    self._batch_event.set()
            try:
                self._commands.put_nowait((action, future, batch))
            except Full as error:
                raise StressFailure(
                    "worker owner mailbox exceeded its bounded request capacity"
                ) from error
            self._max_queue_depth = max(
                self._max_queue_depth,
                self._commands.qsize(),
            )
        timeout = self.worker.remaining_seconds()
        try:
            if timeout is None:
                return future.result()
            return future.result(timeout=timeout)
        except FutureTimeoutError as error:
            raise DeadlineExceeded(
                "concurrent client action exceeded its worker deadline"
            ) from error

    def stats(self) -> dict[str, Any]:
        """Return bounded dispatcher evidence for the current concurrent batch."""

        with self._state_lock:
            process = getattr(self.worker, "process", None)
            worker_pid = getattr(process, "pid", None)
            worker_pids = (
                [worker_pid]
                if (
                    isinstance(worker_pid, int)
                    and not isinstance(worker_pid, bool)
                    and worker_pid > 0
                )
                else []
            )
            owner_thread_ids = sorted(self._owner_thread_ids)
            namespace_owner_bindings = [
                {
                    "namespace_id": namespace_id,
                    "owner_thread_ids": sorted(owner_ids),
                    "worker_pids": worker_pids,
                }
                for namespace_id, owner_ids in sorted(
                    self._namespace_owner_thread_ids.items()
                )
            ]
            operations = getattr(self.worker, "operations", [])
            namespace_ids = (
                sorted(
                    {
                        operation.get("wire_namespace")
                        for operation in operations
                        if isinstance(operation, dict)
                        and operation.get("op") in {"observe", "recall", "inspection"}
                        and isinstance(operation.get("wire_namespace"), str)
                    }
                )
                if isinstance(operations, list)
                else []
            )
            return {
                "owner_threads": len(owner_thread_ids),
                "owner_thread_ids": owner_thread_ids,
                "owner_alive": self._thread.is_alive(),
                "namespace_owner_bindings": namespace_owner_bindings,
                "worker_pids": worker_pids,
                "worker_processes": len(set(worker_pids)),
                "worker_alive": (
                    process is not None and process.poll() is None
                    if process is not None
                    else False
                ),
                "operation_namespace_ids": namespace_ids,
                "batch_expected": self._batch_expected,
                "batch_submitted": self._batch_submitted,
                "batch_caller_thread_ids": sorted(self._batch_caller_thread_ids),
                "batch_client_callers": len(self._batch_caller_thread_ids),
                "max_queue_depth": self._max_queue_depth,
                "batch_released": self._batch_released_at_monotonic is not None,
                "client_overlap": (
                    self._batch_submitted == self._batch_expected
                    and self._batch_released_at_monotonic is not None
                    and len(self._batch_caller_thread_ids) == self._batch_expected
                ),
            }

    def close(self) -> None:
        """Drain queued calls and stop the single owner thread."""

        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            if self._batch_event is not None:
                self._batch_event.set()
        close_error: BaseException | None = None
        timeout = min(
            DISPATCHER_CLOSE_TIMEOUT_SECONDS,
            max(0.05, self.worker.timeout_ms / 1000.0),
        )
        cleanup_budget = timeout
        deadline = time.monotonic() + cleanup_budget

        def cancel_pending(error: BaseException) -> None:
            while True:
                try:
                    _action, future, _is_batch = self._commands.get_nowait()
                except Empty:
                    return
                try:
                    if future is not None and not future.done():
                        future.set_exception(error)
                finally:
                    self._commands.task_done()

        try:
            while self._commands.unfinished_tasks:
                if time.monotonic() >= deadline:
                    raise DeadlineExceeded(
                        "worker owner dispatcher did not drain before its deadline"
                    )
                time.sleep(min(0.01, max(0.001, deadline - time.monotonic())))
        except BaseException as error:
            close_error = close_error or error
            cancel_pending(error)
        try:
            self._commands.put_nowait((self._stop, None, False))
        except Full as error:
            close_error = close_error or error
            cancel_pending(error)
            try:
                self._commands.put_nowait((self._stop, None, False))
            except BaseException as second_error:
                close_error = close_error or second_error
        remaining_join = max(0.0, deadline - time.monotonic())
        self._thread.join(timeout=remaining_join)
        if self._thread.is_alive():
            try:
                self.worker.close(
                    suppress_errors=True,
                    timeout_seconds=max(0.05, deadline - time.monotonic()),
                )
            except BaseException as error:
                close_error = close_error or error
            self._thread.join(timeout=max(0.05, deadline - time.monotonic()))
        if self._thread.is_alive():
            close_error = close_error or DeadlineExceeded(
                "worker owner thread did not stop before its bounded deadline"
            )
        if close_error is not None:
            raise close_error

    def __enter__(self) -> SingleOwnerDispatcher:
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        try:
            self.close()
        except BaseException:
            if _value is None:
                raise


def concurrent_topology(concurrency: int) -> dict[str, Any]:
    """Describe the supported one-owner topology used by concurrent trials."""

    require(2 <= concurrency <= 8, "concurrent topology requires 2..8 callers")
    return {
        "client_callers": concurrency,
        "dispatch": "bounded_single_owner_mailbox",
        "mailbox_capacity": OWNER_MAILBOX_MAX_REQUESTS,
    }


def measured_concurrent_topology(
    topology: dict[str, Any],
    dispatcher: dict[str, Any],
    namespace_ids: Sequence[str],
) -> dict[str, Any]:
    """Bind the supported topology description to observed runtime IDs."""

    observed_namespace_ids = dispatcher.get("operation_namespace_ids")
    if isinstance(observed_namespace_ids, list) and observed_namespace_ids:
        unique_namespace_ids = sorted(
            {value for value in observed_namespace_ids if isinstance(value, str)}
        )
    else:
        unique_namespace_ids = sorted(set(namespace_ids))
    owner_ids = [
        value
        for value in dispatcher.get("owner_thread_ids", [])
        if isinstance(value, int)
    ]
    worker_pids = [
        value for value in dispatcher.get("worker_pids", []) if isinstance(value, int)
    ]
    observed_bindings = dispatcher.get("namespace_owner_bindings")
    if isinstance(observed_bindings, list):
        namespace_owner_bindings = [
            {
                "namespace_id": item.get("namespace_id"),
                "owner_thread_ids": sorted(
                    {
                        value
                        for value in item.get("owner_thread_ids", [])
                        if isinstance(value, int)
                    }
                ),
                "worker_pids": sorted(
                    {
                        value
                        for value in item.get("worker_pids", [])
                        if isinstance(value, int)
                    }
                ),
            }
            for item in observed_bindings
            if isinstance(item, dict) and isinstance(item.get("namespace_id"), str)
        ]
        binding_owner_ids = {
            value
            for item in namespace_owner_bindings
            for value in item["owner_thread_ids"]
        }
        if binding_owner_ids:
            owner_ids = sorted(binding_owner_ids)
    else:
        namespace_owner_bindings = [
            {
                "namespace_id": namespace_id,
                "owner_thread_ids": owner_ids,
                "worker_pids": worker_pids,
            }
            for namespace_id in unique_namespace_ids
        ]
    measured = {
        **topology,
        **dispatcher,
        "client_callers": dispatcher.get(
            "batch_client_callers", topology["client_callers"]
        ),
        "namespace_ids": unique_namespace_ids,
        "namespace_count": len(unique_namespace_ids),
        "namespace_owner_ids": sorted(set(owner_ids)),
        "namespace_owners": len(set(owner_ids)),
        "namespace_owner_bindings": namespace_owner_bindings,
        "worker_pids": sorted(set(worker_pids)),
        "worker_processes": len(set(worker_pids)),
    }
    return measured


def require_success(reply: dict[str, Any], label: str) -> None:
    """Require an available successful worker operation."""

    outcome = outcome_name(reply)
    require(
        outcome == "success",
        f"{label} returned {outcome}: {reply.get('error') or reply.get('payload')}",
    )


def inspect_state(
    worker: WorkerProcess, namespace: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read and validate one durable inspection payload."""

    reply = worker.call("inspection", namespace, {})
    require_success(reply, "inspection")
    payload = reply.get("payload")
    require(isinstance(payload, dict), "inspection payload was not an object")
    state_digest = payload.get("state_digest")
    require(
        isinstance(state_digest, str) and HEX_64.fullmatch(state_digest),
        "inspection omitted a valid state digest",
    )
    commit_seq = payload.get("commit_seq")
    require(
        isinstance(commit_seq, int) and not isinstance(commit_seq, bool),
        "inspection omitted commit_seq",
    )
    records = payload.get("records")
    require(
        isinstance(records, int) and not isinstance(records, bool) and records >= 0,
        "inspection omitted records count",
    )
    evidence = {
        "reply": reply,
        "payload": payload,
        "operation": worker.operations[-1],
        "state_digest": state_digest,
        "commit_seq": commit_seq,
        "records": records,
    }
    return evidence, payload


def _expected_wire_identity(
    admitted_identity: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if admitted_identity is None:
        return None
    expected = admitted_identity.get("expected_wire_identity", admitted_identity)
    require(isinstance(expected, dict), "admitted worker identity was not an object")
    require(
        set(expected) == {"identity_revision", "algorithm", "worker", "encoder"},
        "admitted worker identity was incomplete",
    )
    require(
        expected.get("identity_revision") == IDENTITY_REVISION_V2,
        "admitted worker identity revision was unsupported",
    )
    require(
        expected.get("algorithm")
        == {
            "profile": ALGORITHM_PROFILE,
            "config_sha256": ALGORITHM_CONFIG_SHA256,
        },
        "admitted algorithm identity was stale",
    )
    _validate_worker_wire_identity(expected.get("worker"), label="admitted")
    _validate_encoder_wire_identity(expected.get("encoder"), label="admitted")
    return expected


def _validate_worker_wire_identity(value: Any, *, label: str) -> dict[str, Any]:
    require(isinstance(value, dict), f"{label} worker identity was not an object")
    require(
        set(value) == {"sha256", "bytes", "target"},
        f"{label} worker identity was incomplete",
    )
    digest = value.get("sha256")
    require(
        isinstance(digest, str) and HEX_64.fullmatch(digest),
        f"{label} worker identity omitted a valid digest",
    )
    size = value.get("bytes")
    require(
        isinstance(size, int) and not isinstance(size, bool) and size > 0,
        f"{label} worker identity omitted a valid byte count",
    )
    target = value.get("target")
    require(isinstance(target, dict), f"{label} worker target was incomplete")
    require(
        set(target) == {"triple", "os", "arch", "family"}
        and all(isinstance(target[field], str) and target[field] for field in target),
        f"{label} worker target was incomplete",
    )
    return value


def _validate_encoder_wire_identity(value: Any, *, label: str) -> dict[str, Any]:
    require(isinstance(value, dict), f"{label} encoder identity was not an object")
    required = {
        "model",
        "artifact_sha256",
        "repository",
        "revision",
        "revision_provenance",
        "files",
        "max_length",
        "pooling",
        "normalize",
    }
    require(set(value) == required, f"{label} encoder identity was incomplete")
    require(value.get("model") == MODEL_NAME, f"{label} encoder model was stale")
    artifact = value.get("artifact_sha256")
    require(
        isinstance(artifact, str) and HEX_64.fullmatch(artifact),
        f"{label} encoder artifact digest was invalid",
    )
    require(
        value.get("repository") == MODEL_REPOSITORY,
        f"{label} encoder repository was stale",
    )
    require(
        value.get("revision") == MODEL_REVISION,
        f"{label} encoder revision was stale",
    )
    require(
        value.get("revision_provenance") == MODEL_REVISION_PROVENANCE,
        f"{label} encoder revision provenance was stale",
    )
    require(
        value.get("max_length") == REAL_MODEL_MAX_LENGTH,
        f"{label} encoder max length was stale",
    )
    require(value.get("pooling") == MODEL_POOLING, f"{label} encoder pooling was stale")
    require(
        value.get("normalize") is MODEL_NORMALIZE,
        f"{label} encoder normalization was stale",
    )
    files = value.get("files")
    require(isinstance(files, list), f"{label} encoder files were incomplete")
    require(
        len(files) == len(MODEL_REQUIRED_FILES),
        f"{label} encoder files had the wrong count",
    )
    paths: set[str] = set()
    for item in files:
        require(isinstance(item, dict), f"{label} encoder file was not an object")
        require(
            set(item) == {"path", "sha256", "bytes"},
            f"{label} encoder file was incomplete",
        )
        path = item.get("path")
        require(
            isinstance(path, str)
            and path in MODEL_REQUIRED_FILES
            and path not in paths,
            f"{label} encoder file path was unsafe or repeated",
        )
        paths.add(path)
        require(
            isinstance(item.get("sha256"), str) and HEX_64.fullmatch(item["sha256"]),
            f"{label} encoder file digest was invalid",
        )
        require(
            isinstance(item.get("bytes"), int)
            and not isinstance(item["bytes"], bool)
            and item["bytes"] > 0,
            f"{label} encoder file size was invalid",
        )
    require(
        paths == set(MODEL_REQUIRED_FILES), f"{label} encoder files were incomplete"
    )
    onnx = next(item for item in files if item["path"] == "onnx/model.onnx")
    require(
        onnx["sha256"] == artifact,
        f"{label} encoder artifact did not match onnx/model.onnx",
    )
    return value


def handshake(
    worker: WorkerProcess,
    namespace: str = "0" * 64,
    *,
    admitted_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Perform and validate the complete production V2 handshake."""

    expected = _expected_wire_identity(admitted_identity)
    request_payload: dict[str, Any] = {
        "protocol_version": PROTOCOL_VERSION,
        "protocol_identity": PROTOCOL_IDENTITY,
        "algorithm_profile": ALGORITHM_PROFILE,
        "model": MODEL_NAME,
    }
    if expected is not None:
        request_payload.update(
            {
                "identity_revision": IDENTITY_REVISION_V2,
                "algorithm": expected["algorithm"],
                "worker": expected["worker"],
                "encoder": expected["encoder"],
            }
        )
    reply = worker.call("handshake", namespace, request_payload)
    require_success(reply, "handshake")
    payload = reply.get("payload")
    require(isinstance(payload, dict), "handshake payload was not an object")
    algorithm = payload.get("algorithm")
    encoder = payload.get("encoder")
    require(isinstance(algorithm, dict), "handshake omitted algorithm identity")
    require(isinstance(encoder, dict), "handshake omitted encoder identity")
    require(
        payload.get("identity_revision") == IDENTITY_REVISION_V2,
        "production handshake did not prove complete V2 identity",
    )
    require(
        algorithm.get("profile") == ALGORITHM_PROFILE,
        f"unexpected algorithm profile: {algorithm.get('profile')}",
    )
    require(
        payload.get("ready") is True,
        "handshake did not prove worker readiness",
    )
    worker_identity = _validate_worker_wire_identity(
        payload.get("worker"), label="handshake"
    )
    encoder_identity = _validate_encoder_wire_identity(encoder, label="handshake")
    config_sha256 = algorithm.get("config_sha256")
    require(
        isinstance(config_sha256, str) and HEX_64.fullmatch(config_sha256),
        "handshake omitted algorithm config digest",
    )
    projection = payload.get("projection_sha256")
    require(
        isinstance(projection, str) and HEX_64.fullmatch(projection),
        "handshake omitted projection digest",
    )
    epoch = payload.get("epoch")
    require(
        isinstance(epoch, int) and not isinstance(epoch, bool) and epoch >= 0,
        "handshake omitted epoch",
    )
    if expected is not None:
        require(
            payload.get("algorithm") == expected["algorithm"],
            "handshake algorithm identity did not match the admitted identity",
        )
        require(
            worker_identity == expected["worker"],
            "handshake worker identity did not match the admitted artifact",
        )
        require(
            encoder_identity == expected["encoder"],
            "handshake encoder identity did not match the admitted model",
        )
    return {
        "reply": reply,
        "payload": payload,
        "operation": worker.operations[-1],
        "identity_revision": payload.get("identity_revision"),
        "ready": payload.get("ready"),
        "algorithm": algorithm,
        "algorithm_profile": algorithm.get("profile"),
        "config_sha256": config_sha256,
        "worker": worker_identity,
        "encoder": encoder_identity,
        "model": encoder_identity.get("model"),
        "artifact_sha256": encoder_identity.get("artifact_sha256"),
        "projection_sha256": payload.get("projection_sha256"),
        "epoch": epoch,
    }


def health(
    worker: WorkerProcess,
    *,
    admitted_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Require process and encoder readiness from the production worker."""

    reply = worker.call("health", "0" * 64, {})
    require_success(reply, "health")
    payload = reply.get("payload")
    require(isinstance(payload, dict), "health payload was not an object")
    require(
        payload.get("process_alive") is True,
        "worker health did not prove process_alive",
    )
    require(
        payload.get("encoder_ready") is True,
        "worker health did not prove encoder_ready",
    )
    encoder = payload.get("encoder")
    require(isinstance(encoder, dict), "health omitted encoder identity")
    require(
        encoder.get("model") == MODEL_NAME,
        "health reported a non-production model",
    )
    expected = _expected_wire_identity(admitted_identity)
    if expected is not None:
        expected_encoder = expected["encoder"]
        for field in ("model", "artifact_sha256", "max_length"):
            require(
                encoder.get(field) == expected_encoder[field],
                f"health {field} did not match the admitted model",
            )
    require(
        isinstance(encoder.get("artifact_sha256"), str)
        and HEX_64.fullmatch(encoder["artifact_sha256"]),
        "health omitted a real encoder artifact digest",
    )
    require(
        encoder.get("max_length") == REAL_MODEL_MAX_LENGTH,
        "health reported a stale encoder max length",
    )
    health_operation = worker.operations[-1]
    v2_identity = None
    if expected is not None:
        # The production health payload intentionally exposes a compact encoder
        # summary. Pair it with a fresh complete V2 handshake so the health
        # receipt binds the live process to every admitted worker/model field.
        v2_identity = handshake(worker, admitted_identity=admitted_identity)
    return {
        "reply": reply,
        "payload": payload,
        "process_alive": payload.get("process_alive"),
        "encoder_ready": payload.get("encoder_ready"),
        "encoder": encoder,
        "operation": health_operation,
        "v2_identity": v2_identity,
        "v2_operation": v2_identity["operation"] if v2_identity is not None else None,
    }


def observation_payload(
    *,
    phase: str,
    trial_index: int,
    seed: int,
    ordinal: int = 0,
    query_text: str | None = None,
) -> dict[str, Any]:
    """Create one deterministic effect payload in Rust struct-field order."""

    source = f"ncm-production-stress/{phase}/{trial_index}/{ordinal}"
    key_text = query_text or (
        f"NCM stress {phase} trial {trial_index} seed {seed} preserves the "
        f"durable marker {ordinal}."
    )
    value_text = f"durable-ncm-marker/{phase}/{trial_index}/{seed}/{ordinal}"
    provenance = {"campaign": CAMPAIGN_NAME, "seed": seed, "trial": trial_index}
    effect = {
        "source": source,
        "key_text": key_text,
        "value_text": value_text,
        "affect": None,
        "surprise": 0.95,
        "intensity": 0.95,
        "provenance": provenance,
    }
    return {
        "idempotency_key": f"ncm-production-stress/{phase}/{trial_index}/{seed}/{ordinal}",
        "payload_sha256": sha256_bytes(stable_json_bytes(effect, sort_keys=False)),
        "source": source,
        "key_text": key_text,
        "value_text": value_text,
        "affect": None,
        "surprise": 0.95,
        "intensity": 0.95,
        "provenance": provenance,
    }


def _cancellation_workload_text(prefix: str) -> str:
    """Build one deterministic, model-sized UTF-8 workload without padding fields."""

    require(prefix.isascii(), "cancellation workload prefix must be ASCII")
    chunk = f"{prefix} real production encoder workload "
    repetitions = (CANCELLATION_WORKLOAD_BYTES + len(chunk) - 1) // len(chunk)
    workload = (chunk * repetitions)[:CANCELLATION_WORKLOAD_BYTES]
    require(
        len(workload.encode("utf-8")) == CANCELLATION_WORKLOAD_BYTES,
        "cancellation workload did not reach its bounded byte size",
    )
    return workload


def cancellation_payload(*, seed: int) -> dict[str, Any]:
    """Create a real-encoder observe that occupies the production work path.

    The worker has no production delay hook. Filling both accepted text inputs
    to the store's 16 KiB record bound forces real tokenization/inference and
    durable validation under the five-millisecond request deadline. The
    payload remains below the framed-request bound and contains no test-only
    control fields.
    """

    payload = observation_payload(
        phase="cancellation", trial_index=0, seed=seed, ordinal=99
    )
    payload["key_text"] = _cancellation_workload_text("ncm-cancel-key")
    payload["value_text"] = _cancellation_workload_text("ncm-cancel-value")
    require(
        len(payload["key_text"].encode("utf-8"))
        + len(payload["value_text"].encode("utf-8"))
        == 2 * CANCELLATION_WORKLOAD_BYTES,
        "cancellation observe exceeded the production record byte bound",
    )
    effect = {
        "source": payload["source"],
        "key_text": payload["key_text"],
        "value_text": payload["value_text"],
        "affect": payload["affect"],
        "surprise": payload["surprise"],
        "intensity": payload["intensity"],
        "provenance": payload["provenance"],
    }
    payload["payload_sha256"] = sha256_bytes(stable_json_bytes(effect, sort_keys=False))
    return payload


def observe_record(
    worker: WorkerProcess,
    namespace: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Observe one record and prove its committed generation in inspection."""

    reply = worker.call("observe", namespace, payload)
    require_success(reply, "observe")
    body = reply.get("payload")
    require(isinstance(body, dict), "observe payload was not an object")
    record_id = body.get("record_id")
    require(
        isinstance(record_id, int)
        and not isinstance(record_id, bool)
        and record_id > 0,
        "observe omitted record_id",
    )
    require(
        body.get("replayed") is False,
        "observe was replayed unexpectedly in a fresh trial",
    )
    inspected, _ = inspect_state(worker, namespace)
    require(
        inspected["commit_seq"] == reply.get("state_generation"),
        "observe ACK generation disagreed with durable inspection",
    )
    require(
        inspected["records"] >= 1, "durable inspection did not show the observed record"
    )
    return {
        "request": payload,
        "payload_sha256": payload.get("payload_sha256"),
        "reply": reply,
        "operation": worker.operations[-2],
        "record_id": record_id,
        "state_generation": reply.get("state_generation"),
        "inspection_after_observe": inspected,
    }


def scope_peer_payloads(
    *, phase: str, trial_index: int, seed: int, query_text: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Create two populated peer records with the same query and unique values."""

    return tuple(
        observation_payload(
            phase=f"{phase}-scope-peer-{ordinal}",
            trial_index=trial_index,
            seed=seed,
            ordinal=ordinal,
            query_text=query_text,
        )
        for ordinal in range(2)
    )


def replay_observe_record(
    worker: WorkerProcess,
    namespace: str,
    payload: dict[str, Any],
    original: dict[str, Any],
    *,
    label: str,
) -> dict[str, Any]:
    """Replay one exact observe request and prove it had no second effect."""

    require(
        original.get("request") == payload,
        f"{label} replay payload differed from the original request",
    )
    require(
        original.get("payload_sha256") == payload.get("payload_sha256"),
        f"{label} replay payload digest differed from the original request",
    )
    before, _ = inspect_state(worker, namespace)
    reply = worker.call("observe", namespace, payload)
    require_success(reply, f"{label} observe replay")
    body = reply.get("payload")
    require(isinstance(body, dict), f"{label} observe replay payload was not an object")
    require(
        body.get("replayed") is True,
        f"{label} observe replay was not marked replayed",
    )
    require(
        body.get("record_id") == original["record_id"],
        f"{label} observe replay returned another record",
    )
    require(
        reply.get("state_generation") == original["state_generation"],
        f"{label} observe replay changed the acknowledged generation",
    )
    after, _ = inspect_state(worker, namespace)
    require(
        after["state_digest"] == before["state_digest"],
        f"{label} observe replay changed the durable state digest",
    )
    require(
        after["commit_seq"] == before["commit_seq"],
        f"{label} observe replay changed the durable commit sequence",
    )
    return {
        "label": label,
        "request": payload,
        "payload_sha256": payload.get("payload_sha256"),
        "reply": reply,
        "operation": worker.operations[-2],
        "record_id": body["record_id"],
        "state_generation": reply.get("state_generation"),
        "inspection_before": before,
        "inspection_after": after,
        "effect_unchanged": True,
    }


def candidate_list(payload: Any) -> list[dict[str, Any]]:
    """Extract the externally tagged Candidates payload from Rust."""

    if not isinstance(payload, dict):
        return []
    candidates_payload = payload.get("Candidates")
    if not isinstance(candidates_payload, dict):
        candidates_payload = payload.get("candidates")
    if not isinstance(candidates_payload, dict):
        return []
    candidates = candidates_payload.get("candidates")
    if not isinstance(candidates, list):
        return []
    require(
        all(isinstance(candidate, dict) for candidate in candidates),
        "worker recall returned a non-object candidate",
    )
    return list(candidates)


def validate_candidate_scope(
    candidates: Sequence[dict[str, Any]],
    *,
    namespace: str,
    expected_source_id: str | None,
    label: str,
) -> dict[str, Any]:
    """Validate candidate provenance against the request's exact scope.

    The production recall row carries the admitted source ID.  Namespace is
    carried by the request envelope, and newer workers may also echo it on a
    candidate row.  Validate both when present so a foreign source or an
    incorrectly attributed namespace cannot be hidden behind a correct record
    ID or value.
    """

    wire_name = wire_namespace(namespace)
    source_ids: list[str] = []
    candidate_namespace_ids: list[str] = []
    for candidate in candidates:
        source = candidate.get("source")
        source_id = candidate.get("source_id")
        if "source" in candidate:
            require(
                isinstance(source, str) and bool(source),
                f"{label} candidate source ID was invalid",
            )
        if "source_id" in candidate:
            require(
                isinstance(source_id, str) and bool(source_id),
                f"{label} candidate source_id was invalid",
            )
        if source is not None and source_id is not None:
            require(
                source == source_id,
                f"{label} candidate source and source_id disagreed",
            )
        if source is None:
            source = source_id
        require(
            isinstance(source, str) and bool(source),
            f"{label} candidate omitted a non-empty source ID",
        )
        source_ids.append(source)
        if expected_source_id is not None:
            require(
                source == expected_source_id,
                f"{label} recall returned foreign source ID {source!r}; "
                f"expected {expected_source_id!r}",
            )

        namespace_value = candidate.get("namespace")
        namespace_id_value = candidate.get("namespace_id")
        namespace_values = [
            candidate[field]
            for field in ("namespace", "namespace_id")
            if field in candidate
        ]
        if "namespace" in candidate or "namespace_id" in candidate:
            require(
                all(
                    isinstance(value, str) and value in {namespace, wire_name}
                    for value in namespace_values
                ),
                f"{label} candidate returned a foreign namespace ID",
            )
            if namespace_value is not None and namespace_id_value is not None:
                require(
                    wire_namespace(namespace_value) == wire_name
                    and wire_namespace(namespace_id_value) == wire_name,
                    f"{label} candidate namespace aliases disagreed with the request",
                )
            candidate_namespace_ids.extend(namespace_values)
    return {
        "source_ids": source_ids,
        "namespace_ids": candidate_namespace_ids,
        "request_namespace": namespace,
        "request_wire_namespace": wire_name,
    }


def recall_record(
    worker: WorkerProcess,
    namespace: str,
    *,
    query_text: str,
    expected_record_id: int,
    expected_source_id: str | None = None,
    label: str,
) -> dict[str, Any]:
    """Recall exactly once and prove the expected record is returned."""

    before, _ = inspect_state(worker, namespace)
    reply = worker.call("recall", namespace, {"query_text": query_text, "top_k": 8})
    recall_operation = worker.operations[-1]
    require(
        recall_operation.get("op") == "recall"
        and recall_operation.get("namespace") == namespace
        and recall_operation.get("wire_namespace") == wire_namespace(namespace),
        f"{label} recall operation carried the wrong namespace ID",
    )
    if outcome_name(reply) != "success":
        raise RequiredRecordMiss(
            f"{label} recall returned {outcome_name(reply)} for required record {expected_record_id}"
        )
    candidates = candidate_list(reply.get("payload"))
    candidate_ids = [candidate.get("record_id") for candidate in candidates]
    candidate_values = [candidate.get("value_text") for candidate in candidates]
    require(
        all(
            isinstance(candidate_id, int)
            and not isinstance(candidate_id, bool)
            and candidate_id > 0
            for candidate_id in candidate_ids
        ),
        f"{label} recall returned a candidate without an integer record ID",
    )
    require(
        all(isinstance(value, str) for value in candidate_values),
        f"{label} recall returned a candidate without value text",
    )
    require(
        len(candidate_ids) == len(set(candidate_ids)),
        f"{label} recall returned duplicate candidate IDs: {candidate_ids}",
    )
    scope = validate_candidate_scope(
        candidates,
        namespace=namespace,
        expected_source_id=expected_source_id,
        label=label,
    )
    if expected_record_id not in candidate_ids:
        raise RequiredRecordMiss(
            f"{label} recall missed required record {expected_record_id}: {candidate_ids}"
        )
    if not (candidate_ids and candidate_ids[0] == expected_record_id):
        raise RequiredRecordMiss(
            f"{label} recall ranked another record first: {candidate_ids}"
        )
    after, _ = inspect_state(worker, namespace)
    require(
        after["state_digest"] == before["state_digest"],
        f"{label} recall changed durable state digest",
    )
    require(
        after["commit_seq"] == before["commit_seq"],
        f"{label} recall changed durable commit sequence",
    )
    require(
        reply.get("state_generation") == before["commit_seq"],
        f"{label} recall ACK generation disagreed with inspection",
    )
    return {
        "label": label,
        "query_text": query_text,
        "query_sha256": sha256_bytes(query_text.encode("utf-8")),
        "expected_record_id": expected_record_id,
        "expected_source_id": expected_source_id,
        "reply": reply,
        "operation": recall_operation,
        "candidate_ids": candidate_ids,
        "candidates": candidates,
        "candidate_values": candidate_values,
        "candidate_scope": scope,
        "inspection_before": before,
        "inspection_after": after,
        "durable_state_digest": after["state_digest"],
    }


def scope_probe(
    worker: WorkerProcess,
    namespace: str,
    query_text: str,
    *,
    label: str,
    expected_record_id: int,
    expected_source_id: str,
    peer_payloads: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    """Populate two peers and prove identical queries stay namespace-local."""

    require(len(peer_payloads) == 2, f"{label} requires exactly two peer namespaces")
    require(
        isinstance(query_text, str) and bool(query_text),
        f"{label} scope query was missing",
    )
    require(
        isinstance(expected_source_id, str) and bool(expected_source_id),
        f"{label} primary source ID was missing",
    )
    require(
        all(
            isinstance(payload, dict)
            and payload.get("key_text") == query_text
            and isinstance(payload.get("source"), str)
            and bool(payload["source"])
            and isinstance(payload.get("value_text"), str)
            and bool(payload["value_text"])
            for payload in peer_payloads
        ),
        f"{label} peer payloads were incomplete or did not share the query",
    )
    peer_source_ids = [payload["source"] for payload in peer_payloads]
    require(
        expected_source_id not in peer_source_ids
        and len(set(peer_source_ids)) == len(peer_source_ids),
        f"{label} primary and peer source IDs were not disjoint",
    )
    peer_namespaces = [f"{namespace}/scope-peer-{ordinal}" for ordinal in range(2)]
    peer_wire_namespaces = [wire_namespace(peer) for peer in peer_namespaces]
    require(
        len(set(peer_wire_namespaces)) == 2
        and wire_namespace(namespace) not in set(peer_wire_namespaces),
        f"{label} peer namespaces were not isolated",
    )
    primary_before = recall_record(
        worker,
        namespace,
        query_text=query_text,
        expected_record_id=expected_record_id,
        expected_source_id=expected_source_id,
        label=f"{label}-primary-before-peers",
    )
    peer_observations = [
        observe_record(worker, peer_namespace, payload)
        for peer_namespace, payload in zip(peer_namespaces, peer_payloads)
    ]
    peer_evidence: list[dict[str, Any]] = []
    peer_values = [payload["value_text"] for payload in peer_payloads]
    require(
        len(set(peer_values)) == len(peer_values),
        f"{label} peer values were not distinct",
    )
    primary_after = recall_record(
        worker,
        namespace,
        query_text=query_text,
        expected_record_id=expected_record_id,
        expected_source_id=expected_source_id,
        label=f"{label}-primary-after-peers",
    )
    for ordinal, (peer_namespace, payload, observed) in enumerate(
        zip(peer_namespaces, peer_payloads, peer_observations)
    ):
        recalled = recall_record(
            worker,
            peer_namespace,
            query_text=query_text,
            expected_record_id=observed["record_id"],
            expected_source_id=payload["source"],
            label=f"{label}-peer-{ordinal}",
        )
        candidate_values = [
            candidate.get("value_text") for candidate in recalled["candidates"]
        ]
        require(
            candidate_values and candidate_values[0] == payload["value_text"],
            f"{label} peer {ordinal} returned the wrong namespace value",
        )
        require(
            not any(
                value in peer_values
                for value in candidate_values
                if value != payload["value_text"]
            ),
            f"{label} peer {ordinal} leaked another peer value",
        )
        peer_evidence.append(
            {
                "namespace": peer_namespace,
                "wire_namespace": wire_namespace(peer_namespace),
                "observed": observed,
                "recall": recalled,
            }
        )
    primary_values = [
        candidate.get("value_text") for candidate in primary_after["candidates"]
    ]
    require(
        primary_values and not any(value in peer_values for value in primary_values),
        f"{label} primary namespace leaked a populated peer value",
    )
    primary_value_set = set(primary_values)
    require(
        not any(
            value in primary_value_set
            for evidence in peer_evidence
            for value in evidence["recall"]["candidate_values"]
        ),
        f"{label} peer namespace leaked a primary value",
    )
    return {
        "label": label,
        "namespace": namespace,
        "wire_namespace": wire_namespace(namespace),
        "query_sha256": sha256_bytes(query_text.encode("utf-8")),
        "allowed_source_ids": [expected_source_id, *peer_source_ids],
        "allowed_namespace_ids": [wire_namespace(namespace), *peer_wire_namespaces],
        "primary_before_peers": primary_before,
        "primary": primary_after,
        "peers": peer_evidence,
        "populated_peer_count": len(peer_evidence),
    }


def run_session(
    binary: Path,
    state_root: Path,
    *,
    timeout_ms: int,
    action: Callable[[WorkerProcess], Any],
    deadline: float | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Run one production session and preserve evidence on every path."""

    worker: WorkerProcess | None = None
    error: BaseException | None = None
    value: Any = None
    try:
        worker = WorkerProcess(
            binary,
            state_root,
            timeout_ms=timeout_ms,
            test_double=False,
            deadline=deadline,
        )
        value = action(worker)
        if deadline is not None and time.monotonic() > deadline:
            raise DeadlineExceeded("worker session exceeded its bounded deadline")
    except BaseException as caught:
        error = caught
    evidence: dict[str, Any] = {
        "binary": str(binary),
        "state_root": str(state_root),
        "test_double": False,
        "deadline_monotonic": deadline,
        "deadline_remaining_seconds": (
            max(0.0, deadline - time.monotonic()) if deadline is not None else None
        ),
    }
    if worker is None:
        evidence["operations"] = []
        evidence["close"] = {"returncode": None}
    else:
        try:
            close_kwargs: dict[str, Any] = {
                "suppress_errors": error is not None,
                "timeout_seconds": SESSION_CLEANUP_TIMEOUT_SECONDS,
            }
            evidence["close"] = worker.close(**close_kwargs)
        except BaseException as close_error:
            if error is None:
                error = close_error
            evidence["close_error"] = failure_evidence(close_error)
            evidence["close"] = getattr(worker, "close_evidence", {"returncode": None})
        process = getattr(worker, "process", None)
        process_pid = getattr(process, "pid", None)
        evidence["pid"] = (
            process_pid
            if isinstance(process_pid, int)
            and not isinstance(process_pid, bool)
            and process_pid > 0
            else None
        )
        evidence["operations"] = list(worker.operations)
    if error is not None:
        evidence["error"] = failure_evidence(error)
        if value is not None:
            evidence["partial_value"] = value
    evidence["session_evidence_sha256"] = operation_digest(evidence)
    if error is not None:
        if isinstance(error, KeyboardInterrupt):
            raise SessionInterrupted(
                str(error),
                evidence,
                error_code=failure_error_code(error, "session_interrupted"),
            ) from error
        raise SessionFailure(
            str(error),
            evidence,
            error_code=failure_error_code(error, "session_failed"),
        ) from error
    return value, evidence


def run_real_cancellation_probe(
    binary: Path,
    campaign_root: Path,
    config: CampaignConfig,
    *,
    admitted_identity: dict[str, Any],
    campaign_deadline: float | None,
) -> dict[str, Any]:
    """Exercise a real worker deadline and retain the resulting durable effect."""

    probe_seed = int(sha256_bytes(b"ncm-production-stress/cancellation")[:16], 16)
    namespace = trial_namespace("cold", 0, probe_seed) + "/cancellation"
    payload = cancellation_payload(seed=probe_seed)
    workload = {
        "kind": "real_encoder_max_record_observe",
        "test_double": False,
        "key_bytes": len(payload["key_text"].encode("utf-8")),
        "value_bytes": len(payload["value_text"].encode("utf-8")),
        "record_bytes": len(payload["key_text"].encode("utf-8"))
        + len(payload["value_text"].encode("utf-8")),
        "request_deadline_ms": REAL_CANCELLATION_TIMEOUT_MS,
    }
    require(
        workload["key_bytes"] == CANCELLATION_WORKLOAD_BYTES
        and workload["value_bytes"] == CANCELLATION_WORKLOAD_BYTES
        and workload["record_bytes"] == 2 * CANCELLATION_WORKLOAD_BYTES,
        "real cancellation workload was not at the production record bound",
    )
    probe_root = prepare_trial_state_root(
        campaign_root,
        "cancellation",
        0,
        probe_seed,
    )
    session_timeout_ms = max(config.operation_timeout_ms, 1_000)
    first_deadline = time.monotonic() + min(config.trial_timeout_seconds, 60.0)
    if campaign_deadline is not None:
        first_deadline = min(first_deadline, campaign_deadline)

    def first_action(worker: WorkerProcess) -> dict[str, Any]:
        identity = handshake(worker, admitted_identity=admitted_identity)
        ready = health(worker, admitted_identity=admitted_identity)
        timeout_reply: dict[str, Any] | None = None
        timeout_error: dict[str, Any] | None = None
        try:
            timeout_reply = worker.call(
                "observe",
                namespace,
                payload,
                timeout_ms=REAL_CANCELLATION_TIMEOUT_MS,
            )
        except BaseException as error:
            if isinstance(error, KeyboardInterrupt):
                raise
            timeout_error = failure_evidence(error)
        observe_operations = [
            operation
            for operation in worker.operations
            if operation.get("op") == "observe"
        ]
        require(
            observe_operations,
            "real cancellation probe did not record its in-flight observe operation",
        )
        operation = observe_operations[-1]
        require(
            operation.get("deadline_ms", 0) <= REAL_CANCELLATION_TIMEOUT_MS,
            "real cancellation probe did not send its bounded operation deadline",
        )
        require(
            operation.get("outcome") in {"cancelled", "deadline_exceeded"},
            "real worker completed a supposedly cancelled operation",
        )
        require(
            operation.get("request_bytes", 0) > CANCELLATION_WORKLOAD_BYTES,
            "real cancellation operation did not carry its bounded text workload",
        )
        return {
            "identity": identity,
            "health": ready,
            "timeout_reply": timeout_reply,
            "timeout_error": timeout_error,
            "operation": operation,
            "workload": workload,
        }

    try:
        first_value, first_session = run_session(
            binary,
            probe_root,
            timeout_ms=session_timeout_ms,
            action=first_action,
            deadline=first_deadline,
        )
    except SessionInterrupted as error:
        raise SessionInterrupted(
            str(error),
            {
                "probe_state_root": str(probe_root),
                "sessions": [error.evidence],
                "first": error.evidence,
            },
            error_code=failure_error_code(error, "session_interrupted"),
        ) from error
    except SessionFailure as error:
        raise EvidenceFailure(
            str(error),
            {"probe_state_root": str(probe_root), "sessions": [error.evidence]},
            error_code=failure_error_code(error, "session_failed"),
        ) from error

    def run_second_phase() -> dict[str, Any]:
        second_deadline = time.monotonic() + min(config.trial_timeout_seconds, 60.0)
        if campaign_deadline is not None:
            second_deadline = min(second_deadline, campaign_deadline)

        def second_action(worker: WorkerProcess) -> dict[str, Any]:
            identity = handshake(worker, admitted_identity=admitted_identity)
            ready = health(worker, admitted_identity=admitted_identity)
            reply = worker.call("inspection", namespace, {})
            operation = worker.operations[-1]
            effect: dict[str, Any] = {
                "reply": reply,
                "operation": operation,
                "outcome": outcome_name(reply),
            }
            if outcome_name(reply) == "success":
                _inspection, payload_value = inspect_state(worker, namespace)
                effect["inspection"] = payload_value
            else:
                require(
                    outcome_name(reply) == "empty",
                    f"cancellation effect inspection returned {outcome_name(reply)}",
                )
                effect["inspection"] = None
            return {"identity": identity, "health": ready, "effect": effect}

        second_value, second_session = run_session(
            binary,
            probe_root,
            timeout_ms=session_timeout_ms,
            action=second_action,
            deadline=second_deadline,
        )
        return {
            "namespace": namespace,
            "wire_namespace": wire_namespace(namespace),
            "probe_state_root": str(probe_root),
            "timeout_ms": REAL_CANCELLATION_TIMEOUT_MS,
            "workload": workload,
            "first": first_value,
            "effect": second_value["effect"],
            "sessions": [first_session, second_session],
        }

    try:
        return run_second_phase()
    except SessionInterrupted as error:
        raise SessionInterrupted(
            str(error),
            {
                "probe_state_root": str(probe_root),
                "first": first_value,
                "sessions": [first_session, error.evidence],
                "second": error.evidence,
            },
            error_code=failure_error_code(error, "session_interrupted"),
        ) from error
    except SessionFailure as error:
        raise EvidenceFailure(
            str(error),
            {
                "probe_state_root": str(probe_root),
                "first": first_value,
                "sessions": [first_session, error.evidence],
            },
            error_code=failure_error_code(error, "session_failed"),
        ) from error
    except KeyboardInterrupt as error:
        raise SessionInterrupted(
            str(error),
            {
                "probe_state_root": str(probe_root),
                "first": first_value,
                "sessions": [first_session],
                "error": failure_evidence(error),
            },
            error_code=failure_error_code(error, "session_interrupted"),
        ) from error
    except BaseException as error:
        raise EvidenceFailure(
            str(error),
            {
                "probe_state_root": str(probe_root),
                "first": first_value,
                "sessions": [first_session],
                "error": failure_evidence(error),
            },
            error_code=failure_error_code(error, "session_failed"),
        ) from error


def trial_namespace(phase: str, index: int, seed: int) -> str:
    return f"ncm-production-stress/{phase}/{index:03d}/{seed}"


def run_cold_trial(
    binary: Path,
    root: Path,
    config: CampaignConfig,
    phase: str,
    index: int,
    seed: int,
    *,
    admitted_identity: dict[str, Any] | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    namespace = trial_namespace(phase, index, seed)
    payload = observation_payload(phase=phase, trial_index=index, seed=seed)
    peers = scope_peer_payloads(
        phase=phase,
        trial_index=index,
        seed=seed,
        query_text=payload["key_text"],
    )

    def action(worker: WorkerProcess) -> dict[str, Any]:
        identity = handshake(worker, admitted_identity=admitted_identity)
        ready = health(worker, admitted_identity=admitted_identity)
        observed = observe_record(worker, namespace, payload)
        recalled = recall_record(
            worker,
            namespace,
            query_text=payload["key_text"],
            expected_record_id=observed["record_id"],
            expected_source_id=payload["source"],
            label="cold",
        )
        scope = scope_probe(
            worker,
            namespace,
            payload["key_text"],
            label="cold",
            expected_record_id=observed["record_id"],
            expected_source_id=payload["source"],
            peer_payloads=peers,
        )
        return {
            "identity": identity,
            "health": ready,
            "observations": [observed],
            "recalls": [recalled],
            "scope_probes": [scope],
        }

    value, session = run_session(
        binary,
        root,
        timeout_ms=config.operation_timeout_ms,
        action=action,
        deadline=deadline,
    )
    value["sessions"] = [session]
    value["namespace"] = namespace
    value["wire_namespace"] = wire_namespace(namespace)
    value["expected_record_ids"] = [value["observations"][0]["record_id"]]
    return value


def run_warm_trial(
    binary: Path,
    root: Path,
    config: CampaignConfig,
    phase: str,
    index: int,
    seed: int,
    *,
    admitted_identity: dict[str, Any] | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    namespace = trial_namespace(phase, index, seed)
    payloads = [
        observation_payload(phase=phase, trial_index=index, seed=seed, ordinal=ordinal)
        for ordinal in range(2)
    ]
    peers = scope_peer_payloads(
        phase=phase,
        trial_index=index,
        seed=seed,
        query_text=payloads[0]["key_text"],
    )

    def action(worker: WorkerProcess) -> dict[str, Any]:
        identity = handshake(worker, admitted_identity=admitted_identity)
        ready = health(worker, admitted_identity=admitted_identity)
        observations = [
            observe_record(worker, namespace, payload) for payload in payloads
        ]
        recalls = [
            recall_record(
                worker,
                namespace,
                query_text=payload["key_text"],
                expected_record_id=observed["record_id"],
                expected_source_id=payload["source"],
                label=f"warm-{ordinal}",
            )
            for ordinal, (payload, observed) in enumerate(zip(payloads, observations))
        ]
        recalls.append(
            recall_record(
                worker,
                namespace,
                query_text=payloads[0]["key_text"],
                expected_record_id=observations[0]["record_id"],
                expected_source_id=payloads[0]["source"],
                label="warm-repeat",
            )
        )
        scope = scope_probe(
            worker,
            namespace,
            payloads[0]["key_text"],
            label="warm",
            expected_record_id=observations[0]["record_id"],
            expected_source_id=payloads[0]["source"],
            peer_payloads=peers,
        )
        return {
            "identity": identity,
            "health": ready,
            "observations": observations,
            "recalls": recalls,
            "scope_probes": [scope],
        }

    value, session = run_session(
        binary,
        root,
        timeout_ms=config.operation_timeout_ms,
        action=action,
        deadline=deadline,
    )
    value["sessions"] = [session]
    value["namespace"] = namespace
    value["wire_namespace"] = wire_namespace(namespace)
    value["expected_record_ids"] = [
        observation["record_id"] for observation in value["observations"]
    ]
    return value


def run_restart_trial(
    binary: Path,
    root: Path,
    config: CampaignConfig,
    phase: str,
    index: int,
    seed: int,
    *,
    admitted_identity: dict[str, Any] | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    namespace = trial_namespace(phase, index, seed)
    payload = observation_payload(phase=phase, trial_index=index, seed=seed)
    peers = scope_peer_payloads(
        phase=phase,
        trial_index=index,
        seed=seed,
        query_text=payload["key_text"],
    )

    def first_action(worker: WorkerProcess) -> dict[str, Any]:
        identity = handshake(worker, admitted_identity=admitted_identity)
        ready = health(worker, admitted_identity=admitted_identity)
        observed = observe_record(worker, namespace, payload)
        duplicate = replay_observe_record(
            worker,
            namespace,
            payload,
            observed,
            label="restart-before",
        )
        inspected, _ = inspect_state(worker, namespace)
        return {
            "identity": identity,
            "health": ready,
            "observed": observed,
            "duplicate_before_restart": duplicate,
            "inspection_before_restart": inspected,
        }

    first_value, first_session = run_session(
        binary,
        root,
        timeout_ms=config.operation_timeout_ms,
        action=first_action,
        deadline=deadline,
    )

    def second_action(worker: WorkerProcess) -> dict[str, Any]:
        identity = handshake(worker, admitted_identity=admitted_identity)
        ready = health(worker, admitted_identity=admitted_identity)
        inspected, _ = inspect_state(worker, namespace)
        require(
            inspected["state_digest"]
            == first_value["inspection_before_restart"]["state_digest"],
            "restart changed the durable state digest",
        )
        require(
            inspected["commit_seq"]
            == first_value["inspection_before_restart"]["commit_seq"],
            "restart changed the durable commit sequence",
        )
        duplicate = replay_observe_record(
            worker,
            namespace,
            payload,
            first_value["observed"],
            label="restart-after",
        )
        recalled = recall_record(
            worker,
            namespace,
            query_text=payload["key_text"],
            expected_record_id=first_value["observed"]["record_id"],
            expected_source_id=payload["source"],
            label="restart",
        )
        scope = scope_probe(
            worker,
            namespace,
            payload["key_text"],
            label="restart",
            expected_record_id=first_value["observed"]["record_id"],
            expected_source_id=payload["source"],
            peer_payloads=peers,
        )
        return {
            "identity": identity,
            "health": ready,
            "inspection_after_restart": inspected,
            "duplicate_after_restart": duplicate,
            "recall": recalled,
            "scope": scope,
        }

    try:
        second_value, second_session = run_session(
            binary,
            root,
            timeout_ms=config.operation_timeout_ms,
            action=second_action,
            deadline=deadline,
        )
    except SessionInterrupted as error:
        raise SessionInterrupted(
            str(error),
            {
                "sessions": [first_session, error.evidence],
                "first": first_value,
                "second": error.evidence,
            },
            error_code=failure_error_code(error, "session_interrupted"),
        ) from error
    except SessionFailure as error:
        raise EvidenceFailure(
            str(error),
            {
                "sessions": [first_session, error.evidence],
                "first": first_value,
                "second": error.evidence,
            },
            error_code=failure_error_code(error, "session_failed"),
        ) from error
    return {
        "namespace": namespace,
        "wire_namespace": wire_namespace(namespace),
        "expected_record_ids": [first_value["observed"]["record_id"]],
        "identity": {
            "before_restart": first_value["identity"],
            "after_restart": second_value["identity"],
        },
        "health": {
            "before_restart": first_value["health"],
            "after_restart": second_value["health"],
        },
        "observations": [first_value["observed"]],
        "recalls": [second_value["recall"]],
        "scope_probes": [second_value["scope"]],
        "restart": {
            "inspection_before": first_value["inspection_before_restart"],
            "inspection_after": second_value["inspection_after_restart"],
            "state_digest_equal": True,
            "commit_seq_equal": True,
            "duplicate_before_restart": first_value["duplicate_before_restart"],
            "duplicate_after_restart": second_value["duplicate_after_restart"],
        },
        "sessions": [first_session, second_session],
    }


def run_concurrent_trial(
    binary: Path,
    root: Path,
    config: CampaignConfig,
    phase: str,
    index: int,
    seed: int,
    *,
    admitted_identity: dict[str, Any] | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    namespace = trial_namespace(phase, index, seed)
    payloads = [
        observation_payload(phase=phase, trial_index=index, seed=seed, ordinal=ordinal)
        for ordinal in range(config.concurrency)
    ]
    peers = scope_peer_payloads(
        phase=phase,
        trial_index=index,
        seed=seed,
        query_text=payloads[0]["key_text"],
    )
    topology = concurrent_topology(config.concurrency)
    namespace_ids = [
        wire_namespace(namespace),
        *(
            wire_namespace(f"{namespace}/scope-peer-{ordinal}")
            for ordinal in range(len(peers))
        ),
    ]

    def setup_action(worker: WorkerProcess) -> dict[str, Any]:
        identity = handshake(worker, admitted_identity=admitted_identity)
        ready = health(worker, admitted_identity=admitted_identity)
        observations = [
            observe_record(worker, namespace, payload) for payload in payloads
        ]
        inspected, _ = inspect_state(worker, namespace)
        scope = scope_probe(
            worker,
            namespace,
            payloads[0]["key_text"],
            label="concurrent",
            expected_record_id=observations[0]["record_id"],
            expected_source_id=payloads[0]["source"],
            peer_payloads=peers,
        )
        setup = {
            "identity": identity,
            "health": ready,
            "observations": observations,
            "inspection_before_load": inspected,
            "scope": scope,
        }
        return setup

    def concurrent_action(worker: WorkerProcess) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        interrupted_error: BaseException | None = None

        def concurrent_recall(
            owner: SingleOwnerDispatcher,
            setup: dict[str, Any],
            ordinal: int,
        ) -> dict[str, Any]:
            payload = payloads[ordinal]
            expected = setup["observations"][ordinal]["record_id"]

            def action(owner_worker: WorkerProcess) -> dict[str, Any]:
                recalled = recall_record(
                    owner_worker,
                    namespace,
                    query_text=payload["key_text"],
                    expected_record_id=expected,
                    expected_source_id=payload["source"],
                    label=f"concurrent-{ordinal}",
                )
                return {"recall": recalled}

            return {
                "ordinal": ordinal,
                "value": owner.call(action, batch=True),
            }

        with SingleOwnerDispatcher(worker) as owner:
            setup = owner.call(setup_action)
            owner.begin_batch(config.concurrency)
            executor = ThreadPoolExecutor(
                max_workers=config.concurrency,
                thread_name_prefix="ncm-stress-client",
            )
            futures: dict[Any, int] = {}
            try:
                futures = {
                    executor.submit(concurrent_recall, owner, setup, ordinal): ordinal
                    for ordinal in range(config.concurrency)
                }
                for future in as_completed(futures):
                    ordinal = futures[future]
                    try:
                        results.append(future.result())
                    except BaseException as error:
                        failures.append(
                            {
                                "ordinal": ordinal,
                                "evidence": failure_evidence(error),
                            }
                        )
                        if isinstance(error, KeyboardInterrupt):
                            interrupted_error = error
            except BaseException:
                for future in futures:
                    future.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                raise
            else:
                executor.shutdown(wait=True)
            dispatcher = owner.stats()
            measured_topology = measured_concurrent_topology(
                topology, dispatcher, namespace_ids
            )
        if interrupted_error is not None:
            raise SessionInterrupted(
                str(interrupted_error),
                {
                    "setup": setup,
                    "topology": measured_topology,
                    "concurrent_results": results,
                    "concurrent_failures": failures,
                },
                error_code=failure_error_code(interrupted_error, "session_interrupted"),
            ) from interrupted_error
        results.sort(key=lambda result: result["ordinal"])
        require(
            dispatcher["client_overlap"],
            "concurrent client calls did not overlap at the dispatcher boundary",
        )
        require(
            dispatcher.get("batch_client_callers") == config.concurrency
            and set(dispatcher.get("batch_caller_thread_ids", [])).isdisjoint(
                dispatcher["owner_thread_ids"]
            ),
            "concurrent load did not prove distinct client callers crossed the dispatcher boundary",
        )
        require(
            dispatcher["owner_threads"] == 1 and dispatcher["owner_alive"] is True,
            "concurrent load did not retain exactly one live owner thread",
        )
        require(
            dispatcher["owner_threads"] == len(dispatcher["owner_thread_ids"])
            and measured_topology["namespace_owners"] == 1,
            "concurrent load did not retain one measured namespace owner",
        )
        namespace_bindings = dispatcher.get("namespace_owner_bindings")
        require(
            isinstance(namespace_bindings, list)
            and {
                binding.get("namespace_id")
                for binding in namespace_bindings
                if isinstance(binding, dict)
            }
            == set(namespace_ids),
            "concurrent load did not bind every admitted namespace to an owner",
        )
        require(
            all(
                isinstance(binding, dict)
                and binding.get("owner_thread_ids") == dispatcher["owner_thread_ids"]
                and binding.get("worker_pids") == dispatcher["worker_pids"]
                for binding in namespace_bindings
            ),
            "concurrent load observed an unbound namespace owner or worker",
        )
        require(
            set(dispatcher["operation_namespace_ids"]) == set(namespace_ids),
            "concurrent load did not exercise every admitted namespace ID",
        )
        require(
            dispatcher["worker_processes"] == 1
            and len(dispatcher["worker_pids"]) == 1
            and dispatcher["worker_alive"] is True,
            "concurrent load did not retain one live measured worker process",
        )
        if failures:
            raise EvidenceFailure(
                f"concurrent recall clients failed: {[failure['ordinal'] for failure in failures]}",
                {
                    "setup": setup,
                    "topology": measured_topology,
                    "concurrent_results": results,
                    "concurrent_failures": failures,
                },
                error_code=(
                    "required_record_miss"
                    if any(
                        contains_required_record_miss(failure) for failure in failures
                    )
                    else "concurrent_failure"
                ),
            )
        return {
            "setup": setup,
            "concurrent_results": results,
            "dispatcher": dispatcher,
            "topology": measured_topology,
        }

    try:
        value, session = run_session(
            binary,
            root,
            timeout_ms=config.operation_timeout_ms,
            action=concurrent_action,
            deadline=deadline,
        )
    except SessionFailure as error:
        raise EvidenceFailure(
            str(error),
            {
                "topology": topology,
                "session": error.evidence,
                "sessions": [error.evidence],
            },
            error_code=failure_error_code(error, "session_failed"),
        ) from error
    except SessionInterrupted as error:
        raise SessionInterrupted(
            str(error),
            {
                "topology": topology,
                "session": error.evidence,
                "sessions": [error.evidence],
            },
            error_code=failure_error_code(error, "session_interrupted"),
        ) from error
    setup_value = value["setup"]
    results = value["concurrent_results"]
    return {
        "namespace": namespace,
        "wire_namespace": wire_namespace(namespace),
        "expected_record_ids": [
            observation["record_id"] for observation in setup_value["observations"]
        ],
        "identity": setup_value["identity"],
        "health": setup_value["health"],
        "observations": setup_value["observations"],
        "recalls": [result["value"]["recall"] for result in results],
        "scope_probes": [setup_value["scope"]],
        "sessions": [session],
        "topology": value["topology"],
        "load": {
            "concurrency": config.concurrency,
            "client_calls": len(results),
            "client_callers": value["dispatcher"]["batch_client_callers"],
            "worker_processes": value["dispatcher"]["worker_processes"],
            "namespace_count": value["dispatcher"]["namespace_count"],
        },
    }


TRIAL_RUNNERS: dict[
    str, Callable[[Path, Path, CampaignConfig, str, int, int], dict[str, Any]]
] = {
    "cold": run_cold_trial,
    "warm": run_warm_trial,
    "restart": run_restart_trial,
    "concurrent": run_concurrent_trial,
}


def run_trial(
    binary: Path,
    root: Path,
    config: CampaignConfig,
    phase: str,
    index: int,
    seed: int,
    *,
    admitted_identity: dict[str, Any] | None = None,
    campaign_deadline: float | None = None,
) -> dict[str, Any]:
    """Run one phase-specific trial with an exact seed and namespace."""

    started = time.time_ns()
    trial_deadline = time.monotonic() + config.trial_timeout_seconds
    if campaign_deadline is not None:
        trial_deadline = min(trial_deadline, campaign_deadline)
    runner = TRIAL_RUNNERS[phase]
    result: Any = None
    trial_root = root
    try:
        if time.monotonic() >= trial_deadline:
            raise DeadlineExceeded(
                f"{phase} trial {index} could not start before its bounded deadline"
            )
        if (root / "models").is_dir():
            trial_root = prepare_trial_state_root(root, phase, index, seed)
        if time.monotonic() >= trial_deadline:
            raise DeadlineExceeded(
                f"{phase} trial {index} exceeded its deadline while preparing its state root"
            )
        result = runner(
            binary,
            trial_root,
            config,
            phase,
            index,
            seed,
            admitted_identity=admitted_identity,
            deadline=trial_deadline,
        )
        if time.monotonic() > trial_deadline:
            raise DeadlineExceeded(
                f"{phase} trial {index} exceeded its bounded trial deadline"
            )
        return {
            "phase": phase,
            "index": index,
            "seed": seed,
            "status": "pass",
            "started_at_unix_ns": started,
            "finished_at_unix_ns": time.time_ns(),
            "evidence": result,
        }
    except BaseException as error:
        evidence = failure_evidence(error)
        evidence["trial_state_root"] = str(trial_root)
        if result is not None:
            evidence["partial_result"] = result
        if (
            isinstance(evidence, dict)
            and "sessions" not in evidence
            and "operations" in evidence
        ):
            evidence = {**evidence, "sessions": [evidence]}
        return {
            "phase": phase,
            "index": index,
            "seed": seed,
            "status": "interrupted"
            if isinstance(error, KeyboardInterrupt)
            else "failed",
            "started_at_unix_ns": started,
            "finished_at_unix_ns": time.time_ns(),
            "error": str(error),
            "error_type": type(error).__name__,
            "error_code": getattr(error, "error_code", None),
            "required_record_miss": contains_required_record_miss(error),
            "deadline_monotonic": trial_deadline,
            "evidence": evidence,
        }


def incomplete_trials(
    config: CampaignConfig,
    phase: str,
    *,
    start_index: int = 0,
    end_index: int | None = None,
    message: str,
    error_type: str,
    error_code: str,
    deadline_monotonic: float | None,
) -> list[dict[str, Any]]:
    """Materialize the bounded index range of trials that could not start."""

    end = config.counts[phase] if end_index is None else end_index
    trials = []
    for index in range(start_index, end):
        try:
            seed = phase_seed(config.seed, phase, index)
        except BaseException:
            seed = PHASE_OFFSETS.get(phase, 0) + index
        now = time.time_ns()
        trials.append(
            {
                "phase": phase,
                "index": index,
                "seed": seed,
                "status": "incomplete",
                "error": message,
                "error_type": error_type,
                "error_code": error_code,
                "deadline_monotonic": deadline_monotonic,
                "started_at_unix_ns": now,
                "finished_at_unix_ns": now,
                "evidence": {},
            }
        )
    return trials


def incomplete_phase(
    config: CampaignConfig,
    phase: str,
    *,
    message: str,
    error_type: str,
    error_code: str,
    deadline_monotonic: float | None,
) -> dict[str, Any]:
    """Materialize every trial that a phase could not start."""

    trials = incomplete_trials(
        config,
        phase,
        message=message,
        error_type=error_type,
        error_code=error_code,
        deadline_monotonic=deadline_monotonic,
    )
    return {
        "planned": config.counts[phase],
        "completed": 0,
        "passed": 0,
        "failed": 0,
        "interrupted": 0,
        "incomplete": len(trials),
        "trials": trials,
    }


def _safe_terminal_trial_count(config: CampaignConfig, phase: str) -> int:
    """Return a bounded row count even when configuration validation failed."""

    value = config.counts.get(phase)
    if isinstance(value, int) and not isinstance(value, bool):
        return max(1, min(MAX_TRIALS_PER_PHASE, value))
    return 1


def materialize_invalid_config_phases(
    receipt: dict[str, Any],
    config: CampaignConfig,
    *,
    message: str,
    error_type: str,
    error_code: str,
) -> None:
    """Emit bounded terminal rows for every phase rejected before scheduling."""

    for phase in PHASES:
        if phase in receipt["phases"]:
            continue
        count = _safe_terminal_trial_count(config, phase)
        trials = incomplete_trials(
            config,
            phase,
            end_index=count,
            message=message,
            error_type=error_type,
            error_code=error_code,
            deadline_monotonic=None,
        )
        receipt["phases"][phase] = {
            "planned": count,
            "configuration_count": config.counts.get(phase),
            "completed": 0,
            "passed": 0,
            "failed": 0,
            "interrupted": 0,
            "incomplete": len(trials),
            "trials": trials,
        }


def phase_result(planned: int, trials: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Summarize one phase without discarding any retained trial row."""

    passed = sum(trial.get("status") == "pass" for trial in trials)
    incomplete = sum(trial.get("status") == "incomplete" for trial in trials)
    interrupted = sum(trial.get("status") == "interrupted" for trial in trials)
    failed = sum(trial.get("status") == "failed" for trial in trials)
    return {
        "planned": planned,
        "completed": passed + failed + interrupted,
        "passed": passed,
        "failed": failed,
        "interrupted": interrupted,
        "incomplete": incomplete,
        "trials": list(trials),
    }


def run_phase(
    binary: Path,
    root: Path,
    config: CampaignConfig,
    phase: str,
    *,
    campaign_deadline: float,
    admitted_identity: dict[str, Any] | None = None,
    receipt: dict[str, Any] | None = None,
    persist: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """Run exactly the planned phase count and materialize incomplete rows."""

    planned = config.counts[phase]
    trials: list[dict[str, Any]] = []

    def publish() -> None:
        if receipt is not None:
            receipt["phases"][phase] = phase_result(planned, trials)
            refresh_receipt_metrics(receipt, config)
        if persist is not None:
            persist()

    publish()
    for index in range(planned):
        seed = phase_seed(config.seed, phase, index)
        if time.monotonic() >= campaign_deadline:
            trials.extend(
                incomplete_trials(
                    config,
                    phase,
                    start_index=index,
                    message="campaign deadline expired before trial start",
                    error_type="DeadlineExceeded",
                    error_code="campaign_deadline_exceeded",
                    deadline_monotonic=campaign_deadline,
                )
            )
            publish()
            break
        trial = run_trial(
            binary,
            root,
            config,
            phase,
            index,
            seed,
            admitted_identity=admitted_identity,
            campaign_deadline=campaign_deadline,
        )
        trials.append(trial)
        publish()
        if trial["status"] == "interrupted":
            if planned - index - 1:
                trials.extend(
                    incomplete_trials(
                        config,
                        phase,
                        start_index=index + 1,
                        message="campaign interrupted before trial start",
                        error_type="KeyboardInterrupt",
                        error_code="campaign_interrupted",
                        deadline_monotonic=campaign_deadline,
                    )
                )
                publish()
            break
    result = phase_result(planned, trials)
    if receipt is not None:
        receipt["phases"][phase] = result
    return result


def _load_checker(repository: Path) -> Any:
    """Load the existing strict backend checker without executing its CLI."""

    spec = importlib.util.spec_from_file_location(
        "tracedecay_ncm_backend_checker",
        CHECK_BACKEND_PATH,
    )
    require(
        spec is not None and spec.loader is not None,
        f"could not load backend checker: {CHECK_BACKEND_PATH}",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    del repository
    return module


def _fresh_directory(path: Path, *, label: str) -> None:
    """Create a fresh campaign directory without deleting an existing tree."""

    if path.exists():
        try:
            metadata = path.lstat()
        except OSError as error:
            raise StressFailure(
                f"could not inspect {label}: {path}: {error}"
            ) from error
        require(
            not stat.S_ISLNK(metadata.st_mode), f"{label} must not be a symlink: {path}"
        )
        require(path.is_dir(), f"{label} is not a directory: {path}")
        require(not any(path.iterdir()), f"{label} is not fresh: {path}")
    else:
        path.mkdir(parents=True)
    if os.name != "nt":
        os.chmod(path, 0o700)


def _read_json_object(path: Path, *, label: str, maximum_bytes: int) -> dict[str, Any]:
    """Read one bounded, regular JSON object without following a symlink."""

    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise StressFailure(f"{label} is missing: {path}") from error
    except OSError as error:
        raise StressFailure(f"could not inspect {label} {path}: {error}") from error
    require(
        not stat.S_ISLNK(metadata.st_mode), f"{label} must not be a symlink: {path}"
    )
    require(stat.S_ISREG(metadata.st_mode), f"{label} must be a regular file: {path}")
    require(
        metadata.st_size <= maximum_bytes,
        f"{label} exceeds its bounded size: {path}",
    )
    try:
        raw = path.read_bytes()
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_json_constant,
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise StressFailure(f"could not parse {label} {path}: {error}") from error
    require(isinstance(value, dict), f"{label} was not a JSON object: {path}")
    return value


def _regular_file_sha256(path: Path, *, label: str) -> str:
    """Hash one bounded identity file while rejecting links and non-files."""

    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise StressFailure(f"{label} is missing: {path}") from error
    except OSError as error:
        raise StressFailure(f"could not inspect {label} {path}: {error}") from error
    require(
        not stat.S_ISLNK(metadata.st_mode), f"{label} must not be a symlink: {path}"
    )
    require(stat.S_ISREG(metadata.st_mode), f"{label} must be a regular file: {path}")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise StressFailure(f"could not read {label} {path}: {error}") from error
    return digest.hexdigest()


def model_tree_digest(path: Path) -> str:
    """Match the worker's no-follow digest for the complete models tree."""

    digest = hashlib.sha256()

    def visit(directory: Path, relative: str) -> None:
        try:
            metadata = directory.lstat()
        except OSError as error:
            raise StressFailure(
                f"could not inspect model tree directory {directory}: {error}"
            ) from error
        require(
            stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode),
            f"model tree entry was not a directory: {directory}",
        )
        try:
            entries = sorted(
                directory.iterdir(),
                key=lambda entry: os.fsencode(entry.name),
            )
        except OSError as error:
            raise StressFailure(
                f"could not list model tree directory {directory}: {error}"
            ) from error
        for entry in entries:
            child_relative = f"{relative}/{entry.name}" if relative else entry.name
            try:
                child_metadata = entry.lstat()
            except OSError as error:
                raise StressFailure(
                    f"could not inspect model tree entry {entry}: {error}"
                ) from error
            require(
                not stat.S_ISLNK(child_metadata.st_mode),
                f"model tree contains a symlink: {entry}",
            )
            digest.update(child_relative.encode("utf-8"))
            digest.update(b"\0")
            if stat.S_ISDIR(child_metadata.st_mode):
                digest.update(b"d\0")
                visit(entry, child_relative)
            elif stat.S_ISREG(child_metadata.st_mode):
                digest.update(b"f\0")
                try:
                    with entry.open("rb") as handle:
                        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                            digest.update(chunk)
                except OSError as error:
                    raise StressFailure(
                        f"could not read model tree entry {entry}: {error}"
                    ) from error
            else:
                raise StressFailure(f"model tree contains a non-regular entry: {entry}")

    visit(path, "")
    return digest.hexdigest()


def _acquisition_manifest_path(repository: Path) -> Path:
    """Return the checked-in target-bound acquisition descriptor."""

    path = repository / MODEL_ACQUISITION_MANIFEST_RELATIVE
    require(
        path.is_absolute(),
        f"acquisition manifest path was not absolute after joining repository: {path}",
    )
    return path


def _validate_acquisition_manifest(
    repository: Path,
    model_identity: dict[str, Any],
) -> tuple[Path, dict[str, Any], str]:
    """Validate the target-independent acquisition descriptor and its pins."""

    path = _acquisition_manifest_path(repository)
    manifest = _read_json_object(
        path,
        label="model acquisition manifest",
        maximum_bytes=16 * 1024 * 1024,
    )
    require(
        set(manifest) == ACQUISITION_MANIFEST_FIELDS,
        "model acquisition manifest had unexpected or missing fields",
    )
    expected = {
        "schema_version": 1,
        "manifest_type": "ncm-model-acquisition",
        "provider_id": "ncm",
        "worker": "tracedecay-ncm-worker",
        "embedding_manifest": "product/ncm/reference/embedding-manifest.json",
        "model_root": "models",
        "cache_repository": MODEL_CACHE_REPOSITORY,
        "model": MODEL_NAME,
        "repository": MODEL_REPOSITORY,
        "revision": MODEL_REVISION,
        "revision_provenance": MODEL_REVISION_PROVENANCE,
        "max_length": REAL_MODEL_MAX_LENGTH,
        "pooling": MODEL_POOLING,
        "normalize": MODEL_NORMALIZE,
        "transport": "https",
        "base_url": MODEL_BASE_URL,
    }
    for field, value in expected.items():
        require(
            manifest.get(field) == value,
            f"model acquisition manifest {field} was stale",
        )
    embedding_digest = model_identity.get("manifest_sha256")
    require(
        isinstance(embedding_digest, str) and HEX_64.fullmatch(embedding_digest),
        "admitted model identity omitted its embedding manifest digest",
    )
    require(
        manifest.get("embedding_manifest_sha256") == embedding_digest,
        "model acquisition manifest was not bound to the embedding manifest",
    )
    revision_path, separator, _pointer = MODEL_REVISION_PROVENANCE.partition("#")
    require(
        separator == "#" and revision_path, "model revision provenance was malformed"
    )
    require(
        manifest.get("revision_provenance") == MODEL_REVISION_PROVENANCE,
        "model acquisition manifest revision provenance was stale",
    )
    provenance_file = repository / revision_path
    require(
        manifest.get("revision_provenance_sha256")
        == _regular_file_sha256(
            provenance_file,
            label="model revision provenance receipt",
        ),
        "model acquisition manifest revision provenance digest was stale",
    )
    files = manifest.get("files")
    expected_files = model_identity.get("files")
    require(
        isinstance(files, list) and isinstance(expected_files, list),
        "model acquisition manifest or model identity omitted files",
    )
    require(
        len(files) == len(expected_files) == len(MODEL_REQUIRED_FILES),
        "model acquisition manifest file count was stale",
    )
    for manifest_file, identity_file, expected_path in zip(
        files, expected_files, MODEL_REQUIRED_FILES
    ):
        require(
            isinstance(manifest_file, dict)
            and set(manifest_file) == {"path", "url", "bytes", "sha256"},
            "model acquisition manifest file metadata was incomplete",
        )
        require(
            isinstance(identity_file, dict)
            and set(identity_file) == {"path", "bytes", "sha256"},
            "admitted model file metadata was incomplete",
        )
        require(
            manifest_file["path"] == identity_file["path"] == expected_path,
            "model acquisition manifest file paths were stale",
        )
        require(
            manifest_file["url"] == f"{manifest['base_url']}{expected_path}",
            "model acquisition manifest file URL was stale",
        )
        require(
            manifest_file["bytes"] == identity_file["bytes"]
            and manifest_file["sha256"] == identity_file["sha256"],
            f"model acquisition manifest digest differed for {expected_path}",
        )
    transaction = manifest.get("transaction")
    require(
        transaction
        == {
            "version": 1,
            "publication": "atomic-directory-swap",
            "journal": "ncm-model-lifecycle-v1.json",
            "staging_prefix": ".ncm-model-staging-",
            "backup_prefix": ".ncm-model-backup-",
        },
        "model acquisition manifest transaction contract drifted",
    )
    receipt_contract = manifest.get("receipt")
    required_receipt_fields = (
        receipt_contract.get("required_fields")
        if isinstance(receipt_contract, dict)
        else None
    )
    require(
        isinstance(receipt_contract, dict)
        and set(receipt_contract)
        == {"schema_version", "relative_path", "required_fields"}
        and receipt_contract.get("schema_version") == 1
        and receipt_contract.get("relative_path")
        == str(MODEL_ACQUISITION_RECEIPT_RELATIVE)
        and isinstance(required_receipt_fields, list)
        and all(isinstance(field, str) for field in required_receipt_fields)
        and ACQUISITION_RECEIPT_FIELDS
        - {"acquisition_manifest_sha256", "root", "tree_sha256"}
        <= set(required_receipt_fields),
        "model acquisition manifest receipt contract drifted",
    )
    return path, manifest, sha256_bytes(stable_json_bytes(manifest))


def validate_model_acquisition_receipt(
    repository: Path,
    model_root: Path,
    model_identity: dict[str, Any],
    *,
    acquisition_manifest_path: Path | None = None,
) -> dict[str, Any]:
    """Require the installed receipt to bind the exact model tree and manifest."""

    model_root = model_root.absolute()
    if acquisition_manifest_path is not None:
        require(
            acquisition_manifest_path == _acquisition_manifest_path(repository),
            "model acquisition manifest path was not canonical",
        )
    manifest_path, manifest, acquisition_digest = _validate_acquisition_manifest(
        repository,
        model_identity,
    )
    receipt_path = model_root / MODEL_ACQUISITION_RECEIPT_RELATIVE
    receipt = _read_json_object(
        receipt_path,
        label="model acquisition receipt",
        maximum_bytes=4 * 1024 * 1024,
    )
    allowed_fields = ACQUISITION_RECEIPT_FIELDS | ACQUISITION_RECEIPT_OPTIONAL_FIELDS
    require(
        set(receipt) <= allowed_fields and ACQUISITION_RECEIPT_FIELDS <= set(receipt),
        "model acquisition receipt had unexpected or missing fields",
    )
    operation = receipt.get("operation")
    outcome = receipt.get("outcome")
    require(
        isinstance(operation, str)
        and OPERATION_ID.fullmatch(str(receipt.get("operation_id", "")))
        and operation in {"install", "update"}
        and (
            (operation == "install" and outcome in {"committed", "already_present"})
            or (operation == "update" and outcome == "committed")
        ),
        "model acquisition receipt operation or outcome was invalid",
    )
    require(
        receipt.get("schema_version") == 1
        and receipt.get("model") == manifest["model"]
        and receipt.get("repository") == manifest["repository"]
        and receipt.get("revision") == manifest["revision"]
        and receipt.get("manifest_sha256") == manifest["embedding_manifest_sha256"]
        and receipt.get("revision_provenance_sha256")
        == manifest["revision_provenance_sha256"]
        and receipt.get("acquisition_manifest_sha256") == acquisition_digest,
        "model acquisition receipt was stale or not bound to the canonical manifest",
    )
    require(
        model_root.is_absolute(),
        f"model root must be absolute for acquisition receipt validation: {model_root}",
    )
    root_text = str(model_root)
    require(
        receipt.get("root") == root_text,
        "model acquisition receipt root was not bound to the model tree",
    )
    if "receipt_path" in receipt:
        require(
            receipt["receipt_path"]
            == str(model_root / MODEL_ACQUISITION_RECEIPT_RELATIVE),
            "model acquisition receipt path was not bound to the model root",
        )
    require(
        isinstance(receipt.get("created_at_unix"), int)
        and not isinstance(receipt["created_at_unix"], bool)
        and receipt["created_at_unix"] > 0,
        "model acquisition receipt creation time was invalid",
    )
    tree_path = model_root / "models"
    actual_tree_digest = model_tree_digest(tree_path)
    require(
        receipt.get("tree_sha256") == actual_tree_digest,
        "model acquisition receipt was not bound to the exact model tree",
    )
    receipt_files = receipt.get("files")
    require(
        isinstance(receipt_files, list)
        and len(receipt_files) == len(manifest["files"]),
        "model acquisition receipt file metadata was incomplete",
    )
    for receipt_file, manifest_file in zip(receipt_files, manifest["files"]):
        require(
            isinstance(receipt_file, dict)
            and set(receipt_file) == {"path", "bytes", "sha256"}
            and receipt_file
            == {
                "path": manifest_file["path"],
                "bytes": manifest_file["bytes"],
                "sha256": manifest_file["sha256"],
            },
            "model acquisition receipt file metadata was stale",
        )
    return {
        "path": str(receipt_path),
        "manifest_path": str(manifest_path),
        "manifest_sha256": acquisition_digest,
        "tree_sha256": actual_tree_digest,
        "receipt": receipt,
    }


def _stage_acquisition_receipt(
    source_receipt: dict[str, Any],
    destination_root: Path,
) -> Path:
    """Copy a validated receipt and rebind its root-specific fields."""

    require(
        destination_root.is_absolute(),
        f"staged state root must be absolute: {destination_root}",
    )
    receipts_root = destination_root / MODEL_ACQUISITION_RECEIPT_RELATIVE.parent
    if receipts_root.exists():
        metadata = receipts_root.lstat()
        require(
            stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode),
            f"staged receipt parent is not a directory: {receipts_root}",
        )
    else:
        receipts_root.mkdir(parents=True)
    if os.name != "nt":
        os.chmod(receipts_root, 0o700)
    staged = dict(source_receipt)
    staged["root"] = str(destination_root)
    staged["receipt_path"] = str(destination_root / MODEL_ACQUISITION_RECEIPT_RELATIVE)
    payload = (
        json.dumps(staged, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    destination = destination_root / MODEL_ACQUISITION_RECEIPT_RELATIVE
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(destination, flags, 0o600)
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            require(written > 0, "staged acquisition receipt write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if hasattr(os, "O_DIRECTORY"):
        directory_fd = os.open(receipts_root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    return destination


def stage_model(
    model_root: Path,
    campaign_root: Path,
    embedding_manifest: Path,
    checker: Any,
    *,
    acquisition_receipt: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Copy the already verified model once into an owner-private state root."""

    _fresh_directory(campaign_root, label="campaign state root")
    source_models = model_root / "models"
    destination_models = campaign_root / "models"
    try:
        shutil.copytree(source_models, destination_models, symlinks=False)
        shutil.copy2(
            source_models / "ncm-encoder-manifest.json",
            destination_models / "ncm-encoder-manifest.json",
        )
    except OSError as error:
        raise StressFailure(
            f"could not stage verified model into {campaign_root}: {error}"
        ) from error
    if acquisition_receipt is None:
        receipt_path = model_root / MODEL_ACQUISITION_RECEIPT_RELATIVE
        acquisition_receipt = _read_json_object(
            receipt_path,
            label="model acquisition receipt",
            maximum_bytes=4 * 1024 * 1024,
        )
    _stage_acquisition_receipt(acquisition_receipt, campaign_root)
    return checker.verify_model(campaign_root, embedding_manifest)


def prepare_trial_state_root(
    campaign_root: Path, phase: str, index: int, seed: int
) -> Path:
    """Give one trial an isolated catalog while reusing staged model bytes."""

    trials_root = campaign_root / "trials"
    if trials_root.exists():
        metadata = trials_root.lstat()
        require(
            not stat.S_ISLNK(metadata.st_mode) and stat.S_ISDIR(metadata.st_mode),
            f"trial state root parent is not a directory: {trials_root}",
        )
    else:
        trials_root.mkdir()
    trial_root = trials_root / f"{phase}-{index:03d}-{seed}"
    _fresh_directory(trial_root, label="trial state root")
    source_models = campaign_root / "models"
    destination_models = trial_root / "models"
    require(
        source_models.is_dir(), f"staged model directory is missing: {source_models}"
    )
    try:
        # Hardlinks keep the per-trial worker root isolated without making a
        # second copy of the large verified ONNX artifact.
        shutil.copytree(
            source_models,
            destination_models,
            symlinks=False,
            copy_function=os.link,
        )
    except OSError:
        if destination_models.exists():
            shutil.rmtree(destination_models)
        try:
            shutil.copytree(source_models, destination_models, symlinks=False)
        except OSError as error:
            raise StressFailure(
                f"could not prepare isolated trial state root {trial_root}: {error}"
            ) from error
    source_receipt = campaign_root / MODEL_ACQUISITION_RECEIPT_RELATIVE
    source_receipt_value = _read_json_object(
        source_receipt,
        label="staged model acquisition receipt",
        maximum_bytes=4 * 1024 * 1024,
    )
    _stage_acquisition_receipt(source_receipt_value, trial_root)
    if os.name != "nt":
        os.chmod(trial_root, 0o700)
    return trial_root


def runtime_manifest_digest(model_root: Path) -> str:
    """Hash the exact runtime manifest that the worker opens from models/."""

    path = model_root / "models" / "ncm-encoder-manifest.json"
    try:
        metadata = path.lstat()
        require(
            not stat.S_ISLNK(metadata.st_mode),
            f"runtime model manifest must not be a symlink: {path}",
        )
        require(
            stat.S_ISREG(metadata.st_mode),
            f"runtime model manifest is not a regular file: {path}",
        )
        return sha256_bytes(path.read_bytes())
    except OSError as error:
        raise StressFailure(
            f"could not hash runtime model manifest {path}: {error}"
        ) from error


def validate_real_encoder_markers(worker_identity: dict[str, Any]) -> None:
    """Require binary evidence that the production encoder is linked."""

    markers = worker_identity.get("required_markers")
    require(isinstance(markers, dict), "worker artifact omitted real encoder markers")
    for marker in REAL_ENCODER_MARKERS:
        require(
            markers.get(marker) is True,
            f"worker artifact lacks required real encoder marker: {marker}",
        )


def enrich_model_identity(model_identity: dict[str, Any]) -> dict[str, Any]:
    """Attach immutable manifest metadata returned by the backend checker."""

    require(
        isinstance(model_identity, dict), "verified model identity was not an object"
    )
    enriched = dict(model_identity)
    expected_fields = {
        "repository": MODEL_REPOSITORY,
        "revision_provenance": MODEL_REVISION_PROVENANCE,
        "max_length": REAL_MODEL_MAX_LENGTH,
        "pooling": MODEL_POOLING,
        "normalize": MODEL_NORMALIZE,
    }
    for field, expected in expected_fields.items():
        if field in enriched:
            require(
                enriched[field] == expected,
                f"verified model {field} was stale",
            )
        else:
            enriched[field] = expected
    require(enriched.get("model") == MODEL_NAME, "verified model identity was stale")
    require(
        enriched.get("revision") == MODEL_REVISION, "verified model revision was stale"
    )
    require(
        isinstance(enriched.get("artifact_sha256"), str)
        and HEX_64.fullmatch(enriched["artifact_sha256"]),
        "verified model artifact digest was invalid",
    )
    files = enriched.get("files")
    require(isinstance(files, list), "verified model files were omitted")
    require(
        len(files) == len(MODEL_REQUIRED_FILES)
        and [item.get("path") for item in files if isinstance(item, dict)]
        == list(MODEL_REQUIRED_FILES),
        "verified model files did not match the pinned order",
    )
    for item in files:
        require(
            isinstance(item, dict) and set(item) == {"path", "sha256", "bytes"},
            "verified model file metadata was incomplete",
        )
        require(
            isinstance(item["sha256"], str) and HEX_64.fullmatch(item["sha256"]),
            "verified model file digest was invalid",
        )
        require(
            isinstance(item["bytes"], int)
            and not isinstance(item["bytes"], bool)
            and item["bytes"] > 0,
            "verified model file size was invalid",
        )
    onnx = files[0]
    require(
        onnx["sha256"] == enriched["artifact_sha256"],
        "verified model artifact did not match onnx/model.onnx",
    )
    return enriched


def worker_wire_identity(
    verified_worker: dict[str, Any],
    worker_manifest_path: Path,
) -> dict[str, Any]:
    """Build the exact V2 worker identity admitted by the sibling manifest."""

    try:
        manifest = json.loads(worker_manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StressFailure(
            f"could not read admitted worker manifest {worker_manifest_path}: {error}"
        ) from error
    require(isinstance(manifest, dict), "admitted worker manifest was not an object")
    targets = manifest.get("targets")
    require(isinstance(targets, list), "admitted worker manifest omitted targets")
    target = next(
        (
            item
            for item in targets
            if isinstance(item, dict)
            and item.get("triple") == verified_worker.get("target")
        ),
        None,
    )
    require(isinstance(target, dict), "admitted worker manifest omitted current target")
    require(
        target.get("sha256") == verified_worker.get("sha256")
        and target.get("bytes") == verified_worker.get("bytes"),
        "admitted worker manifest metadata did not match the staged worker",
    )
    target_identity = {
        field: target.get(field) for field in ("triple", "os", "arch", "family")
    }
    require(
        all(isinstance(value, str) and value for value in target_identity.values()),
        "admitted worker target metadata was incomplete",
    )
    identity = {
        "sha256": verified_worker.get("sha256"),
        "bytes": verified_worker.get("bytes"),
        "target": target_identity,
    }
    return _validate_worker_wire_identity(identity, label="admitted")


def expected_wire_identity(
    verified_worker: dict[str, Any],
    worker_manifest_path: Path,
    model_identity: dict[str, Any],
) -> dict[str, Any]:
    """Create the static V2 identity sent in every production handshake."""

    model = enrich_model_identity(model_identity)
    encoder = {
        "model": model["model"],
        "artifact_sha256": model["artifact_sha256"],
        "repository": model["repository"],
        "revision": model["revision"],
        "revision_provenance": model["revision_provenance"],
        "files": model["files"],
        "max_length": model["max_length"],
        "pooling": model["pooling"],
        "normalize": model["normalize"],
    }
    return {
        "identity_revision": IDENTITY_REVISION_V2,
        "algorithm": {
            "profile": ALGORITHM_PROFILE,
            "config_sha256": ALGORITHM_CONFIG_SHA256,
        },
        "worker": worker_wire_identity(verified_worker, worker_manifest_path),
        "encoder": _validate_encoder_wire_identity(encoder, label="admitted"),
    }


def validate_production_identity(
    model_identity: dict[str, Any],
    handshake_identity: dict[str, Any],
    health_identity: dict[str, Any],
    admitted_wire_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Reject stale, test-double, or incomplete worker/model identity."""
    model = enrich_model_identity(model_identity)
    expected_artifact = model["artifact_sha256"]
    expected_files = model["files"]
    require(
        isinstance(model.get("manifest_sha256"), str)
        and HEX_64.fullmatch(model["manifest_sha256"]),
        "verified model manifest digest was invalid",
    )
    if "runtime_manifest_sha256" in model:
        require(
            isinstance(model["runtime_manifest_sha256"], str)
            and HEX_64.fullmatch(model["runtime_manifest_sha256"]),
            "verified runtime model manifest digest was invalid",
        )
    require(
        handshake_identity.get("identity_revision") == IDENTITY_REVISION_V2,
        "worker handshake identity revision was incomplete",
    )
    require(
        handshake_identity.get("ready") is True,
        "worker handshake did not prove readiness",
    )
    require(
        handshake_identity.get("model") == MODEL_NAME,
        "worker handshake reported a stale or incomplete model",
    )
    require(
        handshake_identity.get("artifact_sha256") == model["artifact_sha256"],
        "worker handshake reported a stale or incomplete artifact",
    )
    require(
        isinstance(handshake_identity.get("projection_sha256"), str)
        and HEX_64.fullmatch(handshake_identity["projection_sha256"]),
        "worker handshake projection identity was incomplete",
    )
    algorithm = handshake_identity.get("algorithm")
    require(
        algorithm
        == {
            "profile": ALGORITHM_PROFILE,
            "config_sha256": ALGORITHM_CONFIG_SHA256,
        },
        "worker handshake reported a stale algorithm identity",
    )
    worker = _validate_worker_wire_identity(
        handshake_identity.get("worker"), label="handshake"
    )
    encoder = _validate_encoder_wire_identity(
        handshake_identity.get("encoder"), label="handshake"
    )
    require(
        encoder["artifact_sha256"] == expected_artifact,
        "worker handshake reported a stale model artifact",
    )
    require(
        encoder["files"] == expected_files,
        "worker handshake reported stale model file metadata",
    )
    require(
        encoder["revision"] == model["revision"],
        "worker handshake reported a stale model revision",
    )
    if admitted_wire_identity is not None:
        expected = _expected_wire_identity(admitted_wire_identity)
        require(
            expected is not None,
            "admitted worker identity was missing",
        )
        require(
            algorithm == expected["algorithm"]
            and worker == expected["worker"]
            and encoder == expected["encoder"],
            "worker identity did not match the admitted worker/model metadata",
        )
    require(
        health_identity.get("process_alive") is True
        and health_identity.get("encoder_ready") is True,
        "worker health was incomplete",
    )
    health_encoder = health_identity.get("encoder")
    require(
        isinstance(health_encoder, dict),
        "worker health encoder identity was incomplete",
    )
    for field in ("model", "artifact_sha256", "max_length"):
        require(
            health_encoder.get(field) == encoder[field],
            f"worker health {field} did not match the admitted identity",
        )
    health_v2 = health_identity.get("v2_identity")
    require(
        isinstance(health_v2, dict),
        "worker health did not carry a complete V2 identity binding",
    )
    require(
        health_v2.get("identity_revision") == IDENTITY_REVISION_V2
        and health_v2.get("ready") is True
        and health_v2.get("model") == MODEL_NAME
        and health_v2.get("artifact_sha256") == encoder["artifact_sha256"]
        and health_v2.get("config_sha256") == algorithm["config_sha256"]
        and health_v2.get("algorithm") == algorithm
        and health_v2.get("worker") == worker
        and health_v2.get("encoder") == encoder,
        "worker health V2 identity did not match the handshake",
    )
    require(
        isinstance(health_v2.get("projection_sha256"), str)
        and HEX_64.fullmatch(health_v2["projection_sha256"])
        and health_v2["projection_sha256"] == handshake_identity["projection_sha256"]
        and isinstance(health_v2.get("epoch"), int)
        and not isinstance(health_v2["epoch"], bool)
        and health_v2["epoch"] >= 0,
        "worker health V2 identity was incomplete",
    )
    require(
        health_v2["epoch"] == handshake_identity["epoch"],
        "worker health V2 epoch did not match the handshake",
    )
    return {
        "model": MODEL_NAME,
        "artifact_sha256": expected_artifact,
        "repository": model["repository"],
        "revision": model["revision"],
        "revision_provenance": model["revision_provenance"],
        "files": expected_files,
        "algorithm_profile": ALGORITHM_PROFILE,
        "config_sha256": ALGORITHM_CONFIG_SHA256,
        "identity_revision": IDENTITY_REVISION_V2,
        "worker": worker,
        "real_encoder": True,
        "test_double": False,
        "max_length": REAL_MODEL_MAX_LENGTH,
        "pooling": MODEL_POOLING,
        "normalize": MODEL_NORMALIZE,
    }


def run_test_double_control(
    binary: Path,
    control_root: Path,
    timeout_ms: int,
    *,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Prove the artifact's explicit test-double identity is detectable."""

    _fresh_directory(control_root, label="test-double control root")
    worker: WorkerProcess | None = None
    try:
        worker = WorkerProcess(
            binary,
            control_root,
            timeout_ms=timeout_ms,
            test_double=True,
            deadline=deadline,
        )
        reply = worker.call(
            "handshake",
            "0" * 64,
            {
                "protocol_version": PROTOCOL_VERSION,
                "protocol_identity": PROTOCOL_IDENTITY,
                "algorithm_profile": ALGORITHM_PROFILE,
            },
        )
        require_success(reply, "test-double control handshake")
        payload = reply.get("payload")
        require(
            isinstance(payload, dict),
            "test-double control handshake payload was not an object",
        )
        encoder = payload.get("encoder")
        require(
            isinstance(encoder, dict),
            "test-double control omitted encoder identity",
        )
        require(
            encoder.get("model") == MODEL_ARTIFACT_MARKER,
            "test-double control did not expose test-double/hash identity",
        )
        require(
            encoder.get("artifact_sha256") == "test-double/hash-v1",
            "test-double control artifact identity changed",
        )
        close = worker.close()
        return {
            "detected": True,
            "model": encoder.get("model"),
            "artifact_sha256": encoder.get("artifact_sha256"),
            "reply": reply,
            "operation": worker.operations[-1],
            "close": close,
            "state_root": str(control_root),
            "test_double": True,
        }
    except BaseException:
        if worker is not None:
            try:
                worker.close(suppress_errors=True)
            except BaseException:
                pass
        raise


def preflight(
    repository: Path,
    worker_path: Path,
    model_root: Path,
    embedding_manifest: Path,
    campaign_root: Path,
    config: CampaignConfig,
    *,
    deadline: float | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Admit exact worker/model pins and capture production identity evidence."""

    checker = _load_checker(repository)
    verified_worker: Any | None = None
    try:
        verified_worker = checker.verify_worker_artifact(worker_path, repo=repository)
        validate_real_encoder_markers(verified_worker)
    except Exception as error:
        if verified_worker is not None:
            verified_worker.close()
        raise StressFailure(f"worker admission blocked: {error}") from error
    except BaseException:
        if verified_worker is not None:
            verified_worker.close()
        raise
    try:
        worker_manifest_bytes_sha256 = sha256_bytes(
            Path(verified_worker["staged_manifest_path"]).read_bytes()
        )
    except Exception as error:
        verified_worker.close()
        raise StressFailure(f"worker manifest could not be hashed: {error}") from error
    except BaseException:
        verified_worker.close()
        raise
    try:
        model_identity = enrich_model_identity(
            checker.verify_model(model_root, embedding_manifest)
        )
    except Exception as error:
        verified_worker.close()
        raise StressFailure(f"model admission blocked: {error}") from error
    except BaseException:
        verified_worker.close()
        raise
    try:
        model_identity["runtime_manifest_sha256"] = runtime_manifest_digest(model_root)
    except BaseException:
        verified_worker.close()
        raise
    try:
        acquisition_receipt = validate_model_acquisition_receipt(
            repository,
            model_root,
            model_identity,
        )
    except Exception as error:
        verified_worker.close()
        raise StressFailure(
            f"model acquisition receipt admission blocked: {error}"
        ) from error
    except BaseException:
        verified_worker.close()
        raise
    try:
        staged_model_identity = enrich_model_identity(
            stage_model(
                model_root,
                campaign_root,
                embedding_manifest,
                checker,
                acquisition_receipt=acquisition_receipt["receipt"],
            )
        )
        staged_model_identity["runtime_manifest_sha256"] = runtime_manifest_digest(
            campaign_root
        )
        staged_acquisition_receipt = validate_model_acquisition_receipt(
            repository,
            campaign_root,
            staged_model_identity,
        )
        for field in (
            "model",
            "artifact_sha256",
            "revision",
            "revision_provenance",
            "manifest_sha256",
            "runtime_manifest_sha256",
            "files",
            "repository",
            "max_length",
            "pooling",
            "normalize",
        ):
            require(
                staged_model_identity.get(field) == model_identity.get(field),
                f"staged model {field} differed from the admitted model identity",
            )
    except BaseException:
        verified_worker.close()
        raise
    try:
        worker_manifest_path = Path(verified_worker["staged_manifest_path"])
        admitted_wire_identity = expected_wire_identity(
            verified_worker,
            worker_manifest_path,
            model_identity,
        )

        def probe_action(worker: WorkerProcess) -> dict[str, Any]:
            handshake_identity = handshake(
                worker,
                admitted_identity=admitted_wire_identity,
            )
            health_identity = health(
                worker,
                admitted_identity=admitted_wire_identity,
            )
            return {
                "handshake": handshake_identity,
                "health": health_identity,
            }

        probe_value, probe_session = run_session(
            verified_worker.launch_path,
            campaign_root,
            timeout_ms=config.operation_timeout_ms,
            action=probe_action,
            deadline=deadline,
        )
        production_probe = {
            **probe_value,
            "session": probe_session,
            "operations": probe_session["operations"],
            "test_double": False,
        }
        validate_production_identity(
            model_identity,
            probe_value["handshake"],
            probe_value["health"],
            admitted_wire_identity,
        )
        control_root = campaign_root.parent / (
            f"test-double-control-{os.getpid()}-{time.time_ns()}"
        )
        test_double_control = run_test_double_control(
            verified_worker.launch_path,
            control_root,
            config.operation_timeout_ms,
            deadline=deadline,
        )
        identity = {
            "worker": dict(verified_worker),
            "model": model_identity,
            "staged_model": staged_model_identity,
            "model_acquisition": {
                "source": acquisition_receipt,
                "staged": staged_acquisition_receipt,
            },
            "admitted_wire_identity": admitted_wire_identity,
            "real_encoder_markers": dict(verified_worker["required_markers"]),
            "protocol": {
                "version": PROTOCOL_VERSION,
                "identity": PROTOCOL_IDENTITY,
            },
            "algorithm_profile": ALGORITHM_PROFILE,
            "hashes": {
                "worker_sha256": verified_worker["sha256"],
                "worker_bytes": verified_worker["bytes"],
                "worker_manifest_sha256": worker_manifest_bytes_sha256,
                "worker_manifest_canonical_sha256": verified_worker["manifest_sha256"],
                "model_artifact_sha256": model_identity["artifact_sha256"],
                "model_revision": model_identity["revision"],
                "embedding_manifest_sha256": model_identity["manifest_sha256"],
                "runtime_model_manifest_sha256": model_identity[
                    "runtime_manifest_sha256"
                ],
                "model_files": model_identity["files"],
            },
            "production_probe": production_probe,
            "test_double_control": test_double_control,
            "validated": validate_production_identity(
                model_identity,
                probe_value["handshake"],
                probe_value["health"],
                admitted_wire_identity,
            ),
        }
        return verified_worker, identity
    except BaseException:
        verified_worker.close()
        raise


def _git_metadata(repository: Path) -> dict[str, Any]:
    """Capture the exact source commit without requiring a clean worktree."""

    result = subprocess.run(
        ["git", "-C", str(repository), "rev-parse", "HEAD"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return {"head": result.stdout.strip() if result.returncode == 0 else None}


def write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    """Atomically write the complete machine-readable campaign receipt."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    data = json.dumps(
        receipt,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    ).encode("utf-8")
    try:
        with temporary.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if hasattr(os, "O_DIRECTORY"):
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def new_receipt(
    config: CampaignConfig,
    repository: Path,
    output: Path,
    state_root: Path,
) -> dict[str, Any]:
    return {
        "schema_version": CAMPAIGN_SCHEMA_VERSION,
        "campaign": CAMPAIGN_NAME,
        "status": "running",
        "started_at_unix_ns": time.time_ns(),
        "source": _git_metadata(repository),
        "configuration": {
            "cold_trials": config.cold_trials,
            "warm_trials": config.warm_trials,
            "restart_trials": config.restart_trials,
            "concurrent_trials": config.concurrent_trials,
            "seed": config.seed,
            "concurrency": config.concurrency,
            "operation_timeout_ms": config.operation_timeout_ms,
            "trial_timeout_seconds": config.trial_timeout_seconds,
            "campaign_timeout_seconds": config.campaign_timeout_seconds,
        },
        "paths": {"receipt": str(output), "state_root": str(state_root)},
        "identity": None,
        "runtime_evidence": {},
        "phases": {},
        "failures": [],
        "blockers": [],
    }


def _receipt_trials(receipt: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        trial
        for phase in receipt.get("phases", {}).values()
        if isinstance(phase, dict)
        for trial in phase.get("trials", [])
        if isinstance(trial, dict)
    ]


def refresh_receipt_metrics(receipt: dict[str, Any], config: CampaignConfig) -> None:
    """Refresh counters for an incremental, fsynced receipt snapshot."""

    trials = _receipt_trials(receipt)
    trials = [trial for trial in trials if isinstance(trial, dict)]
    try:
        planned: int | None = config.planned_trials
    except (TypeError, ValueError):
        planned = None
    passed = sum(trial.get("status") == "pass" for trial in trials)
    failed = sum(trial.get("status") == "failed" for trial in trials)
    incomplete = sum(trial.get("status") == "incomplete" for trial in trials)
    interrupted = sum(trial.get("status") == "interrupted" for trial in trials)
    misses = sum(contains_required_record_miss(trial) for trial in trials)
    receipt["counts"] = {
        "planned": planned,
        "completed": passed + failed + interrupted,
        "passed": passed,
        "failed": failed,
        "interrupted": interrupted,
        "incomplete": incomplete,
        "required_record_misses": misses,
    }
    trial_evidence = [
        trial.get("evidence")
        for trial in trials
        if isinstance(trial.get("evidence"), dict)
    ]
    receipt["evidence"] = {
        "durable_observations": sum(
            len(evidence.get("observations", [])) for evidence in trial_evidence
        ),
        "durable_recalls": sum(
            len(evidence.get("recalls", [])) for evidence in trial_evidence
        ),
        "scope_probes": sum(
            len(evidence.get("scope_probes", [])) for evidence in trial_evidence
        ),
        "operation_receipts": sum(
            len(session.get("operations", []))
            for evidence in trial_evidence
            for session in evidence.get("sessions", [])
            if isinstance(session, dict)
        ),
    }


def finalize_receipt(receipt: dict[str, Any], config: CampaignConfig) -> None:
    """Compute strict completeness and miss counters from retained trials."""

    refresh_receipt_metrics(receipt, config)
    trials = _receipt_trials(receipt)
    planned = receipt["counts"].get("planned")
    if planned is not None and len(trials) != planned:
        receipt["failures"].append(
            f"incomplete run: retained {len(trials)} of {planned} planned trials"
        )
    if receipt["counts"]["incomplete"]:
        receipt["failures"].append(
            f"{receipt['counts']['incomplete']} trial(s) did not complete"
        )
    if receipt["counts"]["interrupted"]:
        receipt["failures"].append(
            f"{receipt['counts']['interrupted']} trial(s) were interrupted"
        )
    if receipt["counts"]["required_record_misses"]:
        receipt["failures"].append(
            f"{receipt['counts']['required_record_misses']} trial(s) missed a required record"
        )
    receipt["finished_at_unix_ns"] = time.time_ns()


def materialize_unstarted_phases(
    receipt: dict[str, Any],
    config: CampaignConfig,
    phases: Sequence[str],
    *,
    message: str,
    error_type: str,
    error_code: str,
    deadline_monotonic: float | None,
) -> None:
    """Keep a durable incomplete row for every phase that never started."""

    for phase in phases:
        if phase not in receipt["phases"]:
            receipt["phases"][phase] = incomplete_phase(
                config,
                phase,
                message=message,
                error_type=error_type,
                error_code=error_code,
                deadline_monotonic=deadline_monotonic,
            )
            continue
        current = receipt["phases"][phase]
        if not isinstance(current, dict):
            receipt["phases"][phase] = incomplete_phase(
                config,
                phase,
                message=message,
                error_type=error_type,
                error_code=error_code,
                deadline_monotonic=deadline_monotonic,
            )
            continue
        trials = current.get("trials")
        if not isinstance(trials, list):
            trials = []
            current["trials"] = trials
        planned = config.counts[phase]
        if len(trials) < planned:
            trials.extend(
                incomplete_trials(
                    config,
                    phase,
                    start_index=len(trials),
                    message=message,
                    error_type=error_type,
                    error_code=error_code,
                    deadline_monotonic=deadline_monotonic,
                )
            )
            receipt["phases"][phase] = phase_result(planned, trials)


def run_campaign(
    repository: Path,
    worker_path: Path,
    model_root: Path,
    embedding_manifest: Path,
    output: Path,
    state_root: Path,
    config: CampaignConfig,
) -> int:
    """Run preflight and all bounded phases, always leaving a receipt."""

    receipt = new_receipt(config, repository, output, state_root)
    artifact: Any | None = None
    exit_code = 1
    interrupted = False
    campaign_deadline: float | None = None
    previous_sigterm: Any = None
    sigterm_installed = False

    def persist_snapshot() -> None:
        refresh_receipt_metrics(receipt, config)
        write_receipt(output, receipt)

    try:
        previous_sigterm = signal.getsignal(signal.SIGTERM)

        def handle_sigterm(_signum: int, _frame: Any) -> None:
            raise CampaignTerminated()

        signal.signal(signal.SIGTERM, handle_sigterm)
        sigterm_installed = True
    except (OSError, ValueError):
        # A caller running the harness off the main thread cannot install a
        # process signal handler. Incremental snapshots still retain all
        # completed work in that embedding.
        sigterm_installed = False
    try:
        persist_snapshot()
        try:
            config.validate()
        except KeyboardInterrupt as error:
            interrupted = True
            receipt["status"] = "interrupted"
            receipt["failures"].append(failure_evidence(error))
            materialize_invalid_config_phases(
                receipt,
                config,
                message="campaign interrupted during configuration validation",
                error_type=type(error).__name__,
                error_code="campaign_interrupted",
            )
            exit_code = 130
        except Exception as error:
            receipt["status"] = "blocked"
            receipt["blockers"].append(failure_evidence(error))
            materialize_invalid_config_phases(
                receipt,
                config,
                message="configuration validation blocked the campaign",
                error_type=type(error).__name__,
                error_code="invalid_configuration",
            )
            exit_code = 2
        else:
            campaign_deadline = time.monotonic() + config.campaign_timeout_seconds
            try:
                artifact, identity = preflight(
                    repository,
                    worker_path,
                    model_root,
                    embedding_manifest,
                    state_root,
                    config,
                    deadline=campaign_deadline,
                )
            except KeyboardInterrupt as error:
                interrupted = True
                receipt["status"] = "interrupted"
                receipt["failures"].append(failure_evidence(error))
                materialize_unstarted_phases(
                    receipt,
                    config,
                    PHASES,
                    message="campaign interrupted before trial start",
                    error_type="KeyboardInterrupt",
                    error_code="campaign_interrupted",
                    deadline_monotonic=campaign_deadline,
                )
                exit_code = 130
            except Exception as error:
                receipt["status"] = "blocked"
                receipt["blockers"].append(failure_evidence(error))
                materialize_unstarted_phases(
                    receipt,
                    config,
                    PHASES,
                    message="preflight blocked before trial start",
                    error_type=type(error).__name__,
                    error_code="preflight_blocked",
                    deadline_monotonic=campaign_deadline,
                )
                exit_code = 2
            except BaseException as error:
                receipt["status"] = "failed"
                receipt["failures"].append(failure_evidence(error))
                materialize_unstarted_phases(
                    receipt,
                    config,
                    PHASES,
                    message="campaign failed before trial start",
                    error_type=type(error).__name__,
                    error_code="campaign_failed",
                    deadline_monotonic=campaign_deadline,
                )
                exit_code = 1
            else:
                receipt["identity"] = identity
                persist_snapshot()
                cancellation = run_real_cancellation_probe(
                    artifact.launch_path,
                    state_root,
                    config,
                    admitted_identity=identity["admitted_wire_identity"],
                    campaign_deadline=campaign_deadline,
                )
                receipt["runtime_evidence"] = {"cancellation": cancellation}
                persist_snapshot()
                try:
                    for phase_index, phase in enumerate(PHASES):
                        try:
                            receipt["phases"][phase] = run_phase(
                                artifact.launch_path,
                                state_root,
                                config,
                                phase,
                                campaign_deadline=campaign_deadline,
                                admitted_identity=identity["admitted_wire_identity"],
                                receipt=receipt,
                                persist=persist_snapshot,
                            )
                        except KeyboardInterrupt:
                            materialize_unstarted_phases(
                                receipt,
                                config,
                                PHASES[phase_index:],
                                message="campaign interrupted before trial start",
                                error_type="KeyboardInterrupt",
                                error_code="campaign_interrupted",
                                deadline_monotonic=campaign_deadline,
                            )
                            persist_snapshot()
                            raise
                        except BaseException as error:
                            materialize_unstarted_phases(
                                receipt,
                                config,
                                PHASES[phase_index:],
                                message="campaign failed before trial start",
                                error_type=type(error).__name__,
                                error_code="campaign_failed",
                                deadline_monotonic=campaign_deadline,
                            )
                            persist_snapshot()
                            raise
                        if receipt["phases"][phase].get("interrupted", 0):
                            interrupted = True
                            materialize_unstarted_phases(
                                receipt,
                                config,
                                PHASES[phase_index + 1 :],
                                message="campaign interrupted before trial start",
                                error_type="KeyboardInterrupt",
                                error_code="campaign_interrupted",
                                deadline_monotonic=campaign_deadline,
                            )
                            persist_snapshot()
                            break
                except KeyboardInterrupt as error:
                    interrupted = True
                    receipt["status"] = "interrupted"
                    receipt["failures"].append(failure_evidence(error))
                    exit_code = 130
                except BaseException as error:
                    receipt["status"] = "failed"
                    receipt["failures"].append(failure_evidence(error))
                    exit_code = 1
                else:
                    exit_code = 0
    except KeyboardInterrupt as error:
        interrupted = True
        receipt["status"] = "interrupted"
        receipt["failures"].append(failure_evidence(error))
        if campaign_deadline is not None:
            materialize_unstarted_phases(
                receipt,
                config,
                PHASES,
                message="campaign interrupted before trial start",
                error_type=type(error).__name__,
                error_code="campaign_interrupted",
                deadline_monotonic=campaign_deadline,
            )
        else:
            materialize_invalid_config_phases(
                receipt,
                config,
                message="campaign interrupted before configuration completed",
                error_type=type(error).__name__,
                error_code="campaign_interrupted",
            )
        exit_code = 130
    except BaseException as error:
        receipt["status"] = "failed"
        receipt["failures"].append(failure_evidence(error))
        if campaign_deadline is not None:
            materialize_unstarted_phases(
                receipt,
                config,
                PHASES,
                message="campaign failed before trial start",
                error_type=type(error).__name__,
                error_code="campaign_failed",
                deadline_monotonic=campaign_deadline,
            )
        else:
            materialize_invalid_config_phases(
                receipt,
                config,
                message="campaign failed before configuration completed",
                error_type=type(error).__name__,
                error_code="campaign_failed",
            )
        exit_code = 1
    finally:
        if artifact is not None:
            try:
                artifact.close()
            except BaseException as error:
                receipt["failures"].append(failure_evidence(error))
                if isinstance(error, KeyboardInterrupt):
                    interrupted = True
                    receipt["status"] = "interrupted"
                    exit_code = 130
                elif not interrupted:
                    receipt["status"] = "failed"
                    exit_code = 1
        try:
            finalize_receipt(receipt, config)
        except BaseException as error:
            receipt["failures"].append(failure_evidence(error))
            if isinstance(error, KeyboardInterrupt):
                interrupted = True
            receipt["status"] = "interrupted" if interrupted else "failed"
            exit_code = 130 if interrupted else 1
        else:
            counts = receipt["counts"]
            if interrupted or counts.get("interrupted", 0):
                receipt["status"] = "interrupted"
                exit_code = 130
            elif receipt["status"] == "running":
                receipt["status"] = (
                    "pass"
                    if (
                        counts["completed"] == counts["planned"]
                        and counts["failed"] == 0
                        and counts["interrupted"] == 0
                        and counts["incomplete"] == 0
                        and counts["required_record_misses"] == 0
                    )
                    else "failed"
                )
                exit_code = 0 if receipt["status"] == "pass" else 1
        try:
            write_receipt(output, receipt)
        except BaseException as error:
            # There is no durable path left if atomic receipt writing itself fails.
            if isinstance(error, KeyboardInterrupt):
                interrupted = True
            exit_code = 130 if interrupted else 1
        if sigterm_installed:
            try:
                signal.signal(signal.SIGTERM, previous_sigterm)
            except BaseException:
                pass
    return exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--worker", type=Path, help="absolute production worker executable"
    )
    parser.add_argument(
        "--model-root",
        type=Path,
        help=(
            "verified local state root containing models/ "
            "(defaults to $TRACEDECAY_NCM_MODEL_ROOT or "
            "$TRACEDECAY_NCM_REAL_MODEL_ROOT)"
        ),
    )
    parser.add_argument(
        "--embedding-manifest", type=Path, help="checked-in embedding manifest"
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        help="fresh campaign state root (model is copied once)",
    )
    parser.add_argument("--output", type=Path, help="durable JSON receipt path")
    parser.add_argument("--seed", type=int, default=20260916)
    parser.add_argument("--cold-trials", type=int, default=DEFAULT_TRIALS_PER_PHASE)
    parser.add_argument("--warm-trials", type=int, default=DEFAULT_TRIALS_PER_PHASE)
    parser.add_argument("--restart-trials", type=int, default=DEFAULT_TRIALS_PER_PHASE)
    parser.add_argument(
        "--concurrent-trials", type=int, default=DEFAULT_TRIALS_PER_PHASE
    )
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument(
        "--operation-timeout-ms", type=int, default=DEFAULT_OPERATION_TIMEOUT_MS
    )
    parser.add_argument(
        "--trial-timeout-seconds", type=float, default=DEFAULT_TRIAL_TIMEOUT_SECONDS
    )
    parser.add_argument(
        "--campaign-timeout-seconds",
        type=float,
        default=DEFAULT_CAMPAIGN_TIMEOUT_SECONDS,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repository = REPOSITORY_ROOT
    worker = args.worker or Path(
        os.environ.get("TRACEDECAY_NCM_WORKER", default_worker_path(repository))
    )
    configured_model_root = os.environ.get(
        "TRACEDECAY_NCM_MODEL_ROOT"
    ) or os.environ.get("TRACEDECAY_NCM_REAL_MODEL_ROOT")
    model_root = args.model_root or Path(
        configured_model_root
        if configured_model_root
        else _target_dir(repository) / "ncm-backend-model-root"
    )
    embedding_manifest = args.embedding_manifest or (
        repository / "product" / "ncm" / "reference" / "embedding-manifest.json"
    )
    output = args.output or Path(
        os.environ.get("TRACEDECAY_NCM_STRESS_RECEIPT", default_output_path(repository))
    )
    state_root = args.state_root or Path(
        os.environ.get(
            "TRACEDECAY_NCM_STRESS_STATE_ROOT", default_state_root(repository)
        )
    )
    worker = worker.absolute()
    model_root = model_root.absolute()
    embedding_manifest = embedding_manifest.absolute()
    output = output.absolute()
    state_root = state_root.absolute()
    config = CampaignConfig(
        cold_trials=args.cold_trials,
        warm_trials=args.warm_trials,
        restart_trials=args.restart_trials,
        concurrent_trials=args.concurrent_trials,
        seed=args.seed,
        concurrency=args.concurrency,
        operation_timeout_ms=args.operation_timeout_ms,
        trial_timeout_seconds=args.trial_timeout_seconds,
        campaign_timeout_seconds=args.campaign_timeout_seconds,
    )
    return run_campaign(
        repository,
        worker,
        model_root,
        embedding_manifest,
        output,
        state_root,
        config,
    )


if __name__ == "__main__":
    raise SystemExit(main())
