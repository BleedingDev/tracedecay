#!/usr/bin/env python3
"""Run the direct-original versus product Native comparison.

The runner is deliberately a thin process and evidence boundary.  It does not
implement a memory store, score results, or manufacture a baseline response.
Each side receives the same logical request in a separate process group and a
separate state/artifact root.  The original side is always the immutable 570
checkout supplied by the build owner; a dirty or differently pinned checkout
is refused before a case starts.  Historical b3/571 audit revisions remain
metadata only and are never used as an execution oracle.

The case contract is in ``case-contract.json``.  The usual entry point is the
shipped ``tracedecay tool`` CLI, which dispatches through the production daemon
and MCP tool handlers.  An MCP stdio entry point and an explicitly supplied
argv are also supported for original operations which are not exposed by the
CLI wrapper.  Raw request, stdout, stderr, and process evidence is retained on
success and failure.
"""

from __future__ import annotations

import argparse
import copy
import ctypes
import hashlib
import hmac
import json
import math
import os
import re
import signal
import stat
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


CONTRACT_FORMAT = "tracedecay.native-original.case-contract.v1"
BUILD_ATTESTATION_FORMAT = "tracedecay.native-original.build-attestation.v1"
NCM_WORKER_ATTESTATION_FORMAT = "tracedecay.native-original.ncm-worker-attestation.v1"
# The worker executable is a separately attested runtime, but its protocol
# identity is still part of the frozen NCM contract. Do not accept an
# arbitrary worker label merely because a caller recomputed its pin digest.
NCM_WORKER_ID = "ncm-worker-v1"
# The worker manifest is a trust input to the runner.  The daemon/child may
# report observations, but it may not introduce or replace any of these
# pins.  Keeping the complete set here also gives the case/ledger builders one
# canonical field list to validate.
NCM_WORKER_ATTESTATION_FIELDS = (
    "format",
    "kind",
    "worker_id",
    "binary_path",
    "binary_sha256",
    "source_root",
    "source_revision",
    "protocol_version",
    "implementation_sha256",
    "model_artifact_sha256",
    "tokenizer_sha256",
    "vector_fixture_digest",
    "pin_sha256",
)
NCM_WORKER_PIN_FIELDS = (
    "worker_id",
    "binary_sha256",
    "source_revision",
    "protocol_version",
    "implementation_sha256",
    "model_artifact_sha256",
    "tokenizer_sha256",
    "vector_fixture_digest",
)
# Receipt MAC keys are deliberately retained only in memory.  Once a daemon
# result has been authenticated, this table lets artifact builders and
# same-process reloads bind the public receipt back to the proof that the
# runner actually held.  A copied receipt with a recomputed
# ``receipt_sha256`` cannot enter this table because registration happens only
# after MAC verification in ``_validate_daemon_action_receipt``.
_VERIFIED_RECEIPT_PROOFS: dict[str, dict[str, str]] = {}
REFERENCE_REVISION = "57006f60cb45bcee8487e73a40d4fad1a12ee2b6"
# The product checkout is a moving integration lane.  The actual candidate
# revision is captured from --product-source-revision (or the source root)
# for every run instead of being frozen to a historical audit commit.
PRODUCT_REVISION = "current"
HISTORICAL_REFERENCE_REVISION = "b3b43410e47115056f2066449aafa1822bbb6049"
HISTORICAL_PRODUCT_REVISION = "571daf3a9612e5247443e4da3a107b542686c1ef"
REFERENCE_CHECKOUT_NAME = "native-original-reference-57006f60"
DEFAULT_CONTRACT = Path(__file__).with_name("case-contract.json")
OUTCOME_STATUSES = (
    "pass",
    "fail",
    "unknown",
    "unsupported",
    "invalid",
    "censored",
    "cancelled",
    "partial",
    "effect_unknown",
    "blocked",
)
# Keep the complete production status vocabulary visible at the runner
# boundary. These values are mapped to conservative comparison outcomes;
# none of the non-terminal or negative statuses may silently become pass.
RETAINED_OUTCOME_STATUS_V1 = (
    "aborted",
    "budget_exhausted",
    "busy",
    "cancelled",
    "complete",
    "complete_zero",
    "cursor_manifest_limit_exceeded",
    "deadline_exceeded",
    "deleted",
    "denied",
    "error",
    "failed",
    "joined",
    "locked",
    "not_found",
    "ok",
    "partial",
    "recorded",
    "redacted",
    "running",
    "stale",
    "started",
    "unavailable",
    "unsupported_filter",
    "wrong_scope",
)
CAUSAL_STAGES = (
    "setup",
    "spawn",
    "transport",
    "composition",
    "effect",
    "cleanup",
    "comparison",
    "runner",
)
ROUTE_AVAILABILITY = ("supported", "external_harness", "unsupported", "unknown")
SIDE_NAMES = ("original", "product")
COMPOSITIONS = {"original": "direct_original", "product": "product_native"}
READINESS_MODES = ("comparison", "readiness")
READINESS_TRACKS = ("native", "ncm", "semantic", "release")
# The frozen matrix names the installed entry point.  Source-checkout callers
# may invoke runner.py through Python, but the release/build step must expose
# this same command name for matrix replay.
NORMATIVE_RUNNER_COMMAND = "native-original-runner"
IMMUTABLE_COPY_MODE = 0o500
IMMUTABLE_COPY_FILE_MODE = 0o500
CANONICAL_ATTEMPT_LEDGER_NAME = "attempts.jsonl"
# A case artifact is admissible as readiness evidence only when the
# append-only row can be re-opened through this exact path.  The basename is
# useful for human inspection, but is not an identity: accepting it alone
# permits a receipt copied from a different run to bless this run.
CASE_RESULT_NAME = "case-result.json"
DAEMON_ACTION_RECEIPT_FORMAT = "tracedecay.native-original.daemon-action-receipt.v1"
DAEMON_ACTION_RECEIPT_REQUIRED_FIELDS = (
    "format",
    "side",
    "route",
    "entrypoint",
    "action_id",
    "request_sha256",
    "authority_digest",
    "store_identity",
    "effect",
    "receipt",
    "state",
    "daemon_identity",
    "issued_at",
    "receipt_sha256",
)
READINESS_MATRIX_ID = "native-ncm-semantic-readiness.v1"
# Readiness is governed by the frozen acceptance artifact, not by the small
# public route list used to validate hand-authored comparison contracts.
DEFAULT_READINESS_MATRIX = (
    Path(__file__).resolve().parents[3]
    / ".codex"
    / "plans"
    / "native-original"
    / "execution-results"
    / "readiness-matrix.json"
)
FROZEN_READINESS_MATRIX_SHA256 = "0115793c5909113571f9b5a9f0f73008aa8ba3d5745a904bb34d3a8d689db227"
FROZEN_READINESS_ROW_COUNT = 201
FROZEN_READINESS_PLANNED_ATTEMPTS = 1381
FROZEN_READINESS_ROW_IDS_SHA256 = "1ad228fabfa48dd86371aa9fcf4fb98defba4d62bc1d1cafcb5be7c729c4edee"
FROZEN_READINESS_ORACLE_RUNS_SHA256 = "eecce27fa55b3a421d3bee89b5503d7e35b75262cccc2134e632bbe2f3a24978"
FROZEN_READINESS_ROW_BINDINGS_SHA256 = "a8f46e8c87b0e83ab09798962b101a1fef7428c1202001c166b87ac29b19f445"
REQUIRED_NATIVE_ROUTES = frozenset(
    {
        "fact_store_add",
        "fact_store_search",
        "fact_store_probe",
        "fact_store_related",
        "fact_store_reason",
        "fact_store_contradict",
        "fact_store_get",
        "fact_store_update",
        "fact_store_remove",
        "fact_store_supersede",
        "fact_store_list",
        "fact_feedback",
        "memory_status",
    }
)
REQUIRED_SESSION_LCM_ROUTES = frozenset(
    {
        "session_lookup",
        "message_search",
        "sessions_for",
        "session_refresh_begin",
        "session_refresh_status",
        "session_refresh_cancel",
        "lcm_load_session",
        "lcm_grep",
        "lcm_describe",
        "lcm_expand",
        "lcm_expand_query",
        "lcm_status",
        "lcm_doctor",
    }
)
REQUIRED_HOST_EXTENSION_ROUTES = frozenset(
    {"host_sealed_source_admission", "host_cursor_and_locator_regression"}
)
# The daemon's receipt route mapper accepts both the legacy generic
# ``tracedecay_fact_store`` dispatcher (with an explicit ``action`` argument)
# and these action-specific MCP tools.  The accepted runner contract uses the
# action-specific forms so the callable route is unambiguous before prepare.
# Keep this table in the runner as an integrity check on the frozen contract;
# a contract edit cannot silently turn one fact operation into another tool.
FACT_STORE_MCP_ROUTE_TOOLS = {
    "fact_store_add": "tracedecay_fact_store_add",
    "fact_store_search": "tracedecay_fact_store_search",
    "fact_store_probe": "tracedecay_fact_store_probe",
    "fact_store_related": "tracedecay_fact_store_related",
    "fact_store_reason": "tracedecay_fact_store_reason",
    "fact_store_contradict": "tracedecay_fact_store_contradict",
    "fact_store_get": "tracedecay_fact_store_get",
    "fact_store_update": "tracedecay_fact_store_update",
    "fact_store_remove": "tracedecay_fact_store_remove",
    "fact_store_supersede": "tracedecay_fact_store_supersede",
    "fact_store_list": "tracedecay_fact_store_list",
    "fact_store_curate": "tracedecay_fact_store_curate",
    "fact_feedback": "tracedecay_fact_feedback",
    "memory_status": "tracedecay_memory_status",
}
LEDGER_REQUIRED_FIELDS = (
    "actual",
    "attempt_id",
    "attempt_index",
    "authority_correlated",
    "candidate_binary_sha256",
    "candidate_process_identity",
    "candidate_store_identity",
    "command",
    "composition_reached",
    "effect_digest",
    "evidence_paths",
    "expected",
    "failure_class",
    "feature_set",
    "first_causal_stage",
    "fixture_seed",
    "model_sha256",
    "oracle_kind",
    "outcome",
    "profile_paths",
    "protocol_version",
    "provider_id",
    "provider_implementation_sha256",
    "receipt_digest",
    "reference_binary_sha256",
    "reference_process_identity",
    "reference_store_identity",
    "reopen_or_no_effect",
    "request_digest",
    "result_digest",
    "row_id",
    "scope_digest",
    "source_sha",
    "started_at",
    "state_digest",
    "state_generation",
    "state_schema_version",
    "tokenizer_sha256",
)
_ISOLATION_ENV_KEYS = frozenset(
    {
        "HOME",
        "USERPROFILE",
        "XDG_DATA_HOME",
        "XDG_CONFIG_HOME",
        "XDG_RUNTIME_DIR",
        "TRACEDECAY_HOME",
        "TRACEDECAY_PROFILE_DIR",
        "TRACEDECAY_DATA_DIR",
        "TRACEDECAY_GLOBAL_DB",
        "TRACEDECAY_COMPARISON_SIDE_ROOT",
        "TRACEDECAY_COMPARISON_PROCESS_ROOT",
        "TRACEDECAY_DAEMON_SOCKET",
        "TRACEDECAY_COMPARISON_AUTHORITY_DIGEST",
    }
)
_UNTRUSTED_ENV_KEYS = frozenset(
    {
        "PATH",
        "TMPDIR",
        "TMP",
        "TEMP",
        "BASH_ENV",
        "ENV",
        "CDPATH",
        "PYTHONPATH",
        "PYTHONHOME",
        "RUBYLIB",
        "PERL5LIB",
        "NODE_OPTIONS",
        "RUSTFLAGS",
        "CARGO_BUILD_RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_SYSTEM",
        "GIT_CONFIG_NOSYSTEM",
    }
)
_UNTRUSTED_ENV_PREFIXES = ("GIT_", "LD_", "DYLD_")


def _unsafe_environment_key(key: Any) -> bool:
    """Return whether a child environment key can redirect execution or probes."""

    if not isinstance(key, str):
        return True
    upper = key.upper()
    return (
        key in _ISOLATION_ENV_KEYS
        or key in _UNTRUSTED_ENV_KEYS
        or upper.startswith("TRACEDECAY_")
        or any(upper.startswith(prefix) for prefix in _UNTRUSTED_ENV_PREFIXES)
    )
# `${name}` is the published contract spelling.  Braced `{name}` is accepted
# for old hand-authored fixtures so they fail with a useful unknown-key error
# instead of being passed literally to a child; generated contract examples
# use the canonical `${name}` form.
_PLACEHOLDER = re.compile(r"\$\{([A-Za-z0-9_.-]+)\}|\{([A-Za-z0-9_.-]+)\}")

# Values captured from a daemon handshake are runner evidence, never template
# inputs for a later child.  In particular, a case must not bind a previous
# response's challenge answer and echo it from the next request.  Keep this
# deny-list narrow enough for ordinary response fields while covering the
# authentication/protocol envelopes that can carry a challenge answer.
_RENDER_SECRET_KEYS = frozenset(
    {
        "auth_token",
        "authority_token",
        "challenge_response",
        "authority_evidence",
        "authority_protocol_evidence",
        "protocol_evidence",
    }
)


class RunnerError(RuntimeError):
    """A setup, contract, transport, or comparison preparation failure."""


class ContractError(RunnerError):
    """The case contract or a case does not satisfy the published shape."""


class ReferenceError(RunnerError):
    """The protected 570 source checkout is absent, dirty, or mis-pinned."""


class BinaryError(RunnerError):
    """A selected runtime binary cannot be used by the comparison."""


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError) as error:
        raise RunnerError(f"value cannot be encoded as strict JSON: {error}") from error


def json_digest(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def bytes_digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_bytes(path: Path, value: bytes) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())
    return {"path": str(path), "bytes": len(value), "sha256": bytes_digest(value)}


def _write_json(path: Path, value: Any) -> dict[str, Any]:
    return _write_bytes(path, _json_bytes(value) + b"\n")


def _case_result_digest_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return the canonical case-result payload used for its digest.

    The digest field is deliberately replaced with ``null`` so the digest
    does not refer to itself.  Nested attempt rows carry the same required
    field for ledger shape, but their value is also excluded from the digest;
    the append-only ledger receipt in the enclosing result authenticates that
    row without introducing a receipt/digest cycle.
    """

    _require(isinstance(value, Mapping), "case result must be an object", RunnerError)
    try:
        payload = copy.deepcopy(dict(value))
    except (RecursionError, TypeError, ValueError) as error:
        raise RunnerError(f"case result contains malformed or too deeply nested data: {error}") from error
    payload["result_digest"] = None
    attempts = payload.get("attempt_ledger")
    if isinstance(attempts, list):
        normalized: list[Any] = []
        for attempt in attempts:
            if isinstance(attempt, Mapping):
                attempt_value = dict(attempt)
                attempt_value["result_digest"] = None
                normalized.append(attempt_value)
            else:
                normalized.append(attempt)
        payload["attempt_ledger"] = normalized
    return payload


def case_result_digest(value: Mapping[str, Any]) -> str:
    """Hash the canonical final case-result content using its newline policy."""

    return bytes_digest(_json_bytes(_case_result_digest_payload(value)) + b"\n")


def _finalize_case_result_digest(value: dict[str, Any]) -> str:
    """Set the final case-result digest after all evidence fields are known."""

    _require(isinstance(value, dict), "case result must be a mutable object", RunnerError)
    attempts = value.get("attempt_ledger")
    if isinstance(attempts, list):
        for attempt in attempts:
            if isinstance(attempt, dict):
                # This field is required in append-only rows, but keeping it
                # null in both serialized copies avoids a digest/receipt cycle.
                attempt["result_digest"] = None
    value["result_digest"] = None
    digest = case_result_digest(value)
    value["result_digest"] = digest
    return digest


def verify_case_result_digest(
    value: Mapping[str, Any],
    *,
    raw_bytes: bytes | None = None,
) -> None:
    """Verify a finalized case result and, when supplied, its exact bytes."""

    _require(isinstance(value, Mapping), "case result must be an object", RunnerError)
    observed = value.get("result_digest")
    _require(
        isinstance(observed, str) and re.fullmatch(r"[0-9a-f]{64}", observed) is not None,
        "case result digest is missing or malformed",
        RunnerError,
    )
    attempts = value.get("attempt_ledger")
    if attempts is not None:
        _require(isinstance(attempts, list), "case result attempt_ledger must be a list", RunnerError)
        for index, attempt in enumerate(attempts):
            _require(
                isinstance(attempt, Mapping),
                f"case result attempt ledger row {index} must be an object",
                RunnerError,
            )
            _require(
                "result_digest" in attempt and attempt.get("result_digest") is None,
                f"case result attempt ledger row {index} must carry a null result_digest",
                RunnerError,
            )
            _require(
                _ledger_row_shape_valid(attempt),
                f"case result attempt ledger row {index} is missing its complete identity/field shape",
                RunnerError,
            )
    expected = case_result_digest(value)
    _require(observed == expected, "case result digest does not match its canonical payload", RunnerError)
    if raw_bytes is not None:
        _require(
            isinstance(raw_bytes, bytes),
            "case result bytes must be bytes",
            RunnerError,
        )
        try:
            expected_bytes = _json_bytes(value) + b"\n"
        except RunnerError:
            raise
        _require(
            raw_bytes == expected_bytes,
            "case result bytes are not the canonical newline-terminated JSON",
            RunnerError,
        )


def _append_jsonl(path: Path, value: Any) -> dict[str, Any]:
    """Append one fsynced immutable ledger row and return its file receipt."""

    _require(
        isinstance(path, Path) and path.name == CANONICAL_ATTEMPT_LEDGER_NAME,
        f"attempt ledger path must be named {CANONICAL_ATTEMPT_LEDGER_NAME}",
        RunnerError,
    )
    encoded = _json_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        lexical_info = path.lstat() if path.exists() or path.is_symlink() else None
        if lexical_info is not None:
            _require(
                stat.S_ISREG(lexical_info.st_mode) and not stat.S_ISLNK(lexical_info.st_mode),
                "attempt ledger path must be a regular non-symlink file",
                RunnerError,
            )
        flags = os.O_APPEND | os.O_WRONLY | os.O_CREAT
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(os.fspath(path), flags, 0o600)
        with os.fdopen(descriptor, "ab", closefd=True) as stream:
            opened_info = os.fstat(stream.fileno())
            _require(stat.S_ISREG(opened_info.st_mode), "attempt ledger descriptor is not regular", RunnerError)
            offset = opened_info.st_size
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
            closed_info = os.fstat(stream.fileno())
        final_info = path.lstat()
        _require(
            not stat.S_ISLNK(final_info.st_mode)
            and (final_info.st_dev, final_info.st_ino) == (opened_info.st_dev, opened_info.st_ino)
            and closed_info.st_size == offset + len(encoded),
            "attempt ledger path changed while appending",
            RunnerError,
        )
    except RunnerError:
        raise
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise RunnerError(f"attempt ledger append failed: {error}") from error
    return {
        "path": str(path),
        "offset": offset,
        "bytes": len(encoded),
        "sha256": bytes_digest(encoded),
    }


def _read_stable_file(
    path: Path,
    *,
    label: str,
    max_bytes: int = 16 * 1024 * 1024,
) -> tuple[bytes, os.stat_result]:
    """Read one regular file through a stable descriptor.

    The lexical path is checked before opening and again after the descriptor
    has been consumed.  ``O_NOFOLLOW`` (where available), descriptor
    ``fstat`` and the final lexical ``lstat`` together reject a symlink or a
    path replacement during an attestation/authority read.  Callers therefore
    hash and parse the bytes that came from one stable file handle.
    """

    _require(isinstance(path, Path), f"{label} path must be a Path", RunnerError)
    try:
        lexical = path.expanduser()
        if not lexical.is_absolute():
            lexical = Path.cwd() / lexical
        lexical_info = lexical.lstat()
        _require(
            stat.S_ISREG(lexical_info.st_mode) and not stat.S_ISLNK(lexical_info.st_mode),
            f"{label} path must be a regular non-symlink file",
            RunnerError,
        )
        flags = os.O_RDONLY
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(os.fspath(lexical), flags)
    except RunnerError:
        raise
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise RunnerError(f"{label} cannot be opened: {error}") from error
    try:
        opened_info = os.fstat(descriptor)
        _require(
            stat.S_ISREG(opened_info.st_mode)
            and (opened_info.st_dev, opened_info.st_ino)
            == (lexical_info.st_dev, lexical_info.st_ino),
            f"{label} changed between lexical inspection and open",
            RunnerError,
        )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, max_bytes - total + 1))
            if not chunk:
                break
            total += len(chunk)
            _require(total <= max_bytes, f"{label} exceeds the bounded read size", RunnerError)
            chunks.append(chunk)
        closed_info = os.fstat(descriptor)
        _require(
            stat.S_ISREG(closed_info.st_mode)
            and (closed_info.st_dev, closed_info.st_ino)
            == (opened_info.st_dev, opened_info.st_ino)
            and closed_info.st_size == opened_info.st_size
            and closed_info.st_mtime_ns == opened_info.st_mtime_ns,
            f"{label} changed while it was being read",
            RunnerError,
        )
        final_info = lexical.lstat()
        _require(
            stat.S_ISREG(final_info.st_mode)
            and not stat.S_ISLNK(final_info.st_mode)
            and (final_info.st_dev, final_info.st_ino)
            == (opened_info.st_dev, opened_info.st_ino)
            and final_info.st_size == opened_info.st_size
            and final_info.st_mtime_ns == opened_info.st_mtime_ns,
            f"{label} lexical path changed after it was read",
            RunnerError,
        )
        return b"".join(chunks), closed_info
    except RunnerError:
        raise
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise RunnerError(f"{label} could not be read stably: {error}") from error
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass


def _ledger_row_shape_valid(
    value: Any,
    *,
    expected_case_id: str | None = None,
    expected_attempt_id: str | None = None,
    expected_attempt_index: int | None = None,
    require_complete: bool = True,
) -> bool:
    """Validate the identity/shape of one authoritative ledger row.

    The nested row is part of the case-result authentication boundary.  It
    therefore has to carry the complete ledger contract, including a
    nullable nested digest, and its case identity cannot be inferred from a
    receipt path or from a caller supplied outer label.
    """

    if not isinstance(value, Mapping):
        return False
    if require_complete and any(field not in value for field in LEDGER_REQUIRED_FIELDS):
        return False
    if "result_digest" not in value or value.get("result_digest") is not None:
        return False
    row_id = value.get("row_id")
    case_id = value.get("case_id")
    if not isinstance(row_id, str) or not row_id.strip():
        return False
    if not isinstance(case_id, str) or case_id != row_id:
        return False
    if "id" in value and value.get("id") != row_id:
        return False
    if expected_case_id is not None and case_id != expected_case_id:
        return False
    if expected_attempt_id is not None and value.get("attempt_id") != expected_attempt_id:
        return False
    if expected_attempt_index is not None and value.get("attempt_index") != expected_attempt_index:
        return False
    attempt_id = value.get("attempt_id")
    attempt_index = value.get("attempt_index")
    if not isinstance(attempt_id, str) or not attempt_id.strip():
        return False
    if (
        isinstance(attempt_index, bool)
        or not isinstance(attempt_index, int)
        or attempt_index < 0
    ):
        return False
    outcome = value.get("outcome")
    if not isinstance(outcome, str) or outcome not in OUTCOME_STATUSES:
        return False
    return True


def _case_result_expected_ledger_path(path: Path) -> Path:
    """Resolve the run-owned attempts path for a case-result artifact."""

    _require(isinstance(path, Path), "case-result path must be a Path", RunnerError)
    # Normal artifacts are run/<case>/case-result.json and therefore use the
    # run directory's ledger.  Keep the sibling form for focused standalone
    # artifacts, provided that it is the actual file beside the result.
    candidates = [
        path.parent.parent / CANONICAL_ATTEMPT_LEDGER_NAME,
        path.parent / CANONICAL_ATTEMPT_LEDGER_NAME,
    ]
    for candidate in candidates:
        if candidate.is_file() and not candidate.is_symlink():
            return candidate
    return candidates[0]


def _read_ledger_receipt_row(
    value: Any,
    *,
    expected_path: Path | None = None,
    expected_case_id: str | None = None,
    expected_attempt_id: str | None = None,
    expected_attempt_index: int | None = None,
    require_complete: bool = True,
) -> dict[str, Any] | None:
    """Read and parse the exact canonical JSONL row named by a receipt."""

    if not isinstance(value, Mapping):
        return None
    path_value = value.get("path")
    if not isinstance(path_value, str) or not path_value.strip():
        return None
    try:
        path = Path(path_value).expanduser()
        if path.name != CANONICAL_ATTEMPT_LEDGER_NAME:
            return None
        if expected_path is not None:
            if not isinstance(expected_path, Path):
                return None
            # Compare resolved lexical paths only after rejecting the
            # receipt's symlink path below.  A basename match is never enough
            # to identify a run's ledger.
            if path.resolve(strict=False) != expected_path.expanduser().resolve(strict=False):
                return None
        raw, _ = _read_stable_file(path, label="attempt ledger")
        offset = value.get("offset")
        length = value.get("bytes")
        if (
            isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 0
            or isinstance(length, bool)
            or not isinstance(length, int)
            or length <= 0
            or offset + length > len(raw)
        ):
            return None
        # A receipt identifies one complete JSONL record.  Checking only a
        # hash/parseable slice would allow a span beginning in the middle of a
        # line (or spanning two records) to be presented as an observed
        # append.  Require both line boundaries and exactly one physical
        # newline terminator in the recorded bytes.
        if (
            (offset > 0 and raw[offset - 1 : offset] != b"\n")
            or raw[offset : offset + length].count(b"\n") != 1
            or raw[offset + length - 1 : offset + length] != b"\n"
        ):
            return None
        payload = raw[offset : offset + length]
        if (
            bytes_digest(payload) != value.get("sha256")
            or not payload.endswith(b"\n")
        ):
            return None
        parsed = _strict_json_loads(payload.decode("utf-8", errors="strict"))
        # Every authoritative attempt row has an explicit nullable digest
        # field.  Requiring it on the parsed JSONL row prevents an artifact
        # reload from accepting an older/handwritten row whose shape only
        # resembles the ledger contract.
        if (
            not _ledger_row_shape_valid(
                parsed,
                expected_case_id=expected_case_id,
                expected_attempt_id=expected_attempt_id,
                expected_attempt_index=expected_attempt_index,
                require_complete=require_complete,
            )
            or _json_bytes(parsed) + b"\n" != payload
        ):
            return None
        return dict(parsed)
    except (OSError, UnicodeDecodeError, RuntimeError, TypeError, ValueError, RecursionError, RunnerError):
        return None


def _valid_ledger_receipt(
    value: Any,
    *,
    verify_file: bool = True,
    expected_path: Path | None = None,
    expected_case_id: str | None = None,
    expected_attempt_id: str | None = None,
    expected_attempt_index: int | None = None,
    require_complete: bool = True,
) -> bool:
    """Return whether a value proves one durable append-only ledger write.

    The default is the strong boundary: re-open the named JSONL file, seek to
    the recorded offset, and hash exactly the recorded byte span.  A producer
    supplied path/offset/hash tuple therefore cannot manufacture an observed
    attempt for a file that was never written or was later edited.  Callers
    that are only checking an append shape may explicitly pass
    ``verify_file=False``.
    """

    valid = bool(
        isinstance(value, Mapping)
        and isinstance(value.get("path"), str)
        and bool(value["path"].strip())
        and Path(value["path"]).name == CANONICAL_ATTEMPT_LEDGER_NAME
        and isinstance(value.get("offset"), int)
        and not isinstance(value["offset"], bool)
        and value["offset"] >= 0
        and isinstance(value.get("bytes"), int)
        and not isinstance(value["bytes"], bool)
        and value["bytes"] > 0
        and isinstance(value.get("sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", value["sha256"]) is not None
    )
    if not valid or not verify_file:
        return valid
    return _read_ledger_receipt_row(
        value,
        expected_path=expected_path,
        expected_case_id=expected_case_id,
        expected_attempt_id=expected_attempt_id,
        expected_attempt_index=expected_attempt_index,
        require_complete=require_complete,
    ) is not None


def _validate_existing_attempt_ledger(path: Path) -> list[dict[str, Any]]:
    """Validate an append target before adding a matrix compatibility attempt."""

    _require(isinstance(path, Path), "attempt ledger path must be a Path", RunnerError)
    if not path.exists():
        return []
    _require(path.name == CANONICAL_ATTEMPT_LEDGER_NAME, "attempt ledger path must be attempts.jsonl", RunnerError)
    try:
        raw, _ = _read_stable_file(path, label="existing attempt ledger")
        _require(raw.endswith(b"\n"), "existing attempt ledger must be newline terminated", RunnerError)
        lines = raw.splitlines()
    except (OSError, RunnerError) as error:
        raise RunnerError(f"cannot read existing attempt ledger {path}: {error}") from error
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_positions: set[tuple[str, int]] = set()
    for index, line in enumerate(lines):
        if not line.strip():
            raise RunnerError(f"existing attempt ledger has an empty line at {index}")
        try:
            value = _strict_json_loads(line.decode("utf-8", errors="strict"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
            raise RunnerError(f"existing attempt ledger line {index} is not strict JSON") from error
        _require(isinstance(value, Mapping), f"existing attempt ledger line {index} must be an object", RunnerError)
        row = dict(value)
        _require(
            _json_bytes(row) + b"\n" == line,
            f"existing attempt ledger line {index} is not canonical JSONL",
            RunnerError,
        )
        _require(
            "result_digest" in row and row.get("result_digest") is None,
            f"existing attempt ledger line {index} lacks its nullable result_digest",
            RunnerError,
        )
        _require(
            _ledger_row_shape_valid(row),
            f"existing attempt ledger line {index} lacks the complete ledger identity/field shape",
            RunnerError,
        )
        attempt_id = row.get("attempt_id")
        _require(
            isinstance(attempt_id, str) and bool(attempt_id.strip()),
            f"existing attempt ledger line {index} has an invalid attempt_id",
            RunnerError,
        )
        _require(
            attempt_id not in seen_ids,
            f"existing attempt ledger has duplicate attempt_id: {attempt_id}",
            RunnerError,
        )
        attempt_index = row.get("attempt_index")
        _require(
            isinstance(attempt_index, int) and not isinstance(attempt_index, bool) and attempt_index >= 0,
            f"existing attempt ledger line {index} has an invalid attempt_index",
            RunnerError,
        )
        row_id = row.get("row_id")
        _require(
            isinstance(row_id, str) and bool(row_id.strip()),
            f"existing attempt ledger line {index} has an invalid row_id",
            RunnerError,
        )
        _require(
            isinstance(row.get("outcome"), str) and row.get("outcome") in OUTCOME_STATUSES,
            f"existing attempt ledger line {index} has an invalid outcome",
            RunnerError,
        )
        position = (row_id, attempt_index)
        _require(
            position not in seen_positions,
            f"existing attempt ledger has duplicate row/index: {row_id}/{attempt_index}",
            RunnerError,
        )
        seen_ids.add(attempt_id)
        seen_positions.add(position)
        rows.append(row)
    return rows


def _require(condition: bool, message: str, error_type: type[Exception] = ContractError) -> None:
    if not condition:
        raise error_type(message)


def _slug(value: str) -> str:
    _require(isinstance(value, str), "slug value must be a string", RunnerError)
    result = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-")
    return result or "case"


def _git(path: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    # Git probes must never consume caller-controlled repository selectors or
    # config paths.  In particular, GIT_DIR/GIT_WORK_TREE can make a probe
    # inspect a different checkout than the path recorded in the evidence.
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.upper().startswith("GIT_")
    }
    environment["PATH"] = os.defpath
    return subprocess.run(
        ["git", "-C", str(path), *arguments],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )


def _paths_overlap(left: Path, right: Path) -> bool:
    """Return whether two resolved paths are equal or one contains the other."""

    try:
        left_resolved = left.resolve()
        right_resolved = right.resolve()
        return (
            left_resolved == right_resolved
            or left_resolved.is_relative_to(right_resolved)
            or right_resolved.is_relative_to(left_resolved)
        )
    except (OSError, RuntimeError, TypeError, ValueError):
        return True


def verify_reference_checkout(
    checkout: str | os.PathLike[str],
    expected_revision: str | None = None,
) -> dict[str, Any]:
    """Verify the existing checkout against the immutable 570 reference.

    ``expected_revision`` is retained as a compatibility guard for callers that
    used the old parameter, but it cannot change the pinned reference.  Any
    supplied value must equal :data:`REFERENCE_REVISION`.
    """

    expected = REFERENCE_REVISION.lower()
    if expected_revision is not None and str(expected_revision).strip().lower() != expected:
        raise ReferenceError(
            "reference revision override is not permitted; "
            f"the runner is fixed to {REFERENCE_REVISION}"
        )

    try:
        root = Path(checkout).expanduser()
    except (TypeError, ValueError) as error:
        raise ReferenceError(f"reference checkout path is invalid: {checkout!r}") from error
    if not root.is_absolute():
        root = root.resolve()
    _require(root.is_dir(), f"reference checkout is absent: {root}", ReferenceError)
    revision = _git(root, "rev-parse", "--verify", "HEAD")
    if revision.returncode != 0:
        raise ReferenceError(f"reference checkout has no readable HEAD: {root}: {revision.stderr.strip()}")
    actual_revision = revision.stdout.strip().lower()
    if actual_revision != expected:
        raise ReferenceError(
            f"reference checkout revision mismatch: expected {REFERENCE_REVISION}, got {actual_revision}"
        )
    status = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status.returncode != 0:
        raise ReferenceError(f"reference checkout status failed: {status.stderr.strip()}")
    dirty = status.stdout
    if dirty:
        raise ReferenceError(f"reference checkout is modified; refusing original execution: {dirty.strip()}")
    toplevel = _git(root, "rev-parse", "--show-toplevel")
    if toplevel.returncode != 0:
        raise ReferenceError(f"reference checkout top-level probe failed: {root}: {toplevel.stderr.strip()}")
    try:
        git_root = Path(toplevel.stdout.strip()).expanduser().resolve()
        requested_root = root.resolve()
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise ReferenceError(f"reference checkout top-level path is invalid: {error}") from error
    if git_root != requested_root:
        raise ReferenceError(
            "reference checkout path must be the exact Git top-level; "
            f"requested {requested_root}, top-level is {git_root}"
        )
    return {
        "path": str(root),
        "revision": actual_revision,
        "clean": True,
        "git_readable": True,
        "status": "",
        "git_root": str(git_root),
    }


def verify_source_root(
    source_root: str | os.PathLike[str],
    *,
    side: str,
    reference_root: Path | None = None,
) -> dict[str, Any]:
    """Resolve a source root and prove it is distinct from the reference.

    The reference source is verified by ``verify_reference_checkout``.  The
    candidate source is intentionally only inspected here: a moving checkout
    may have a later revision, but it must still be recorded as its own source
    root rather than inferred from a binary path or a historical pin.
    """

    try:
        root = Path(source_root).expanduser()
    except (TypeError, ValueError) as error:
        raise RunnerError(f"{side} source root is invalid: {source_root!r}") from error
    if not root.is_absolute():
        root = root.resolve()
    _require(root.is_dir(), f"{side} source root is absent: {root}", RunnerError)
    resolved = root.resolve()
    if reference_root is not None:
        _require(
            not _paths_overlap(resolved, reference_root.resolve()),
            "original and product source roots overlap; refusing a shared source comparison",
            RunnerError,
        )
    revision = _git(resolved, "rev-parse", "--verify", "HEAD")
    observed_revision = revision.stdout.strip().lower() if revision.returncode == 0 else None
    # A nested path can resolve to a different checkout than the source root
    # recorded in an attestation.  Require the exact Git top-level for every
    # source side so source/binary correspondence cannot be relabelled by a
    # child directory or an overlapping worktree.
    toplevel = _git(resolved, "rev-parse", "--show-toplevel")
    if toplevel.returncode != 0:
        raise RunnerError(f"{side} source root is not a readable Git checkout: {resolved}")
    try:
        git_root = Path(toplevel.stdout.strip()).expanduser().resolve()
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise RunnerError(f"{side} source Git top-level is invalid: {error}") from error
    if git_root != resolved:
        raise RunnerError(
            f"{side} source root must be the exact Git top-level; "
            f"requested {resolved}, top-level is {git_root}"
        )
    return {
        "side": side,
        "path": str(resolved),
        "revision": observed_revision,
        "git_readable": revision.returncode == 0,
        "status": "clean-check-not-required",
        "git_root": str(git_root),
    }


def resolve_product_revision(source_root: Path, supplied: str | None) -> str:
    """Return a full candidate revision and reject unverifiable identities."""

    if not isinstance(source_root, Path):
        try:
            source_root = Path(source_root)
        except (TypeError, ValueError) as error:
            raise RunnerError("candidate source root is invalid") from error
    observed = _git(source_root, "rev-parse", "--verify", "HEAD")
    observed_value = observed.stdout.strip().lower() if observed.returncode == 0 else ""
    if not re.fullmatch(r"[0-9a-f]{40}", observed_value):
        raise RunnerError("candidate source revision is unknown; source HEAD is not a full Git SHA")
    if supplied is not None:
        value = str(supplied).strip().lower()
        if not re.fullmatch(r"[0-9a-f]{40}", value):
            raise RunnerError("candidate source revision must be a full 40-character Git SHA")
        if value != observed_value:
            raise RunnerError(
                "candidate source revision does not match the selected source HEAD"
            )
        return value
    return observed_value


def verify_binary(binary: str | os.PathLike[str], side: str) -> dict[str, Any]:
    """Validate and fingerprint a separately built executable."""

    try:
        lexical_path = Path(binary).expanduser()
    except (TypeError, ValueError) as error:
        raise BinaryError(f"{side} binary path is invalid: {binary!r}") from error
    try:
        # Inspect the caller-supplied spelling before resolving it.  A
        # symlink can otherwise be replaced between validation and spawn,
        # turning a mutable alias into an attested executable.
        if not lexical_path.is_absolute():
            lexical_path = Path.cwd() / lexical_path
        lexical_info = lexical_path.lstat()
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise BinaryError(f"{side} binary cannot be inspected: {lexical_path}: {error}") from error
    if stat.S_ISLNK(lexical_info.st_mode):
        raise BinaryError(f"{side} binary path must not be a symlink: {lexical_path}")
    if not stat.S_ISREG(lexical_info.st_mode):
        raise BinaryError(f"{side} binary is not a regular file: {lexical_path}")
    try:
        path = lexical_path.resolve(strict=True)
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise BinaryError(f"{side} binary cannot be resolved: {lexical_path}: {error}") from error
    if not os.access(path, os.X_OK):
        raise BinaryError(f"{side} binary is not executable: {path}")
    hasher = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                hasher.update(chunk)
                size += len(chunk)
    except OSError as error:
        raise BinaryError(f"{side} binary cannot be read: {path}: {error}") from error
    return {"side": side, "path": str(path), "bytes": size, "sha256": hasher.hexdigest()}


def verify_distinct_binaries(
    original_binary: str | os.PathLike[str],
    product_binary: str | os.PathLike[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Fingerprint both runtimes and refuse a shared executable/backend."""

    original = verify_binary(original_binary, "original")
    product = verify_binary(product_binary, "product")
    _require(
        original["sha256"] != product["sha256"],
        "original and product binaries have the same SHA-256; refusing a shared-backend comparison",
        BinaryError,
    )
    _require(
        not _paths_overlap(Path(original["path"]).resolve().parent, Path(product["path"]).resolve().parent),
        "original and product binary roots overlap; refusing a non-isolated comparison",
        BinaryError,
    )
    return original, product


def _copy_immutable_binary(
    binary: Mapping[str, Any],
    *,
    root: Path,
    side: str,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    """Copy one executable into the run-owned execution root and recheck it.

    The build artifact remains the attested input, while the child receives a
    private copy.  Hashing the source before and after the copy and hashing the
    copy closes the common path replacement/TOCTOU window without relying on a
    platform-specific executable file descriptor API.
    """

    _require(isinstance(binary, Mapping), f"{side} binary evidence must be an object", BinaryError)
    _require(isinstance(root, Path), "immutable binary root must be a Path", BinaryError)
    if expected_sha256 is not None:
        _require(
            isinstance(expected_sha256, str)
            and re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is not None,
            f"{side} supplied attestation hash is malformed",
            BinaryError,
        )
    source_value = binary.get("path")
    _require(isinstance(source_value, str) and bool(source_value.strip()), f"{side} binary evidence lacks a path", BinaryError)
    try:
        lexical_root = root.expanduser()
        if not lexical_root.is_absolute():
            lexical_root = lexical_root.resolve()
        root_created = False
        if lexical_root.exists():
            root_info = lexical_root.lstat()
            _require(
                not stat.S_ISLNK(root_info.st_mode) and stat.S_ISDIR(root_info.st_mode),
                "immutable binary root must be a real directory",
                BinaryError,
            )
        else:
            lexical_root.mkdir(parents=True, exist_ok=True, mode=0o700)
            # ``mkdir(mode=...)`` is still subject to the process umask and a
            # raced/pre-created path must never reach the first copy with
            # broad permissions.  Narrow the freshly-created root before any
            # executable bytes are written below it.
            lexical_root.chmod(0o700)
            root_created = True
            root_info = lexical_root.lstat()
        _require(
            not hasattr(os, "getuid") or root_info.st_uid == os.getuid(),
            "immutable binary root is not owned by the runner user",
            BinaryError,
        )
        root_mode = stat.S_IMODE(root_info.st_mode)
        _require(
            not (root_mode & 0o077),
            "immutable binary root must be owner-private before copying",
            BinaryError,
        )
        _require(
            not (root_mode & 0o022),
            "immutable binary root must not be writable by group or other users",
            BinaryError,
        )
        if not root_created:
            _require(
                bool(root_mode & 0o200),
                "pre-existing immutable binary root is not owner-writable; refusing to alter it",
                BinaryError,
            )
        root_path = lexical_root.resolve()
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise BinaryError(f"{side} immutable binary root cannot be prepared: {error}") from error
    try:
        lexical_source = Path(source_value).expanduser()
        if not lexical_source.is_absolute():
            lexical_source = Path.cwd() / lexical_source
        # Inspect the caller-supplied path before resolving it.  Resolving a
        # symlink first would make a mutable alias look like the attested
        # executable and reopen the binary TOCTOU window.
        source_info = lexical_source.lstat()
        _require(not stat.S_ISLNK(source_info.st_mode), f"{side} binary path must not be a symlink", BinaryError)
        source = lexical_source.resolve(strict=True)
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise BinaryError(f"{side} binary cannot be inspected for immutable copying: {error}") from error
    before = verify_binary(source, side)
    if expected_sha256 is not None:
        _require(
            before.get("sha256") == expected_sha256,
            f"{side} source binary does not match its supplied attestation",
            BinaryError,
        )
    destination = root_path / side / source.name
    try:
        destination.parent.mkdir(parents=True, exist_ok=False)
        with source.open("rb") as source_stream:
            payload = source_stream.read()
        with destination.open("xb") as destination_stream:
            destination_stream.write(payload)
            destination_stream.flush()
            os.fsync(destination_stream.fileno())
        # Executables and their containing directories are private to this
        # runner UID.  ``0555`` is tempting because it looks immutable, but
        # it grants read/traverse access to every other user.  The comparison
        # root is a private evidence boundary, so use one consistent
        # owner-only executable/read mode throughout the copy tree.
        destination.chmod(IMMUTABLE_COPY_FILE_MODE)
        destination.parent.chmod(IMMUTABLE_COPY_MODE)
        # The final side copy seals the common parent.  The runner never
        # executes children from this root before both side copies exist;
        # sealing it here prevents a child from replacing or adding an
        # executable during the comparison window.
        if all((root_path / candidate_side).is_dir() for candidate_side in SIDE_NAMES):
            root_path.chmod(IMMUTABLE_COPY_MODE)
        destination_info = destination.lstat()
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise BinaryError(f"{side} binary immutable copy failed: {error}") from error
    _require(not stat.S_ISLNK(destination_info.st_mode), f"{side} immutable binary copy is a symlink", BinaryError)
    after = verify_binary(source, side)
    copied = verify_binary(destination, side)
    try:
        # Re-inspect the exact lexical spelling supplied by the build owner.
        # A symlink replaced after the copy can still resolve to the same inode
        # and would otherwise evade the resolved-path hash check.
        final_source_info = lexical_source.lstat()
    except OSError as error:
        raise BinaryError(f"{side} binary disappeared after immutable copy: {error}") from error
    _require(
        not stat.S_ISLNK(final_source_info.st_mode)
        and stat.S_ISREG(final_source_info.st_mode),
        f"{side} binary lexical path was replaced by a non-regular file during immutable copying",
        BinaryError,
    )
    _require(
        before["sha256"] == after["sha256"] == copied["sha256"]
        and before["bytes"] == after["bytes"] == copied["bytes"],
        f"{side} binary changed while creating immutable execution copy",
        BinaryError,
    )
    if expected_sha256 is not None:
        _require(
            after["sha256"] == copied["sha256"] == expected_sha256,
            f"{side} immutable copy does not match its supplied attestation",
            BinaryError,
        )
    _require(
        (source_info.st_dev, source_info.st_ino)
        == (final_source_info.st_dev, final_source_info.st_ino),
        f"{side} binary path was replaced while creating immutable execution copy",
        BinaryError,
    )
    _require(
        stat.S_IMODE(destination_info.st_mode) == IMMUTABLE_COPY_FILE_MODE
        and stat.S_IMODE(destination.parent.lstat().st_mode) == IMMUTABLE_COPY_MODE,
        f"{side} immutable copy permissions are not owner-only",
        BinaryError,
    )
    return {
        **copied,
        "input_path": before["path"],
        "input_sha256": before["sha256"],
        "source_pre_sha256": before["sha256"],
        "source_post_sha256": after["sha256"],
        "immutable": True,
        "mode": stat.S_IMODE(destination_info.st_mode),
    }


def verify_build_attestation(
    attestation: str | os.PathLike[str],
    *,
    binary: Mapping[str, Any],
    source_root: Mapping[str, Any],
    expected_revision: str,
    side: str = "product",
    expected_features: str | None = None,
) -> dict[str, Any]:
    """Verify source/binary correspondence supplied by the build owner.

    A binary hash alone cannot prove which checkout produced it.  The build
    owner therefore writes a small JSON attestation beside the artifact.  The
    runner binds its source root, full revision and executable hash before any
    comparison process is started.
    """

    _require(isinstance(binary, Mapping), "selected binary evidence must be an object", BinaryError)
    _require(isinstance(source_root, Mapping), "selected source evidence must be an object", BinaryError)
    binary_path_value = binary.get("path")
    binary_hash_value = binary.get("sha256")
    source_path_value = source_root.get("path")
    _require(isinstance(binary_path_value, str) and bool(binary_path_value), "selected binary evidence lacks a path", BinaryError)
    _require(
        isinstance(binary_hash_value, str)
        and re.fullmatch(r"[0-9a-f]{64}", binary_hash_value) is not None,
        "selected binary evidence lacks a valid SHA-256",
        BinaryError,
    )
    _require(isinstance(source_path_value, str) and bool(source_path_value), "selected source evidence lacks a path", BinaryError)
    try:
        selected_source_path = Path(source_path_value).expanduser().resolve()
        selected_git_root = Path(str(source_root.get("git_root", ""))).expanduser().resolve()
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise BinaryError(f"selected source evidence path is invalid: {error}") from error
    _require(
        selected_git_root == selected_source_path,
        "selected source evidence must identify the exact Git top-level",
        BinaryError,
    )
    if not re.fullmatch(r"[0-9a-f]{40}", str(expected_revision).strip().lower()):
        raise BinaryError("expected source revision must be a full 40-character Git SHA")
    expected_revision_value = str(expected_revision).strip().lower()
    observed_source_revision = source_root.get("revision")
    _require(
        isinstance(observed_source_revision, str)
        and re.fullmatch(r"[0-9a-f]{40}", observed_source_revision.strip().lower()) is not None
        and observed_source_revision.strip().lower() == expected_revision_value,
        "selected source evidence is not bound to the expected Git revision",
        BinaryError,
    )
    _require(
        source_root.get("git_readable") is True,
        "selected source evidence is not Git-readable",
        BinaryError,
    )
    if side == "original":
        _require(
            expected_revision_value == REFERENCE_REVISION,
            "original build attestation must use the pinned 570 revision",
            BinaryError,
        )
    try:
        path = Path(attestation).expanduser()
    except (TypeError, ValueError) as error:
        raise BinaryError(f"{side} build attestation path is invalid: {attestation!r}") from error
    if not path.is_absolute():
        path = path.resolve()
    try:
        # Read through one descriptor and re-lstat the lexical path.  A
        # symlink or a replacement between ``is_file`` and ``read_bytes``
        # must never be accepted as a build attestation.
        attestation_bytes, _ = _read_stable_file(
            path,
            label=f"{side} build attestation",
        )
        value = _strict_json_loads(attestation_bytes.decode("utf-8", errors="strict"))
    except (RunnerError, UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as error:
        raise BinaryError(f"cannot read build attestation {path}: {error}") from error
    _require(isinstance(value, Mapping), "build attestation must be an object", BinaryError)
    _require(value.get("format") == BUILD_ATTESTATION_FORMAT, "build attestation format is invalid", BinaryError)
    _require(value.get("side") == side, f"build attestation side must be {side!r}", BinaryError)
    features = value.get("features")
    _require(isinstance(features, str) and bool(features.strip()), "build attestation features are missing", BinaryError)
    if expected_features is not None:
        _require(
            isinstance(expected_features, str)
            and features == expected_features,
            "build attestation features do not match the selected contract",
            BinaryError,
        )
    revision = str(value.get("source_revision", "")).strip().lower()
    _require(re.fullmatch(r"[0-9a-f]{40}", revision) is not None, "build attestation source revision is not a full Git SHA", BinaryError)
    _require(revision == expected_revision_value, "build attestation source revision does not match the selected source", BinaryError)
    attested_source = Path(str(value.get("source_root", ""))).expanduser()
    _require(attested_source.is_absolute(), "build attestation source_root must be absolute", BinaryError)
    _require(attested_source.resolve() == selected_source_path, "build attestation source_root does not match the selected source", BinaryError)
    attested_binary = Path(str(value.get("binary_path", ""))).expanduser()
    _require(attested_binary.is_absolute(), "build attestation binary_path must be absolute", BinaryError)
    _require(attested_binary.resolve() == Path(binary_path_value).resolve(), "build attestation binary_path does not match the selected binary", BinaryError)
    _require(
        isinstance(value.get("binary_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", value["binary_sha256"]) is not None
        and value.get("binary_sha256") == binary_hash_value,
        "build attestation binary hash does not match the selected binary",
        BinaryError,
    )
    return {
        "path": str(path),
        "format": BUILD_ATTESTATION_FORMAT,
        "side": side,
        "source_root": str(attested_source.resolve()),
        "source_revision": revision,
        "binary_path": str(attested_binary.resolve()),
        "binary_sha256": binary_hash_value,
        "features": features,
        "attestation_sha256": bytes_digest(attestation_bytes),
    }


def _load_json(path: Path) -> Any:
    _require(isinstance(path, Path), "JSON artifact path must be a Path", RunnerError)
    try:
        raw, _ = _read_stable_file(path, label=f"JSON artifact {path}")
        value = _strict_json_loads(raw.decode("utf-8", errors="strict"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError, TypeError, RunnerError) as error:
        raise ContractError(f"cannot read JSON artifact {path}: {error}") from error
    if path.name == "case-result.json":
        if not isinstance(value, Mapping) or "result_digest" not in value:
            raise ContractError(f"case-result artifact {path} lacks its final result_digest")
        case_id = value.get("case_id", value.get("id"))
        if not isinstance(case_id, str) or not case_id.strip():
            raise ContractError(f"case-result artifact {path} lacks a non-empty case_id")
        if "id" in value and value.get("id") != case_id:
            raise ContractError(f"case-result artifact {path} has conflicting id and case_id")
        verify_case_result_digest(value, raw_bytes=raw)
        receipt = value.get("attempt_ledger_receipt")
        if receipt is not None:
            ledger_path = _case_result_expected_ledger_path(path)
            if not _valid_ledger_receipt(
                receipt,
                verify_file=True,
                expected_path=ledger_path,
                expected_case_id=case_id,
            ):
                raise ContractError(f"case-result artifact {path} has an invalid attempt ledger receipt")
            ledger_rows = value.get("attempt_ledger")
            if not isinstance(ledger_rows, list) or len(ledger_rows) != 1:
                raise ContractError(
                    f"case-result artifact {path} must carry exactly one nested attempt ledger row with its receipt"
                )
            authoritative = _read_ledger_receipt_row(
                receipt,
                expected_path=ledger_path,
                expected_case_id=case_id,
            )
            matches = sum(
                isinstance(row, Mapping)
                and _json_semantic_equal(dict(row), dict(authoritative))
                for row in ledger_rows
            ) if isinstance(authoritative, Mapping) else 0
            if matches != 1:
                raise ContractError(
                    f"case-result artifact {path} does not contain exactly one authoritative ledger row"
                )
            nested = ledger_rows[0]
            if not _ledger_row_shape_valid(
                nested,
                expected_case_id=case_id,
            ):
                raise ContractError(
                    f"case-result artifact {path} nested ledger row has incomplete or conflicting identity fields"
                )
        elif isinstance(value.get("attempt_ledger"), list) and value.get("attempt_ledger"):
            raise ContractError(
                f"case-result artifact {path} has nested ledger rows without an authoritative receipt"
            )
    return value


def _load_ncm_worker_attestation(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Load the runner-owned NCM worker trust input through one stable file.

    A worker manifest copied from a child response is not an attestation.  The
    caller must provide a regular, non-symlink JSON file; the runner retains
    its path and digest and re-reads that same file when worker identity is
    evaluated.  Private ``_runner_input_*`` fields never enter child argv,
    environment, or the public worker manifest emitted in a result.
    """

    try:
        manifest_path = Path(path).expanduser()
    except (TypeError, ValueError) as error:
        raise RunnerError(f"NCM worker attestation path is invalid: {path!r}") from error
    if not manifest_path.is_absolute():
        manifest_path = (Path.cwd() / manifest_path)
    try:
        raw, _ = _read_stable_file(manifest_path, label="NCM worker attestation")
        parsed = _strict_json_loads(raw.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError, RunnerError) as error:
        raise RunnerError(f"NCM worker attestation is not a stable strict JSON object: {error}") from error
    _require(isinstance(parsed, Mapping), "NCM worker attestation must be an object", RunnerError)
    _require(
        set(parsed) == set(NCM_WORKER_ATTESTATION_FIELDS),
        "NCM worker attestation must contain exactly its pinned identity fields",
        RunnerError,
    )
    manifest = {field: copy.deepcopy(parsed[field]) for field in NCM_WORKER_ATTESTATION_FIELDS}
    _require(
        all(isinstance(manifest.get(field), str) and bool(manifest[field].strip()) for field in NCM_WORKER_ATTESTATION_FIELDS),
        "NCM worker attestation contains an empty or non-string identity field",
        RunnerError,
    )
    _require(
        manifest["format"] == NCM_WORKER_ATTESTATION_FORMAT
        and manifest["kind"] == "ncm_encoder_worker"
        and manifest["worker_id"] == NCM_WORKER_ID,
        "NCM worker attestation format/kind/worker_id is not recognized",
        RunnerError,
    )
    for field in (
        "binary_sha256",
        "implementation_sha256",
        "model_artifact_sha256",
        "tokenizer_sha256",
        "vector_fixture_digest",
        "pin_sha256",
    ):
        _require(
            re.fullmatch(r"[0-9a-f]{64}", manifest[field]) is not None,
            f"NCM worker attestation {field} is not a lowercase SHA-256",
            RunnerError,
        )
    _require(
        re.fullmatch(r"[0-9a-f]{40}", manifest["source_revision"]) is not None,
        "NCM worker attestation source_revision is not a full lowercase Git SHA",
        RunnerError,
    )
    for field in ("binary_path", "source_root"):
        try:
            candidate = Path(manifest[field]).expanduser()
        except (TypeError, ValueError) as error:
            raise RunnerError(f"NCM worker attestation {field} is not a valid path") from error
        _require(candidate.is_absolute(), f"NCM worker attestation {field} must be absolute", RunnerError)
    expected_pin = json_digest({field: manifest[field] for field in NCM_WORKER_PIN_FIELDS})
    _require(manifest["pin_sha256"] == expected_pin, "NCM worker attestation pin_sha256 is not canonical", RunnerError)
    try:
        manifest_path = manifest_path.resolve(strict=True)
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise RunnerError(f"NCM worker attestation path cannot be resolved: {error}") from error
    manifest.update(
        {
            "_runner_input": True,
            "_runner_input_path": str(manifest_path),
            "_runner_input_sha256": bytes_digest(raw),
            "_runner_input_bytes": len(raw),
        }
    )
    return manifest


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> Any:
    raise ValueError(f"non-finite JSON constant is not accepted: {value}")


def _reject_overflow_json_float(value: str) -> float:
    """Parse JSON numbers without allowing an overflow to become ``inf``."""

    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError(f"invalid JSON number: {value!r}") from error
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite JSON number is not accepted: {value}")
    return parsed


def _strict_json_loads(value: str) -> Any:
    if not isinstance(value, (str, bytes, bytearray)):
        raise ValueError("JSON input must be text or bytes")
    try:
        return json.loads(
            value,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_json_constant,
            parse_float=_reject_overflow_json_float,
        )
    except RecursionError as error:
        # Keep every public JSON boundary on the typed parser path.  The
        # standard decoder otherwise leaks RecursionError for hostile deep
        # input before a case can record unknown evidence.
        raise ValueError("JSON input is too deeply nested") from error


def _schema_ref(root: Mapping[str, Any], reference: Any) -> Mapping[str, Any]:
    """Resolve the local ``$defs`` references used by the case schema."""

    _require(isinstance(reference, str) and reference.startswith("#/"), "case schema has an unsupported $ref")
    value: Any = root
    for token in reference[2:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        _require(isinstance(value, Mapping) and token in value, f"case schema $ref is unresolved: {reference}")
        value = value[token]
    _require(isinstance(value, Mapping), f"case schema $ref does not resolve to an object: {reference}")
    return value


def _schema_type_matches(instance: Any, expected: str) -> bool:
    if expected == "object":
        return isinstance(instance, Mapping)
    if expected == "array":
        return isinstance(instance, list)
    if expected == "string":
        return isinstance(instance, str)
    if expected == "boolean":
        return isinstance(instance, bool)
    if expected == "null":
        return instance is None
    if expected == "integer":
        return isinstance(instance, int) and not isinstance(instance, bool)
    if expected == "number":
        return (
            isinstance(instance, (int, float))
            and not isinstance(instance, bool)
            and math.isfinite(float(instance))
        )
    return False


def _validate_case_schema(
    instance: Any,
    schema: Mapping[str, Any],
    *,
    root: Mapping[str, Any] | None = None,
    path: str = "$",
    depth: int = 0,
) -> None:
    """Validate the embedded Draft 2020-12 case subset before runtime checks.

    The release contract is validated independently with Ajv in the focused
    test suite.  The runner keeps its own small dependency-free implementation
    for the keywords used by that schema so a case cannot bypass the embedded
    shape merely by calling the Python library API instead of the CLI.
    """

    _require(isinstance(schema, Mapping), f"case schema at {path} must be an object")
    if depth > 64:
        raise ContractError("case schema is too deeply nested")
    root = schema if root is None else root
    if "$ref" in schema:
        _validate_case_schema(
            instance,
            _schema_ref(root, schema["$ref"]),
            root=root,
            path=path,
            depth=depth + 1,
        )
        return
    if "allOf" in schema:
        branches = schema["allOf"]
        _require(isinstance(branches, list), f"case schema allOf at {path} must be a list")
        for branch in branches:
            _validate_case_schema(instance, branch, root=root, path=path, depth=depth + 1)
    if "anyOf" in schema:
        branches = schema["anyOf"]
        _require(isinstance(branches, list) and branches, f"case schema anyOf at {path} must be a non-empty list")
        valid = False
        for branch in branches:
            try:
                _validate_case_schema(instance, branch, root=root, path=path, depth=depth + 1)
            except ContractError:
                continue
            valid = True
            break
        _require(valid, f"case value at {path} does not match any schema branch")
    if "oneOf" in schema:
        branches = schema["oneOf"]
        _require(isinstance(branches, list) and branches, f"case schema oneOf at {path} must be a non-empty list")
        matches = 0
        for branch in branches:
            try:
                _validate_case_schema(instance, branch, root=root, path=path, depth=depth + 1)
            except ContractError:
                continue
            matches += 1
        _require(matches == 1, f"case value at {path} must match exactly one schema branch")
    if "not" in schema:
        try:
            _validate_case_schema(instance, schema["not"], root=root, path=path, depth=depth + 1)
        except ContractError:
            pass
        else:
            raise ContractError(f"case value at {path} matches a forbidden schema")
    if "const" in schema:
        _require(_json_semantic_equal(instance, schema["const"]), f"case value at {path} does not match the schema constant")
    if "enum" in schema:
        choices = schema["enum"]
        _require(
            isinstance(choices, list) and any(_json_semantic_equal(instance, choice) for choice in choices),
            f"case value at {path} is outside the schema enum",
        )
    expected_type = schema.get("type")
    if expected_type is not None:
        expected_types = expected_type if isinstance(expected_type, list) else [expected_type]
        _require(
            isinstance(expected_types, list)
            and all(isinstance(item, str) for item in expected_types)
            and any(_schema_type_matches(instance, item) for item in expected_types),
            f"case value at {path} has the wrong schema type",
        )
    if isinstance(instance, str):
        if "minLength" in schema:
            _require(isinstance(schema["minLength"], int) and len(instance) >= schema["minLength"], f"case string at {path} is too short")
        if "maxLength" in schema:
            _require(isinstance(schema["maxLength"], int) and len(instance) <= schema["maxLength"], f"case string at {path} is too long")
        if "pattern" in schema:
            _require(isinstance(schema["pattern"], str) and re.search(schema["pattern"], instance) is not None, f"case string at {path} does not match its schema pattern")
    if isinstance(instance, list):
        if "minItems" in schema:
            _require(isinstance(schema["minItems"], int) and len(instance) >= schema["minItems"], f"case array at {path} has too few items")
        if "maxItems" in schema:
            _require(isinstance(schema["maxItems"], int) and len(instance) <= schema["maxItems"], f"case array at {path} has too many items")
        if "items" in schema:
            for index, item in enumerate(instance):
                _validate_case_schema(item, schema["items"], root=root, path=f"{path}/{index}", depth=depth + 1)
    if isinstance(instance, Mapping):
        required = schema.get("required", [])
        _require(isinstance(required, list), f"case schema required at {path} must be a list")
        for key in required:
            _require(isinstance(key, str) and key in instance, f"case object at {path} lacks required property {key!r}")
        if "minProperties" in schema:
            _require(isinstance(schema["minProperties"], int) and len(instance) >= schema["minProperties"], f"case object at {path} has too few properties")
        properties = schema.get("properties", {})
        _require(isinstance(properties, Mapping), f"case schema properties at {path} must be an object")
        for key, property_schema in properties.items():
            if key in instance:
                _validate_case_schema(instance[key], property_schema, root=root, path=f"{path}.{key}", depth=depth + 1)
        additional = schema.get("additionalProperties", True)
        for key, value in instance.items():
            if key in properties:
                continue
            if additional is False:
                raise ContractError(f"case object at {path} has unexpected property {key!r}")
            if isinstance(additional, Mapping):
                _validate_case_schema(value, additional, root=root, path=f"{path}.{key}", depth=depth + 1)


def load_contract(path: str | os.PathLike[str] = DEFAULT_CONTRACT) -> dict[str, Any]:
    try:
        contract_path = Path(path)
    except (TypeError, ValueError) as error:
        raise ContractError(f"contract path is invalid: {path!r}") from error
    value = _load_json(contract_path)
    validate_contract(value)
    return value


def load_cases(path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    try:
        case_path = Path(path)
    except (TypeError, ValueError) as error:
        raise ContractError(f"cases path is invalid: {path!r}") from error
    value = _load_json(case_path)
    if isinstance(value, dict) and "cases" in value:
        value = value["cases"]
    elif isinstance(value, dict):
        value = [value]
    _require(isinstance(value, list), "cases artifact must be an object with cases or an array")
    try:
        cases = [copy.deepcopy(case) for case in value]
    except (RecursionError, TypeError, ValueError) as error:
        raise ContractError(f"cases artifact contains malformed or too deeply nested data: {error}") from error
    seen_ids: set[str] = set()
    for index, case in enumerate(cases):
        _require(isinstance(case, Mapping), f"case {index} must be an object")
        case_id = case.get("id", case.get("case_id"))
        _require(isinstance(case_id, str) and bool(case_id.strip()), f"case {index} has no stable id")
        _require(
            "id" not in case or "case_id" not in case or case.get("id") == case.get("case_id"),
            f"case {index} id and case_id aliases disagree",
        )
        _require(case_id not in seen_ids, f"duplicate case id: {case_id}")
        seen_ids.add(case_id)
    return cases


def load_readiness_matrix(
    path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Load the immutable acceptance matrix used by readiness mode."""

    try:
        matrix_path = Path(path if path is not None else DEFAULT_READINESS_MATRIX).expanduser()
    except (TypeError, ValueError) as error:
        raise RunnerError(f"frozen readiness matrix path is invalid: {path!r}") from error
    if not matrix_path.is_absolute():
        matrix_path = matrix_path.resolve()
    _require(matrix_path.is_file(), f"frozen readiness matrix is absent: {matrix_path}", RunnerError)
    try:
        payload = matrix_path.read_bytes()
    except OSError as error:
        raise RunnerError(f"cannot read frozen readiness matrix {matrix_path}: {error}") from error
    _require(
        bytes_digest(payload) == FROZEN_READINESS_MATRIX_SHA256,
        f"frozen readiness matrix digest mismatch: {matrix_path}",
        RunnerError,
    )
    try:
        value = _strict_json_loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise RunnerError(f"frozen readiness matrix is not valid JSON: {matrix_path}: {error}") from error
    _require(isinstance(value, Mapping), "frozen readiness matrix must be an object", RunnerError)
    try:
        matrix = copy.deepcopy(dict(value))
    except (RecursionError, TypeError, ValueError) as error:
        raise RunnerError(f"frozen readiness matrix contains malformed or too deeply nested data: {error}") from error
    matrix["_path"] = str(matrix_path)
    matrix["_sha256"] = bytes_digest(payload)
    validate_frozen_readiness_matrix(matrix)
    return matrix


def _validated_readiness_matrix_input(
    matrix: Mapping[str, Any] | None,
    *,
    path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Revalidate an in-memory matrix against the frozen bytes on disk.

    Callers may pass a parsed matrix for convenience, but that object is not
    an authority.  The bytes at its recorded path must still be the accepted
    matrix digest and the parsed value must be identical to those bytes.  This
    prevents a library caller from mutating a row or injecting a test matrix
    after ``load_readiness_matrix`` and accidentally obtaining readiness
    evidence for a different campaign.
    """

    if matrix is None:
        return load_readiness_matrix(path)
    _require(isinstance(matrix, Mapping), "readiness matrix must be an object", RunnerError)
    raw_path = matrix.get("_path")
    if raw_path is None:
        raw_path = path
    _require(raw_path is not None, "readiness matrix must identify its frozen source path", RunnerError)
    frozen = load_readiness_matrix(raw_path)
    try:
        supplied = {key: copy.deepcopy(value) for key, value in matrix.items() if not str(key).startswith("_")}
        source = {key: copy.deepcopy(value) for key, value in frozen.items() if not str(key).startswith("_")}
    except (RecursionError, TypeError, ValueError) as error:
        raise RunnerError(f"readiness matrix contains malformed or too deeply nested data: {error}") from error
    _require(
        _json_semantic_equal(supplied, source),
        "in-memory readiness matrix does not match its frozen source bytes",
        RunnerError,
    )
    _require(
        matrix.get("_sha256", frozen.get("_sha256")) == FROZEN_READINESS_MATRIX_SHA256,
        "in-memory readiness matrix has an invalid frozen digest",
        RunnerError,
    )
    return frozen


def _readiness_row_identity_digest(rows: Sequence[Mapping[str, Any]]) -> str:
    return json_digest(sorted(str(row["id"]) for row in rows))


def _readiness_oracle_runs_digest(rows: Sequence[Mapping[str, Any]]) -> str:
    return json_digest(
        sorted(
            (
                str(row["id"]),
                str(row["oracle_kind"]),
                int(row["runs"]["planned"]),
            )
            for row in rows
        )
    )


def _readiness_row_bindings_digest(rows: Sequence[Mapping[str, Any]]) -> str:
    """Digest the semantic binding fields of every frozen readiness row."""

    return json_digest(
        [
            {
                "id": row["id"],
                "track": row["track"],
                "operation": row["operation"],
                "oracle_case": row["oracle"]["case"],
                "profile": row["runs"]["profile"],
                "identity": row["identity"],
                "identity_evidence": row["identity_evidence"],
                "effect_receipt_state": row["effect_receipt_state"],
                "identity_requirements": row.get("identity_requirements"),
                "track_identity_requirements": row.get("track_identity_requirements"),
            }
            for row in rows
        ]
    )


def _validate_frozen_identity_requirements(value: Any) -> dict[str, Any]:
    """Validate the matrix-wide identity requirements before row execution."""

    _require(isinstance(value, Mapping), "frozen matrix identity_requirements must be an object", RunnerError)
    required_tracks = {"reference", "candidate", "native", "ncm", "semantic"}
    _require(
        set(value) == required_tracks,
        "frozen matrix identity_requirements must declare reference, candidate, native, ncm and semantic requirements",
        RunnerError,
    )
    expected_fields = {
        "reference": {"source_revision", "binary", "features", "profile", "rule"},
        "candidate": {"source_revision", "binary", "features", "profile", "rule"},
        "native": {"provider_id", "registration_revision", "provider_instance", "exact_scope", "request_digest", "contribution_digest", "marker"},
        "ncm": {"provider_id", "implementation_sha256", "protocol_version", "state_schema_version", "state_generation", "exact_scope", "model_artifact_sha256", "tokenizer_sha256", "vector_fixture_digest"},
        "semantic": {"model_artifact_sha256", "tokenizer_sha256", "projection_key", "search_index_key", "source_generation", "vector_generation", "capability_manifest_digest", "calibration_profile_id", "calibration_digest", "rerank_policy_pins"},
    }
    for track, fields in expected_fields.items():
        section = value.get(track)
        _require(isinstance(section, Mapping), f"frozen matrix {track} identity requirements must be an object", RunnerError)
        _require(
            set(section) == fields,
            f"frozen matrix {track} identity requirements have an incomplete or unexpected field set",
            RunnerError,
        )
        for field, requirement in section.items():
            if field == "exact_scope" or (field == "features" and isinstance(requirement, list)):
                _require(
                    isinstance(requirement, list)
                    and bool(requirement)
                    and all(isinstance(item, str) and bool(item.strip()) for item in requirement),
                    f"frozen matrix {track}.{field} must be a non-empty string list",
                    RunnerError,
                )
            elif isinstance(requirement, Mapping):
                _require(
                    set(requirement) == {"reason", "state", "value"}
                    or set(requirement) == {"state", "value"},
                    f"frozen matrix {track}.{field} requirement descriptor has unexpected fields",
                    RunnerError,
                )
                _require(
                    isinstance(requirement.get("state"), str)
                    and bool(requirement.get("state", "").strip()),
                    f"frozen matrix {track}.{field} requirement state is missing",
                    RunnerError,
                )
            else:
                _require(
                    isinstance(requirement, str) and bool(requirement.strip()),
                    f"frozen matrix {track}.{field} requirement must be a non-empty string or descriptor",
                    RunnerError,
                )
    reference_requirement_revision = value["reference"].get("source_revision")
    # The frozen matrix's top-level audit text predates the runner and carries
    # a truncated 39-character prefix.  Preserve that historical declaration
    # for audit, while the executable source/binary attestation gates below
    # continue to require the exact 40-character 570 revision.
    _require(
        isinstance(reference_requirement_revision, str)
        and len(reference_requirement_revision) in (39, 40)
        and REFERENCE_REVISION.startswith(reference_requirement_revision),
        "frozen matrix reference identity requirement is not pinned to the 570 checkout",
        RunnerError,
    )
    for track in ("native", "ncm"):
        _require(
            value[track].get("provider_id") == ("tracedecay.native" if track == "native" else "ncm"),
            f"frozen matrix {track} provider identity is not exact",
            RunnerError,
        )
    _require(
        value["ncm"].get("protocol_version") == "1.0",
        "frozen matrix NCM protocol version is not exact",
        RunnerError,
    )
    return copy.deepcopy(dict(value))


def validate_frozen_readiness_matrix(matrix: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the frozen matrix shape, row identities and campaign counts."""

    _require(isinstance(matrix, Mapping), "frozen readiness matrix must be an object", RunnerError)
    _require(matrix.get("schema_version") == 1, "frozen readiness matrix schema version changed", RunnerError)
    _require(matrix.get("matrix_id") == READINESS_MATRIX_ID, "frozen readiness matrix id changed", RunnerError)
    rows = matrix.get("rows")
    _require(isinstance(rows, list), "frozen readiness matrix rows must be a list", RunnerError)
    _require(
        len(rows) == FROZEN_READINESS_ROW_COUNT,
        f"frozen readiness matrix must contain {FROZEN_READINESS_ROW_COUNT} rows",
        RunnerError,
    )
    seen: set[str] = set()
    normalized_rows: list[dict[str, Any]] = []
    profiles = matrix.get("run_profiles")
    _require(isinstance(profiles, Mapping), "frozen readiness matrix run_profiles must be an object", RunnerError)
    identity_requirements = _validate_frozen_identity_requirements(matrix.get("identity_requirements"))
    for index, raw_row in enumerate(rows):
        _require(isinstance(raw_row, Mapping), f"frozen readiness matrix row {index} must be an object", RunnerError)
        row = dict(raw_row)
        row_id = row.get("id")
        _require(isinstance(row_id, str) and bool(row_id.strip()), f"frozen readiness matrix row {index} lacks id", RunnerError)
        _require(row_id not in seen, f"frozen readiness matrix has duplicate row id: {row_id}", RunnerError)
        seen.add(row_id)
        oracle_kind = row.get("oracle_kind")
        _require(
            isinstance(oracle_kind, str) and bool(oracle_kind.strip()),
            f"frozen readiness matrix row {row_id} lacks oracle_kind",
            RunnerError,
        )
        oracle = row.get("oracle")
        _require(
            isinstance(oracle, Mapping),
            f"frozen readiness matrix row {row_id} lacks oracle details",
            RunnerError,
        )
        _require(
            oracle.get("kind") == oracle_kind,
            f"frozen readiness matrix row {row_id} oracle kind is inconsistent",
            RunnerError,
        )
        _require(
            isinstance(oracle.get("case"), str) and bool(oracle["case"].strip()),
            f"frozen readiness matrix row {row_id} lacks oracle case",
            RunnerError,
        )
        track = row.get("track")
        _require(
            isinstance(track, str) and track in READINESS_TRACKS,
            f"frozen readiness matrix row {row_id} has invalid track",
            RunnerError,
        )
        _require(
            isinstance(row.get("operation"), str) and bool(row["operation"].strip()),
            f"frozen readiness matrix row {row_id} lacks operation identity",
            RunnerError,
        )
        _require(
            isinstance(row.get("identity"), str) and bool(row["identity"].strip()),
            f"frozen readiness matrix row {row_id} lacks identity profile",
            RunnerError,
        )
        _require(
            isinstance(row.get("identity_evidence"), Mapping) and bool(row["identity_evidence"]),
            f"frozen readiness matrix row {row_id} lacks identity evidence shape",
            RunnerError,
        )
        _require(
            isinstance(row.get("effect_receipt_state"), Mapping) and bool(row["effect_receipt_state"]),
            f"frozen readiness matrix row {row_id} lacks effect evidence shape",
            RunnerError,
        )
        runs = row.get("runs")
        _require(isinstance(runs, Mapping), f"frozen readiness matrix row {row_id} lacks runs", RunnerError)
        planned = runs.get("planned")
        _require(
            isinstance(planned, int) and not isinstance(planned, bool) and planned > 0,
            f"frozen readiness matrix row {row_id} has invalid planned run count",
            RunnerError,
        )
        executed = runs.get("executed")
        _require(
            isinstance(executed, int) and not isinstance(executed, bool) and executed >= 0,
            f"frozen readiness matrix row {row_id} has invalid executed run count",
            RunnerError,
        )
        profile = runs.get("profile")
        _require(
            isinstance(profile, str) and profile in profiles,
            f"frozen readiness matrix row {row_id} references an unknown run profile",
            RunnerError,
        )
        _require(
            runs.get("pass_required") is True,
            f"frozen readiness matrix row {row_id} must require a pass outcome",
            RunnerError,
        )
        # Attach the frozen matrix-wide identity envelope before computing the
        # semantic row digest.  A file-level SHA protects the bytes on disk,
        # while this independent digest also rejects a caller that relabels
        # Native/NCM/semantic identity requirements in an in-memory matrix.
        row["identity_requirements"] = copy.deepcopy(identity_requirements)
        row["track_identity_requirements"] = copy.deepcopy(identity_requirements.get(track, {}))
        normalized_rows.append(row)
    _require(
        _readiness_row_identity_digest(normalized_rows) == FROZEN_READINESS_ROW_IDS_SHA256,
        "frozen readiness matrix row identities changed",
        RunnerError,
    )
    _require(
        _readiness_oracle_runs_digest(normalized_rows) == FROZEN_READINESS_ORACLE_RUNS_SHA256,
        "frozen readiness matrix oracle or planned run counts changed",
        RunnerError,
    )
    _require(
        _readiness_row_bindings_digest(normalized_rows) == FROZEN_READINESS_ROW_BINDINGS_SHA256,
        "frozen readiness matrix operation/oracle/profile/evidence bindings changed",
        RunnerError,
    )
    planned_total = sum(int(row["runs"]["planned"]) for row in normalized_rows)
    _require(
        planned_total == FROZEN_READINESS_PLANNED_ATTEMPTS,
        f"frozen readiness matrix planned run total changed: {planned_total}",
        RunnerError,
    )
    summary = matrix.get("summary")
    if isinstance(summary, Mapping) and "total_rows" in summary:
        _require(summary.get("total_rows") == FROZEN_READINESS_ROW_COUNT, "frozen readiness matrix summary row count changed", RunnerError)
    return {
        "path": matrix.get("_path"),
        "sha256": matrix.get("_sha256", FROZEN_READINESS_MATRIX_SHA256),
        "matrix_id": matrix["matrix_id"],
        "row_count": len(normalized_rows),
        "row_ids": sorted(seen),
        "planned_attempts": planned_total,
        "rows": [
            {
                **row,
                # Preserve the matrix-wide requirements with every row that
                # reaches execution.  A track-specific view makes ledger
                # validation unambiguous while the full map keeps the source
                # contract auditable.
                "identity_requirements": copy.deepcopy(identity_requirements),
                "track_identity_requirements": copy.deepcopy(identity_requirements.get(row["track"], {})),
            }
            for row in normalized_rows
        ],
        "identity_requirements": identity_requirements,
    }


def _route_map(contract: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    _require(isinstance(contract, Mapping), "case contract must be an object", ContractError)
    routes = contract.get("routes")
    _require(isinstance(routes, list), "case contract routes must be a list", ContractError)
    result: dict[str, dict[str, Any]] = {}
    for index, route in enumerate(routes):
        _require(isinstance(route, Mapping), f"route {index} must be an object", ContractError)
        route_id = route.get("id")
        _require(isinstance(route_id, str) and bool(route_id), f"route {index} has no id", ContractError)
        _require(route_id not in result, f"duplicate route id: {route_id}", ContractError)
        result[route_id] = dict(route)
    return result


def _route_effect_policy(route: Mapping[str, Any] | None) -> str | None:
    if route is None:
        return None
    policy = route.get("effect_policy")
    if isinstance(policy, str):
        return policy
    # Keep hand-authored older contracts useful while requiring the accepted
    # contract to publish the explicit policy on every route.  This inference
    # is also useful for callers that exercise compare helpers directly.
    operation_kind = route.get("operation_kind")
    if operation_kind in ("mutation", "host_extension"):
        return "required"
    if operation_kind == "explicit_search":
        return "retrieval"
    if operation_kind in ("read", "semantic_read", "temporal_read"):
        return "none"
    return "unknown"


def _validate_pointer_list(value: Any, label: str) -> None:
    _require(isinstance(value, (list, tuple)), f"{label} must be a list")
    for pointer in value:
        _require(isinstance(pointer, str), f"{label} entries must be strings")
        _pointer_parts(pointer)


def _validate_comparison(value: Any, label: str) -> None:
    if value is None:
        return
    _require(isinstance(value, Mapping), f"{label} comparison must be an object")
    for field in (
        "semantic_json_pointers",
        "ignore_json_pointers",
        "required_json_pointers",
        "effect_json_pointers",
        "receipt_json_pointers",
        "state_json_pointers",
        "no_effect_json_pointers",
        "reopen_or_no_effect_json_pointers",
    ):
        _validate_pointer_list(value.get(field, ()), f"{label} {field}")
    effect_mode = value.get("effect_mode")
    if effect_mode is not None:
        _require(
            effect_mode in ("required", "none", "retrieval", "unknown"),
            f"{label} effect_mode is invalid",
        )
    expected_terminal = value.get("expected_terminal")
    if expected_terminal is not None:
        _require(
            expected_terminal in (
                "completed",
                "error",
                "unsupported",
                "unknown",
                "invalid",
                "censored",
                "cancelled",
                "partial",
                "effect_unknown",
                "blocked",
            ),
            f"{label} expected_terminal is invalid",
        )
    if "required_json_pointers" in value and not value.get("required_json_pointers"):
        _require(
            expected_terminal is not None,
            f"{label} must declare expected_terminal when no semantic observable is required",
        )
    mappings = value.get("identity_mappings", ())
    _require(isinstance(mappings, (list, tuple)), f"{label} identity_mappings must be a list")
    names: set[str] = set()
    for mapping in mappings:
        _require(isinstance(mapping, Mapping), f"{label} identity mapping must be an object")
        name = mapping.get("name")
        _require(isinstance(name, str) and name and name not in names, f"{label} identity mapping name is invalid")
        names.add(name)
        for field in ("original_pointer", "product_pointer"):
            pointer = mapping.get(field)
            _require(isinstance(pointer, str), f"{label} identity mapping lacks {field}")
            _pointer_parts(pointer)


def _validate_effect_declaration(
    value: Any,
    route: Mapping[str, Any],
    label: str,
) -> None:
    """Require every paired operation to declare its effect/no-effect proof."""

    if value is None:
        return
    policy = _route_effect_policy(route)
    if policy not in ("required", "none", "retrieval"):
        return
    mode = value.get("effect_mode")
    _require(mode == policy, f"{label} effect_mode must be {policy!r} for route {route['id']}")
    if policy == "required":
        for field in ("effect_json_pointers", "receipt_json_pointers", "state_json_pointers"):
            _require(bool(value.get(field)), f"{label} must declare {field} for a mutating route")
    elif policy == "none":
        _require(bool(value.get("no_effect_json_pointers")), f"{label} must declare no_effect_json_pointers for a read route")
    else:
        _require(
            bool(value.get("effect_json_pointers")) or bool(value.get("no_effect_json_pointers")),
            f"{label} must declare effect or no-effect observables for a retrieval route",
        )


def _validate_output_bindings(value: Any, label: str) -> None:
    if value is None:
        return
    _require(isinstance(value, Mapping), f"{label} output_bindings must be an object")
    for name, pointer in value.items():
        _require(isinstance(name, str) and bool(name.strip()), f"{label} output binding name is invalid")
        _require(isinstance(pointer, str), f"{label} output binding {name!r} must be a JSON pointer")
        _pointer_parts(pointer)


def _validate_embedded_case_schema_document(schema: Mapping[str, Any]) -> None:
    """Check the contract's embedded Draft 2020-12 document is substantive.

    Ajv validates the complete document in the independent test suite, while
    this dependency-free guard prevents a caller from injecting a vacuous
    ``case_schema`` through the Python API and then using runtime validation as
    if it were the accepted contract.
    """

    _require(isinstance(schema, Mapping), "case contract embedded schema must be an object")
    _require(
        schema.get("$schema") == "https://json-schema.org/draft/2020-12/schema",
        "case contract embedded schema must declare Draft 2020-12",
    )
    schema_id = schema.get("$id")
    _require(
        isinstance(schema_id, str)
        and re.fullmatch(r"https://schemas\.tracedecay\.invalid/[^#]+", schema_id)
        is not None,
        "case contract embedded schema id must be an absolute non-fragment URI",
    )
    _require(schema.get("type") == "object", "case contract embedded schema must describe an object")
    required = schema.get("required")
    _require(
        isinstance(required, list)
        and "classification" in required
        and "actions" in required,
        "case contract embedded schema must require classification and actions",
    )
    properties = schema.get("properties")
    _require(isinstance(properties, Mapping), "case contract embedded schema properties are missing")
    actions_schema = properties.get("actions")
    _require(
        isinstance(actions_schema, Mapping)
        and actions_schema.get("type") == "array"
        and actions_schema.get("minItems") == 1
        and isinstance(actions_schema.get("items"), Mapping),
        "case contract embedded schema must require at least one action",
    )
    action_schema = actions_schema["items"]
    _require(
        isinstance(action_schema.get("required"), list)
        and all(field in action_schema["required"] for field in ("id", "route", "request")),
        "case contract embedded action schema is incomplete",
    )
    matrix_schema = properties.get("matrix_binding")
    matrix_required = (
        "matrix_row_id",
        "track",
        "profile",
        "operation",
        "oracle_kind",
        "oracle_case",
        "identity",
        "identity_evidence",
        "effect_receipt_state",
        "required_identity_fields",
        "required_evidence_fields",
        "identity_requirements",
        "track_identity_requirements",
    )
    _require(
        isinstance(matrix_schema, Mapping)
        and isinstance(matrix_schema.get("required"), list)
        and all(field in matrix_schema["required"] for field in matrix_required),
        "case contract embedded matrix binding schema is incomplete",
    )
    matrix_properties = matrix_schema.get("properties")
    _require(isinstance(matrix_properties, Mapping), "case contract embedded matrix binding properties are missing")
    defs = schema.get("$defs")
    _require(isinstance(defs, Mapping), "case contract embedded schema definitions are missing")
    required_attempt = defs.get("requiredAttemptEvidence")
    required_attempt_properties = (
        required_attempt.get("properties")
        if isinstance(required_attempt, Mapping)
        else None
    )
    required_attempt_state = (
        required_attempt_properties.get("state")
        if isinstance(required_attempt_properties, Mapping)
        else None
    )
    required_attempt_value = (
        required_attempt_properties.get("value")
        if isinstance(required_attempt_properties, Mapping)
        else None
    )
    _require(
        isinstance(required_attempt, Mapping)
        and required_attempt.get("type") == "object"
        and required_attempt.get("required") == ["state", "value"]
        and isinstance(required_attempt_properties, Mapping)
        and isinstance(required_attempt_state, Mapping)
        and required_attempt_state.get("const") == "required_per_attempt"
        and isinstance(required_attempt_value, Mapping)
        and required_attempt_value.get("type") == "null"
        and required_attempt.get("additionalProperties") is False,
        "case contract required attempt evidence definition is incomplete",
    )
    identity_definition = defs.get("identityEvidence")
    identity_fields = (
        "candidate_binary_sha256",
        "process_identity",
        "reference_binary_sha256",
        "route_identity",
        "store_identity",
    )
    _require(
        isinstance(identity_definition, Mapping)
        and identity_definition.get("type") == "object"
        and isinstance(identity_definition.get("required"), list)
        and all(field in identity_definition["required"] for field in identity_fields)
        and isinstance(identity_definition.get("properties"), Mapping)
        and all(
            isinstance(identity_definition["properties"].get(field), Mapping)
            and identity_definition["properties"][field].get("$ref")
            == "#/$defs/requiredAttemptEvidence"
            for field in identity_fields
        )
        and identity_definition.get("additionalProperties") is False,
        "case contract identity evidence definition is incomplete",
    )
    effect_definition = defs.get("effectReceiptState")
    effect_properties = (
        effect_definition.get("properties")
        if isinstance(effect_definition, Mapping)
        else None
    )
    effect_fields = (
        "effect_digest",
        "expected_effect",
        "receipt_digest",
        "receipt_policy",
        "reopen_or_no_effect",
        "state_digest",
    )
    _require(
        isinstance(effect_definition, Mapping)
        and effect_definition.get("type") == "object"
        and isinstance(effect_definition.get("required"), list)
        and all(field in effect_definition["required"] for field in effect_fields)
        and isinstance(effect_properties, Mapping)
        and all(
            isinstance(effect_properties.get(field), Mapping)
            and (
                effect_properties[field].get("$ref")
                == "#/$defs/requiredAttemptEvidence"
                if field in ("effect_digest", "receipt_digest", "reopen_or_no_effect", "state_digest")
                else effect_properties[field].get("type") == "string"
            )
            for field in effect_fields
        )
        and effect_definition.get("additionalProperties") is False,
        "case contract effect/receipt/state definition is incomplete",
    )
    _require(
        isinstance(matrix_properties.get("identity_evidence"), Mapping)
        and matrix_properties["identity_evidence"].get("$ref") == "#/$defs/identityEvidence"
        and isinstance(matrix_properties.get("effect_receipt_state"), Mapping)
        and matrix_properties["effect_receipt_state"].get("$ref") == "#/$defs/effectReceiptState",
        "case contract matrix binding does not reference its complete evidence definitions",
    )


def validate_contract(contract: Mapping[str, Any]) -> None:
    _require(isinstance(contract, Mapping), "case contract must be an object")
    _require(contract.get("format") == CONTRACT_FORMAT, "case contract format is not the accepted Native original contract")
    _require(contract.get("contract_id") == CONTRACT_FORMAT, "case contract id is not the accepted Native original contract")
    _require(contract.get("status") == "accepted", "case contract is not marked accepted")
    _validate_embedded_case_schema_document(contract.get("case_schema"))
    baseline = contract.get("baseline")
    _require(isinstance(baseline, dict), "case contract is missing baseline")
    _require(
        baseline.get("reference_revision") == REFERENCE_REVISION,
        "case contract has an incorrect 570 reference revision",
    )
    _require(
        baseline.get("product_revision") == PRODUCT_REVISION,
        "case contract must use the moving current candidate revision",
    )
    _require(
        baseline.get("reference_checkout") in (REFERENCE_CHECKOUT_NAME, f".worktrees/{REFERENCE_CHECKOUT_NAME}"),
        "case contract must name the detached 570 reference checkout",
    )
    _require(
        baseline.get("reference_features") == "test-transport",
        "case contract reference feature set must remain test-transport",
    )
    _require(
        baseline.get("candidate_features") == "production,memory-provider-host,semantic-fastembed",
        "case contract candidate feature set must remain production,memory-provider-host,semantic-fastembed",
    )
    runner = contract.get("runner")
    _require(isinstance(runner, Mapping), "case contract is missing runner description")
    entrypoints = runner.get("entrypoints")
    _require(isinstance(entrypoints, Mapping), "case contract runner entrypoints must be an object")
    cli_entrypoint = entrypoints.get("cli_tool")
    mcp_entrypoint = entrypoints.get("mcp_stdio")
    _require(isinstance(cli_entrypoint, Mapping), "case contract is missing cli_tool entrypoint")
    _require(isinstance(mcp_entrypoint, Mapping), "case contract is missing mcp_stdio entrypoint")
    _require(cli_entrypoint.get("transport") == "process", "cli_tool entrypoint must be a process")
    _require(mcp_entrypoint.get("transport") == "mcp_jsonl", "mcp_stdio entrypoint must use MCP JSONL")
    _require(runner.get("normative_command") == NORMATIVE_RUNNER_COMMAND, "case contract normative runner command changed")
    attestation = runner.get("build_attestation")
    _require(isinstance(attestation, Mapping), "case contract is missing build attestation policy")
    _require(attestation.get("format") == BUILD_ATTESTATION_FORMAT, "case contract build attestation format changed")
    _require(
        tuple(attestation.get("fields", ()))
        == ("side", "features", "source_root", "source_revision", "binary_path", "binary_sha256"),
        "case contract build attestation fields changed",
    )
    _require(
        attestation.get("required_for") == "both original and candidate binaries",
        "case contract must attest both original and candidate binaries",
    )
    ncm_attestation = runner.get("ncm_worker_attestation")
    _require(isinstance(ncm_attestation, Mapping), "case contract is missing NCM worker attestation policy")
    _require(
        ncm_attestation.get("format") == NCM_WORKER_ATTESTATION_FORMAT
        and ncm_attestation.get("required_for") == "normal NCM worker execution; independent 570 oracle records the reference worker as unavailable",
        "case contract NCM worker attestation policy changed",
    )
    _require(
        tuple(ncm_attestation.get("fields", ())) == NCM_WORKER_ATTESTATION_FIELDS
        and tuple(ncm_attestation.get("identity_pin_fields", ())) == NCM_WORKER_PIN_FIELDS,
        "case contract NCM worker identity pins changed",
    )
    outcome = runner.get("outcomes")
    _require(isinstance(outcome, dict), "case contract is missing outcome semantics")
    _require(tuple(outcome.get("statuses", ())) == OUTCOME_STATUSES, "case contract outcome statuses changed")
    _require(tuple(outcome.get("retained_statuses", ())) == RETAINED_OUTCOME_STATUS_V1, "case contract retained outcome status vocabulary changed")
    ledger = runner.get("ledger")
    _require(isinstance(ledger, dict), "case contract is missing attempt ledger semantics")
    _require(ledger.get("storage") == "append-only JSONL", "attempt ledger must be append-only JSONL")
    _require(
        tuple(ledger.get("outcome_enum", ())) == OUTCOME_STATUSES,
        "case contract ledger outcome enum changed",
    )
    _require(
        tuple(ledger.get("required_fields", ())) == LEDGER_REQUIRED_FIELDS,
        "case contract ledger required fields changed",
    )
    readiness = runner.get("readiness")
    _require(isinstance(readiness, dict), "case contract is missing readiness selection semantics")
    _require(readiness.get("matrix_id") == READINESS_MATRIX_ID, "case contract readiness matrix is not accepted")
    for field, expected_value in (
        ("matrix_sha256", FROZEN_READINESS_MATRIX_SHA256),
        ("matrix_row_count", FROZEN_READINESS_ROW_COUNT),
        ("matrix_planned_attempts", FROZEN_READINESS_PLANNED_ATTEMPTS),
        ("matrix_row_ids_sha256", FROZEN_READINESS_ROW_IDS_SHA256),
        ("matrix_oracle_runs_sha256", FROZEN_READINESS_ORACLE_RUNS_SHA256),
        ("matrix_row_bindings_sha256", FROZEN_READINESS_ROW_BINDINGS_SHA256),
    ):
        _require(
            readiness.get(field) == expected_value,
            f"case contract readiness {field} does not match the frozen matrix",
        )
    _require(
        readiness.get("filtered_suite_policy") == "reject_as_readiness_evidence",
        "readiness mode must reject filtered suites as evidence",
    )
    _require(isinstance(readiness.get("attempt_population_policy"), str), "case contract readiness population policy is missing")
    _require(
        readiness.get("case_binding")
        == "matrix_binding must exactly repeat matrix_row_id, track, profile, operation, oracle_kind, oracle_case, identity, identity_evidence, effect_receipt_state, required_identity_fields, required_evidence_fields, identity_requirements, and track_identity_requirements; every readiness action must repeat matrix_row_id, matrix_operation, matrix_oracle_case, and matrix_route, with public route translation checked against the frozen row",
        "case contract readiness case binding policy changed",
    )
    _require(
        readiness.get("matrix_compatibility_command") == NORMATIVE_RUNNER_COMMAND + " --case <row-id> --ledger <append-only-jsonl>",
        "case contract matrix compatibility command changed",
    )
    capture = runner.get("capture")
    _require(isinstance(capture, dict), "case contract is missing capture semantics")
    for field in ("request", "stdout", "stderr", "process", "side_state", "comparison"):
        _require(field in capture, f"case contract capture is missing {field}")
    routes = contract.get("routes")
    _require(isinstance(routes, list) and routes, "case contract must publish at least one route")
    seen: set[str] = set()
    for route in routes:
        _require(isinstance(route, dict), "route entry must be an object")
        route_id = route.get("id")
        _require(isinstance(route_id, str) and route_id and route_id not in seen, f"invalid or duplicate route id: {route_id!r}")
        seen.add(route_id)
        _require(
            route.get("classification") in ("original_native", "session_lcm", "host_extension", "unsupported"),
            f"invalid route classification: {route_id}",
        )
        _require(isinstance(route.get("operation_kind"), str) and route["operation_kind"], f"route lacks operation kind: {route_id}")
        _require(
            route.get("effect_policy") in ("required", "none", "retrieval", "unknown"),
            f"route has invalid effect policy: {route_id}",
        )
        _require(isinstance(route.get("direct_boundary"), str) and route["direct_boundary"], f"route lacks direct boundary: {route_id}")
        availability = route.get("availability")
        if isinstance(availability, str):
            _require(availability in ROUTE_AVAILABILITY, f"invalid route availability: {route_id}")
        else:
            _require(isinstance(availability, dict), f"route availability must be object or string: {route_id}")
            for side in SIDE_NAMES:
                _require(availability.get(side) in ROUTE_AVAILABILITY, f"route {route_id} lacks {side} availability")
        entrypoints = route.get("entrypoints", {})
        _require(isinstance(entrypoints, dict), f"route entrypoints must be an object: {route_id}")
        for name, entrypoint in entrypoints.items():
            _require(name in ("cli_tool", "mcp_stdio", "command"), f"unknown route entrypoint {name}: {route_id}")
            _require(isinstance(entrypoint, dict), f"route entrypoint must be an object: {route_id}/{name}")
        expected_mcp_tool = FACT_STORE_MCP_ROUTE_TOOLS.get(route_id)
        if expected_mcp_tool is not None:
            mcp_entrypoint = entrypoints.get("mcp_stdio")
            _require(
                isinstance(mcp_entrypoint, Mapping)
                and mcp_entrypoint.get("tool") == expected_mcp_tool,
                f"route {route_id} must publish daemon-mapped MCP tool {expected_mcp_tool!r}",
            )
    required_routes = REQUIRED_NATIVE_ROUTES | REQUIRED_SESSION_LCM_ROUTES | REQUIRED_HOST_EXTENSION_ROUTES
    missing_routes = sorted(required_routes - seen)
    _require(
        not missing_routes,
        "case contract is missing required Native/session/LCM/host routes: " + ", ".join(missing_routes),
    )


def validate_case(case: Mapping[str, Any], contract: Mapping[str, Any]) -> None:
    _require(isinstance(case, Mapping), "case must be an object")
    _require(isinstance(contract, Mapping), "case contract must be an object")
    embedded_schema = contract.get("case_schema")
    _require(isinstance(embedded_schema, Mapping), "case contract is missing its embedded case schema")
    _validate_case_schema(case, embedded_schema)
    routes = _route_map(contract)
    case_id = case.get("id", case.get("case_id"))
    _require(isinstance(case_id, str) and bool(case_id.strip()), "case id is required")
    _require(
        "id" not in case or "case_id" not in case or case.get("id") == case.get("case_id"),
        "case id and case_id aliases disagree",
    )
    classification = case.get("classification", "original_native")
    _require(classification in ("original_native", "host_extension", "state_regression", "session_lcm"), f"invalid case classification: {case_id}")
    actions = case.get("actions")
    _require(isinstance(actions, list) and actions, f"case has no actions: {case_id}")
    action_ids: set[str] = set()
    for action in actions:
        _require(isinstance(action, dict), f"case action is not an object: {case_id}")
        action_id = action.get("id", action.get("action_id"))
        _require(isinstance(action_id, str) and bool(action_id.strip()) and action_id not in action_ids, f"invalid or duplicate action id in {case_id}")
        _require(
            "id" not in action or "action_id" not in action or action.get("id") == action.get("action_id"),
            f"case {case_id} action id and action_id aliases disagree",
        )
        action_ids.add(action_id)
        for input_field in ("request", "argv", "environment", "cwd"):
            if input_field in action:
                _reject_side_dependent_logical_input(action.get(input_field), f"{case_id}/{action_id}/{input_field}")
        route_id = action.get("route", action.get("operation"))
        if "route" in action and "operation" in action:
            _require(
                action.get("route") == action.get("operation"),
                f"case {case_id}/{action_id} route and operation aliases disagree",
            )
        _require(isinstance(route_id, str) and route_id in routes, f"case {case_id} references unknown route {route_id!r}")
        route = routes[route_id]
        callable_sides = {
            side
            for side in SIDE_NAMES
            if _availability(route, side) in ("supported", "external_harness")
        }
        if callable_sides:
            _require(isinstance(action.get("request"), dict), f"callable action request must be an object: {case_id}/{action_id}")
        entrypoint = action.get("entrypoint")
        default_availability = _availability(route, "original")
        if entrypoint is None and default_availability not in ("unsupported", "unknown"):
            entrypoint = "command" if default_availability == "external_harness" else "cli_tool"
        if entrypoint is not None:
            _require(entrypoint in ("cli_tool", "mcp_stdio", "command"), f"unknown action entrypoint: {case_id}/{action_id}")
            entrypoints = route.get("entrypoints", {})
            _require(isinstance(entrypoints, Mapping), f"route {route_id} entrypoints are malformed: {case_id}/{action_id}")
            _require(
                entrypoint in entrypoints
                or any(_availability(route, side) == "external_harness" for side in callable_sides),
                f"route {route_id} does not publish the {entrypoint} entrypoint: {case_id}/{action_id}",
            )
        if entrypoint == "mcp_stdio":
            _validate_mcp_tool(route, action, f"{case_id}/{action_id}")
        if entrypoint == "command" and callable_sides:
            route_entrypoints = route.get("entrypoints", {})
            _require(isinstance(route_entrypoints, Mapping), f"route {route_id} entrypoints are malformed: {case_id}/{action_id}")
            command_entrypoint = route_entrypoints.get("command", {})
            _require(isinstance(command_entrypoint, Mapping), f"route {route_id} command entrypoint is malformed: {case_id}/{action_id}")
            argv = action.get("argv", command_entrypoint.get("argv_template"))
            _require(isinstance(argv, list) and argv, f"command action requires argv: {case_id}/{action_id}")
        _validate_output_bindings(action.get("output_bindings"), f"{case_id}/{action_id}")
        _validate_comparison(action.get("comparison"), f"{case_id}/{action_id}")
        if action.get("comparison") is not None:
            _validate_effect_declaration(action.get("comparison"), route, f"{case_id}/{action_id}")
        checkpoints = action.get("checkpoints", {})
        _require(isinstance(checkpoints, dict), f"case action checkpoints must be an object: {case_id}/{action_id}")
        for phase in ("before", "after", "reopened"):
            specs = checkpoints.get(phase, [])
            if isinstance(specs, dict):
                specs = [specs]
            _require(isinstance(specs, list), f"checkpoint list is invalid: {case_id}/{action_id}/{phase}")
            for spec in specs:
                _require(isinstance(spec, dict), f"checkpoint must be an object: {case_id}/{action_id}/{phase}")
                checkpoint_id = spec.get("id", spec.get("action_id"))
                if "id" in spec or "action_id" in spec:
                    _require(
                        isinstance(checkpoint_id, str) and bool(checkpoint_id.strip()),
                        f"checkpoint action id is invalid: {case_id}/{action_id}/{phase}",
                    )
                    _require(
                        "id" not in spec or "action_id" not in spec or spec.get("id") == spec.get("action_id"),
                        f"checkpoint id and action_id aliases disagree: {case_id}/{action_id}/{phase}",
                    )
                for input_field in ("request", "argv", "environment", "cwd"):
                    if input_field in spec:
                        _reject_side_dependent_logical_input(spec.get(input_field), f"{case_id}/{action_id}/{phase}/{input_field}")
                checkpoint_route = spec.get("route", spec.get("operation"))
                if "route" in spec and "operation" in spec:
                    _require(
                        spec.get("route") == spec.get("operation"),
                        f"checkpoint route and operation aliases disagree: {case_id}/{action_id}/{phase}",
                    )
                _require(
                    isinstance(checkpoint_route, str) and checkpoint_route in routes,
                    f"checkpoint references unknown route: {case_id}/{action_id}/{phase}",
                )
                checkpoint_route_obj = routes[checkpoint_route]
                checkpoint_callable_sides = {
                    side
                    for side in SIDE_NAMES
                    if _availability(checkpoint_route_obj, side) in ("supported", "external_harness")
                }
                _require(
                    isinstance(spec.get("request"), dict)
                    if checkpoint_callable_sides
                    else isinstance(spec.get("request", {}), dict),
                    f"checkpoint request must be an object: {case_id}/{action_id}/{phase}",
                )
                checkpoint_entrypoint = spec.get("entrypoint")
                if checkpoint_entrypoint is not None:
                    checkpoint_entrypoints = checkpoint_route_obj.get("entrypoints", {})
                    _require(isinstance(checkpoint_entrypoints, Mapping), f"checkpoint route entrypoints are malformed: {case_id}/{action_id}/{phase}")
                    _require(
                        checkpoint_entrypoint in checkpoint_entrypoints,
                        f"checkpoint references unpublished entrypoint: {case_id}/{action_id}/{phase}",
                    )
                if checkpoint_entrypoint == "mcp_stdio":
                    _validate_mcp_tool(
                        routes[checkpoint_route],
                        spec,
                        f"{case_id}/{action_id}/{phase}",
                    )
                _validate_output_bindings(
                    spec.get("output_bindings"), f"{case_id}/{action_id}/{phase}"
                )
                _validate_comparison(spec.get("comparison"), f"{case_id}/{action_id}/{phase}")
                if spec.get("comparison") is not None:
                    _validate_effect_declaration(spec.get("comparison"), routes[checkpoint_route], f"{case_id}/{action_id}/{phase}")

    side_setup = case.get("side_setup", {})
    _require(isinstance(side_setup, Mapping), f"case side_setup must be an object: {case_id}")
    for side in SIDE_NAMES:
        specs = side_setup.get(side, [])
        _require(isinstance(specs, list), f"case side_setup.{side} must be a list: {case_id}")
        for setup_index, spec in enumerate(specs):
            if isinstance(spec, Mapping):
                for input_field in ("request", "argv", "environment", "cwd"):
                    if input_field in spec:
                        _reject_side_dependent_logical_input(spec.get(input_field), f"{case_id}/side_setup/{side}/{setup_index}/{input_field}")
            _validate_aux_action(spec, contract, f"{case_id}/side_setup/{side}/{setup_index}")
    _validate_side_setup_pair(
        side_setup,
        case_id=case_id,
        classification=classification,
        contract=contract,
    )
    # A retrieval oracle over an empty store is not a meaningful side effect
    # comparison.  Require a paired setup mutation for the public search route
    # when the case declares retrieval evidence; otherwise an empty response
    # can satisfy both sides without ever exercising the intended store.
    retrieval_setup_routes = {
        spec.get("route", spec.get("operation"))
        for spec in side_setup.get("original", [])
        if isinstance(spec, Mapping)
    }
    for action in actions:
        action_route = action.get("route", action.get("operation"))
        route = routes.get(action_route) if isinstance(action_route, str) else None
        comparison = action.get("comparison")
        if (
            isinstance(route, Mapping)
            and _route_effect_policy(route) == "retrieval"
            and isinstance(comparison, Mapping)
            and comparison.get("effect_mode") == "retrieval"
        ):
            has_seed = any(
                isinstance(setup_route, str)
                and isinstance(routes.get(setup_route), Mapping)
                and _route_effect_policy(routes[setup_route]) == "required"
                for setup_route in retrieval_setup_routes
            )
            _require(
                has_seed,
                f"case {case_id}/{action.get('id')} retrieval oracle requires a paired non-empty setup mutation",
                ContractError,
            )

    proof = case.get("composition_proof")
    if classification in ("original_native", "session_lcm", "state_regression"):
        _require(isinstance(proof, Mapping), f"case requires side-specific composition_proof: {case_id}")
        for side in SIDE_NAMES:
            _require(isinstance(proof.get(side), Mapping), f"case composition_proof lacks {side}: {case_id}")
    elif proof is not None:
        _require(isinstance(proof, Mapping), f"case composition_proof must be an object: {case_id}")
        _require(isinstance(proof.get("product"), Mapping), f"host_extension proof must include product: {case_id}")
    if isinstance(proof, Mapping):
        for side, spec in proof.items():
            if side not in SIDE_NAMES:
                continue
            proof_label = f"{case_id}/composition_proof/{side}"
            checks = spec.get("checks") if isinstance(spec, Mapping) else None
            if checks is not None:
                _require(isinstance(checks, list) and checks, f"composition proof checks must be non-empty: {proof_label}")
                for check_index, check in enumerate(checks):
                    check_label = f"{proof_label}/checks/{check_index}"
                    if isinstance(check, Mapping):
                        for input_field in ("request", "argv", "environment", "cwd"):
                            if input_field in check:
                                _reject_side_dependent_logical_input(check.get(input_field), f"{check_label}/{input_field}")
                    _validate_aux_action(check, contract, check_label)
                    _validate_proof_observables(check, check_label)
            else:
                for input_field in ("request", "argv", "environment", "cwd"):
                    if isinstance(spec, Mapping) and input_field in spec:
                        _reject_side_dependent_logical_input(spec.get(input_field), f"{proof_label}/{input_field}")
                _validate_aux_action(spec, contract, proof_label)
                _validate_proof_observables(spec, proof_label)

    lifecycle = case.get("lifecycle")
    if lifecycle is not None:
        _require(isinstance(lifecycle, Mapping), f"case lifecycle must be an object: {case_id}")
        close = lifecycle.get("close")
        reopen = lifecycle.get("reopen")
        _require(isinstance(close, Mapping), f"case lifecycle.close must contain per-side observations: {case_id}")
        _require(isinstance(reopen, Mapping), f"case lifecycle.reopen must contain per-side actions: {case_id}")
        for side in SIDE_NAMES:
            close_spec = close.get(side)
            _require(isinstance(close_spec, Mapping), f"case lifecycle.close lacks {side}: {case_id}")
            _require(close_spec.get("mode") in ("process_exit", "command"), f"invalid lifecycle close mode: {case_id}/{side}")
            if close_spec.get("mode") == "command":
                close_action = close_spec.get("action")
                if isinstance(close_action, Mapping):
                    for input_field in ("request", "argv", "environment", "cwd"):
                        if input_field in close_action:
                            _reject_side_dependent_logical_input(close_action.get(input_field), f"{case_id}/lifecycle/close/{side}/{input_field}")
                _validate_aux_action(close_spec.get("action"), contract, f"{case_id}/lifecycle/close/{side}")
            reopen_spec = reopen.get(side)
            _require(isinstance(reopen_spec, Mapping), f"case lifecycle.reopen lacks {side}: {case_id}")
            _require(
                reopen_spec.get("mode", "action") == "action",
                f"{case_id}/lifecycle/reopen/{side} requires mode='action'",
            )
            if reopen_spec.get("mode", "action") == "action":
                reopen_action = reopen_spec.get("action")
                _validate_aux_action(reopen_action, contract, f"{case_id}/lifecycle/reopen/{side}")
                if isinstance(reopen_action, Mapping):
                    for input_field in ("request", "argv", "environment", "cwd"):
                        if input_field in reopen_action:
                            _reject_side_dependent_logical_input(reopen_action.get(input_field), f"{case_id}/lifecycle/reopen/{side}/{input_field}")
                    declared_side = reopen_action.get("side")
                    _require(
                        declared_side in (None, side),
                        f"{case_id}/lifecycle/reopen/{side} action is assigned to {declared_side!r}",
                    )
        original_reopen_action = reopen.get("original", {}).get("action")
        product_reopen_action = reopen.get("product", {}).get("action")
        reopen_pair_error = _validate_reopen_pair(original_reopen_action, product_reopen_action, contract=contract)
        if reopen_pair_error is not None and reopen_pair_error.get("status") == "invalid":
            raise ContractError(
                f"{case_id} lifecycle reopen specs do not describe the same operation: "
                + str(reopen_pair_error.get("reason"))
            )


def _validate_proof_observables(spec: Mapping[str, Any], label: str) -> None:
    _require(isinstance(spec, Mapping), f"composition proof must be an object: {label}")
    required = spec.get("required_json_pointers", ())
    _require(isinstance(required, (list, tuple)), f"composition proof required_json_pointers must be a list: {label}")
    _require(isinstance(required, list) and required, f"composition proof requires observable pointers: {label}")
    for pointer in required:
        _pointer_parts(pointer)
    equals = spec.get("equals", {})
    _require(isinstance(equals, Mapping) and bool(equals), f"composition proof requires at least one exact value: {label}")
    for pointer in equals:
        _pointer_parts(pointer)


def _validate_mcp_tool(
    route: Mapping[str, Any],
    action: Mapping[str, Any],
    label: str,
) -> str:
    _require(isinstance(route, Mapping), f"{label} route must be an object", RunnerError)
    _require(isinstance(action, Mapping), f"{label} action must be an object", RunnerError)
    entrypoints = route.get("entrypoints", {})
    _require(isinstance(entrypoints, Mapping), f"{label} route entrypoints must be an object", RunnerError)
    endpoint = entrypoints.get("mcp_stdio", {})
    _require(isinstance(endpoint, Mapping), f"{label} MCP entrypoint must be an object", RunnerError)
    expected = endpoint.get("tool") if isinstance(endpoint, Mapping) else None
    actual = action.get("tool")
    _require(
        isinstance(expected, str) and bool(expected),
        f"{label} route does not publish an MCP tool",
    )
    _require(
        isinstance(actual, str) and actual == expected,
        f"{label} MCP tool {actual!r} does not match published tool {expected!r}",
    )
    return expected


def _validate_aux_action(spec: Any, contract: Mapping[str, Any], label: str) -> None:
    _require(isinstance(spec, Mapping), f"{label} must be an action object")
    allowed_keys = {
        "id",
        "action_id",
        "side",
        "route",
        "operation",
        "entrypoint",
        "request",
        "tool",
        "argv",
        "includes_binary",
        "environment",
        "cwd",
        "output_bindings",
        "comparison",
        "checkpoints",
        "composition_proof",
        "required_json_pointers",
        "equals",
        "phase",
        "protocol_version",
    }
    unknown_keys = sorted(str(key) for key in spec if key not in allowed_keys)
    _require(
        not unknown_keys,
        f"{label} contains unknown action fields: {', '.join(unknown_keys)}",
        ContractError,
    )
    for input_field in ("request", "argv", "environment", "cwd"):
        if input_field in spec:
            _reject_side_dependent_logical_input(spec.get(input_field), f"{label}/{input_field}")
    routes = _route_map(contract)
    route_id = spec.get("route", spec.get("operation"))
    action_id = spec.get("id", spec.get("action_id"))
    _require(
        isinstance(action_id, str) and bool(action_id.strip()),
        f"{label} action id is required",
    )
    _require(
        "id" not in spec or "action_id" not in spec or spec.get("id") == spec.get("action_id"),
        f"{label} id and action_id aliases disagree",
    )
    _require(
        "route" not in spec or spec.get("route") == route_id,
        f"{label} route and operation aliases disagree",
    )
    _require(isinstance(route_id, str) and route_id in routes, f"{label} references unknown route")
    route = routes[route_id]
    original_availability = _availability(route, "original")
    callable_sides = {
        side
        for side in SIDE_NAMES
        if _availability(route, side) in ("supported", "external_harness")
    }
    entrypoint = spec.get("entrypoint")
    if entrypoint is None and original_availability not in ("unsupported", "unknown"):
        entrypoint = "command" if original_availability == "external_harness" else "cli_tool"
    if entrypoint is not None:
        _require(entrypoint in ("cli_tool", "mcp_stdio", "command"), f"{label} has invalid entrypoint")
        entrypoints = route.get("entrypoints", {})
        _require(isinstance(entrypoints, Mapping), f"{label} route entrypoints are malformed")
        _require(entrypoint in entrypoints or any(_availability(route, side) == "external_harness" for side in callable_sides), f"{label} entrypoint is not published")
    if entrypoint == "mcp_stdio":
        _validate_mcp_tool(route, spec, label)
    _require(
        isinstance(spec.get("request"), dict) if callable_sides else isinstance(spec.get("request", {}), dict),
        f"{label} request must be an object",
    )
    _validate_output_bindings(spec.get("output_bindings"), label)
    _validate_comparison(spec.get("comparison"), label)
    if spec.get("comparison") is not None:
        _validate_effect_declaration(spec.get("comparison"), route, label)


def _validate_side_setup_pair(
    side_setup: Mapping[str, Any],
    *,
    case_id: str,
    classification: str,
    contract: Mapping[str, Any],
) -> None:
    """Require setup mutations to be the same logical operation on both sides."""

    if classification not in ("original_native", "session_lcm", "state_regression"):
        return
    original = side_setup.get("original", [])
    product = side_setup.get("product", [])
    _require(isinstance(original, list) and isinstance(product, list), f"case side_setup is malformed: {case_id}")
    # A setup declared for one side only would seed a different logical state
    # and let an empty store produce a superficially equal search result.
    _require(
        bool(original) == bool(product),
        f"case {case_id} side_setup must be present on both sides",
        ContractError,
    )
    by_side: dict[str, dict[str, Mapping[str, Any]]] = {}
    for side, specs in (("original", original), ("product", product)):
        values: dict[str, Mapping[str, Any]] = {}
        for index, spec in enumerate(specs):
            _require(isinstance(spec, Mapping), f"case {case_id} side_setup.{side}/{index} must be an object")
            action_id = spec.get("id", spec.get("action_id"))
            _require(
                isinstance(action_id, str) and bool(action_id.strip()) and action_id not in values,
                f"case {case_id} side_setup.{side} has an invalid or duplicate id",
                ContractError,
            )
            values[action_id] = spec
        by_side[side] = values
    _require(
        set(by_side["original"]) == set(by_side["product"]),
        f"case {case_id} side_setup ids differ between original and product",
        ContractError,
    )
    comparable_fields = (
        "route",
        "operation",
        "entrypoint",
        "tool",
        "argv",
        "includes_binary",
        "request",
        "environment",
        "cwd",
        "output_bindings",
        "comparison",
        "checkpoints",
        "composition_proof",
        "required_json_pointers",
        "equals",
        "phase",
        "protocol_version",
    )
    routes = _route_map(contract)
    for action_id in sorted(by_side["original"]):
        left = by_side["original"][action_id]
        right = by_side["product"][action_id]
        for field in comparable_fields:
            _require(
                _json_semantic_equal(left.get(field), right.get(field)),
                f"case {case_id} setup {action_id!r} field {field!r} differs between sides",
                ContractError,
            )
        route_id = left.get("route", left.get("operation"))
        route = routes.get(route_id) if isinstance(route_id, str) else None
        if isinstance(route, Mapping) and _route_effect_policy(route) in ("required", "retrieval"):
            _require(
                isinstance(left.get("request"), Mapping) and bool(left.get("request")),
                f"case {case_id} setup {action_id!r} mutation request must be non-empty",
                ContractError,
            )


def _owned_relative_path(value: Any, *, side_dir: Path, field: str, default: str) -> Path:
    """Resolve a case path inside the side's private root.

    Comparison cases are deliberately relocatable.  Relative paths stay under
    the side root; absolute paths are accepted only when they already resolve
    inside that same private root.  This prevents ``..`` traversal or a
    fixture symlink from touching operator state.
    """

    raw = default if value is None else value
    _require(isinstance(raw, str) and bool(raw), f"{field} must be a non-empty relative path")
    _require(isinstance(side_dir, Path), f"{field} side directory must be a Path", RunnerError)
    try:
        candidate = Path(raw)
        resolved_side = side_dir.resolve()
        resolved = candidate.resolve() if candidate.is_absolute() else (side_dir / candidate).resolve()
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise RunnerError(f"{field} path cannot be resolved: {raw!r}") from error
    _require(resolved == resolved_side or resolved.is_relative_to(resolved_side), f"{field} escapes the side root: {raw!r}")
    return resolved


def select_relevant_cases(
    cases: Sequence[Mapping[str, Any]],
    required_operations: Iterable[str] = (),
) -> list[dict[str, Any]]:
    _require(
        isinstance(cases, (list, tuple)),
        "cases must be a list",
        RunnerError,
    )
    for index, case in enumerate(cases):
        _require(isinstance(case, Mapping), f"case {index} must be an object", RunnerError)
        actions = case.get("actions", ())
        _require(isinstance(actions, (list, tuple)), f"case {index} actions must be a list", RunnerError)
        for action_index, action in enumerate(actions):
            _require(
                isinstance(action, Mapping),
                f"case {index} action {action_index} must be an object",
                RunnerError,
            )
    _require(
        not isinstance(required_operations, (str, bytes)) and required_operations is not None,
        "required operations must be an iterable of route names",
        RunnerError,
    )
    try:
        required = set(required_operations)
    except (TypeError, ValueError) as error:
        raise RunnerError("required operations must be an iterable of route names") from error
    _require(
        all(isinstance(operation, str) and bool(operation.strip()) for operation in required),
        "required operations must contain non-empty route names",
        RunnerError,
    )
    try:
        selected = [copy.deepcopy(dict(case)) for case in cases]
    except (RecursionError, TypeError, ValueError) as error:
        raise RunnerError(f"cases contain malformed or too deeply nested data: {error}") from error
    if required:
        covered = {
            action.get("route", action.get("operation"))
            for case in selected
            for action in case.get("actions", [])
            if isinstance(action, Mapping)
        }
        missing = sorted(required - covered)
        _require(
            not missing,
            "requested operations have no case coverage: " + ", ".join(missing),
        )
        selected = [
            case
            for case in selected
            if any(action.get("route", action.get("operation")) in required for action in case.get("actions", []))
        ]
    if not selected:
        target = ", ".join(sorted(required)) if required else "the requested comparison"
        raise RunnerError(f"zero relevant cases for {target}; refusing a vacuous comparison")
    return selected


def _matrix_row_binding(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return the immutable identity shape a readiness case must repeat."""

    _require(isinstance(row, Mapping), "readiness matrix row must be an object", RunnerError)
    _require(
        isinstance(row.get("id"), str) and bool(row["id"].strip()),
        "readiness matrix row lacks a stable id",
        RunnerError,
    )
    _require(
        isinstance(row.get("track"), str) and row["track"] in READINESS_TRACKS,
        f"matrix row {row.get('id')!r} has an invalid track",
        RunnerError,
    )
    _require(
        isinstance(row.get("operation"), str) and bool(row["operation"].strip()),
        f"matrix row {row.get('id')!r} lacks an operation",
        RunnerError,
    )
    _require(
        isinstance(row.get("identity"), str) and bool(row["identity"].strip()),
        f"matrix row {row.get('id')!r} lacks an identity profile",
        RunnerError,
    )
    _require(
        isinstance(row.get("oracle_kind"), str) and bool(row["oracle_kind"].strip()),
        f"matrix row {row.get('id')!r} lacks an oracle kind",
        RunnerError,
    )
    oracle = row.get("oracle")
    runs = row.get("runs")
    identity_evidence = row.get("identity_evidence")
    effect_receipt_state = row.get("effect_receipt_state")
    _require(
        isinstance(oracle, Mapping)
        and isinstance(runs, Mapping)
        and isinstance(identity_evidence, Mapping)
        and isinstance(effect_receipt_state, Mapping),
        f"matrix row {row.get('id')!r} has incomplete binding metadata",
        RunnerError,
    )
    _require(
        isinstance(oracle.get("case"), str) and bool(oracle["case"].strip()),
        f"matrix row {row.get('id')!r} lacks an oracle case",
        RunnerError,
    )
    _require(
        isinstance(runs.get("profile"), str) and bool(runs["profile"].strip()),
        f"matrix row {row.get('id')!r} lacks a run profile",
        RunnerError,
    )
    identity_requirements = row.get("identity_requirements")
    _require(
        isinstance(identity_requirements, Mapping),
        f"matrix row {row.get('id')!r} has malformed top-level identity requirements",
        RunnerError,
    )
    track_identity_requirements = row.get("track_identity_requirements")
    _require(
        isinstance(track_identity_requirements, Mapping),
        f"matrix row {row.get('id')!r} has malformed track identity requirements",
        RunnerError,
    )
    return {
        "matrix_row_id": row["id"],
        "track": row["track"],
        "profile": runs["profile"],
        "operation": row["operation"],
        "oracle_kind": row["oracle_kind"],
        "oracle_case": oracle["case"],
        "identity": row["identity"],
        "identity_evidence": copy.deepcopy(dict(identity_evidence)),
        "effect_receipt_state": copy.deepcopy(dict(effect_receipt_state)),
        "required_identity_fields": sorted(str(key) for key in identity_evidence),
        "required_evidence_fields": sorted(str(key) for key in effect_receipt_state),
        # Carry the frozen identity profile into the case binding itself.  A
        # row digest alone cannot prevent a fixture from silently dropping
        # track-specific Native/NCM/semantic requirements.
        "identity_requirements": copy.deepcopy(dict(identity_requirements)),
        "track_identity_requirements": copy.deepcopy(dict(track_identity_requirements)),
    }


# A matrix row without a reviewed callable route is explicitly unavailable.
# Keeping one route identity for these rows preserves the frozen reference and
# candidate route descriptions without allowing a convenient public route to
# masquerade as the missing operation.
MATRIX_UNAVAILABLE_ROUTE = "original_route_unavailable"

# The frozen matrix records the production operation in its own vocabulary,
# while the runner contract names the callable route.  Keep this translation
# runner-owned and deterministic: a readiness fixture cannot select one
# convenient route (for example, search) and merely relabel it as every row.
# Rows without a public route are required to carry an explicit
# ``matrix_route`` declaration and remain blocked until a corresponding
# external command adapter is supplied.
_MATRIX_ROUTE_BY_NATIVE_SUFFIX = {
    "commit": "fact_store_add",
    "add": "fact_store_add",
    "query_current": "fact_store_list",
    "query_current_by_id": "fact_store_get",
    "query_current_response": "fact_store_get",
    # These original operations have no reviewed public candidate route.  A
    # get/remove route is not a valid substitute merely because its response
    # shape looks similar.
    "query_as_of": MATRIX_UNAVAILABLE_ROUTE,
    "query_as_of_response": MATRIX_UNAVAILABLE_ROUTE,
    "query_lineage": MATRIX_UNAVAILABLE_ROUTE,
    "query_lineage_response": MATRIX_UNAVAILABLE_ROUTE,
    "retrieval_anchor": MATRIX_UNAVAILABLE_ROUTE,
    "purge_superseded": MATRIX_UNAVAILABLE_ROUTE,
    "list": "fact_store_list",
    "search": "fact_store_search",
    "probe": "fact_store_probe",
    "related": "fact_store_related",
    "reason": "fact_store_reason",
    "contradictions": "fact_store_contradict",
    "get": "fact_store_get",
    "history": MATRIX_UNAVAILABLE_ROUTE,
    "status": "memory_status",
    "update": "fact_store_update",
    "remove": "fact_store_remove",
    "supersede": "fact_store_supersede",
    "feedback": "fact_feedback",
}
_MATRIX_ROUTE_BY_PREFIX = {
    "native.session.temporal_query": "session_lookup",
    "native.session.task_lookup": MATRIX_UNAVAILABLE_ROUTE,
    "native.session.refresh_begin": "session_refresh_begin",
    "native.session.refresh_status": "session_refresh_status",
    "native.session.refresh_cancel": "session_refresh_cancel",
    "native.session.sessions_for": "sessions_for",
    "native.lcm.load_session": "lcm_load_session",
    "native.lcm.grep": "lcm_grep",
    "native.lcm.describe": "lcm_describe",
    "native.lcm.expand_query": "lcm_expand_query",
    "native.lcm.expand": "lcm_expand",
    "native.lcm.status": "lcm_status",
    "native.lcm.doctor": "lcm_doctor",
    "native.host.safety_extensions": MATRIX_UNAVAILABLE_ROUTE,
    "native.host.claude_journey": "host_sealed_source_admission",
    "native.host.codex_journey": "host_cursor_and_locator_regression",
}
def _matrix_row_contract_route(row: Mapping[str, Any]) -> str | None:
    """Return the frozen row's callable route when one is public and known."""

    _require(isinstance(row, Mapping), "readiness matrix row must be an object", RunnerError)
    row_id = row.get("id")
    if not isinstance(row_id, str) or not row_id.strip():
        return None
    direct = _MATRIX_ROUTE_BY_PREFIX.get(row_id)
    if direct is not None:
        return direct
    if row_id.startswith("native.fact."):
        suffix = row_id[len("native.fact."):]
        return _MATRIX_ROUTE_BY_NATIVE_SUFFIX.get(suffix)
    return None


def _case_matrix_binding(case: Mapping[str, Any]) -> Mapping[str, Any] | None:
    for key in ("matrix_binding", "readiness_binding"):
        value = case.get(key)
        if value is not None:
            return value if isinstance(value, Mapping) else {}
    return None


def _validate_readiness_case_binding(
    case: Mapping[str, Any],
    row: Mapping[str, Any],
) -> dict[str, Any]:
    """Reject a case that relabels any frozen matrix identity or evidence."""

    _require(isinstance(case, Mapping), "readiness case must be an object", RunnerError)
    _require(isinstance(row, Mapping), "readiness matrix row must be an object", RunnerError)
    actions = case.get("actions", ())
    _require(isinstance(actions, (list, tuple)), "readiness case actions must be a list", RunnerError)
    case_id = case.get("id", case.get("case_id"))
    binding = _case_matrix_binding(case)
    _require(
        isinstance(binding, Mapping),
        f"readiness case {case_id!r} must declare matrix_binding metadata",
        RunnerError,
    )
    expected = _matrix_row_binding(row)
    _require(
        set(binding) == set(expected),
        f"readiness case {case_id!r} matrix binding fields differ from the frozen row",
        RunnerError,
    )
    for key, expected_value in expected.items():
        actual = binding.get(key)
        if isinstance(expected_value, list):
            _require(
                isinstance(actual, list) and _json_semantic_equal(actual, expected_value),
                f"readiness case {case_id!r} relabels matrix {key}",
                RunnerError,
            )
        else:
            _require(
                _json_semantic_equal(actual, expected_value),
                f"readiness case {case_id!r} relabels matrix {key}: expected {expected_value!r}",
                RunnerError,
            )
    for label in ("matrix_row_id", "track", "profile", "operation", "oracle_kind", "oracle_case", "identity"):
        if label in case:
            _require(
                _json_semantic_equal(case[label], expected[label]),
                f"readiness case {case_id!r} relabels matrix {label}",
                RunnerError,
            )
    for action in actions:
        _require(isinstance(action, Mapping), f"readiness case {case_id!r} action must be an object", RunnerError)
        if not isinstance(action, Mapping):
            continue
        action_row_id = action.get("matrix_row_id")
        _require(
            isinstance(action_row_id, str) and action_row_id == row["id"],
            f"readiness case {case_id!r} action {action.get('id')} must bind its route to matrix row {row['id']!r}",
            RunnerError,
        )
        action_operation = action.get("matrix_operation")
        _require(
            isinstance(action_operation, str)
            and _json_semantic_equal(action_operation, expected["operation"]),
            f"readiness case {case_id!r} action {action.get('id')} must declare the frozen matrix operation",
            RunnerError,
        )
        action_oracle_case = action.get("matrix_oracle_case")
        _require(
            isinstance(action_oracle_case, str)
            and _json_semantic_equal(action_oracle_case, expected["oracle_case"]),
            f"readiness case {case_id!r} action {action.get('id')} must declare the frozen oracle case",
            RunnerError,
        )
        declared_route = action.get("route", action.get("operation"))
        _require(
            isinstance(declared_route, str) and bool(declared_route.strip()),
            f"readiness case {case_id!r} action {action.get('id')} must declare a callable route",
            RunnerError,
        )
        if "route" in action and "operation" in action:
            _require(
                action.get("route") == action.get("operation"),
                f"readiness case {case_id!r} action {action.get('id')} route and operation aliases disagree",
                RunnerError,
            )
        matrix_route = action.get("matrix_route")
        _require(
            isinstance(matrix_route, str) and matrix_route == declared_route,
            f"readiness case {case_id!r} action {action.get('id')} must bind its route through matrix_route",
            RunnerError,
        )
        expected_route = _matrix_row_contract_route(row)
        if expected_route is not None:
            _require(
                declared_route == expected_route,
                f"readiness case {case_id!r} action {action.get('id')} route {declared_route!r} does not implement frozen row {row['id']!r}; expected {expected_route!r}",
                RunnerError,
            )
            if expected_route == MATRIX_UNAVAILABLE_ROUTE:
                for field in ("matrix_candidate_route", "matrix_reference_route"):
                    _require(
                        isinstance(action.get(field), str)
                        and action[field] == row.get(
                            "candidate_route"
                            if field == "matrix_candidate_route"
                            else "reference_route"
                        ),
                        f"readiness case {case_id!r} action {action.get('id')} lacks the frozen {field} binding",
                        RunnerError,
                    )
        else:
            # NCM/semantic/release rows describe a production operation that
            # has no callable route in this contract.  Preserve the exact
            # frozen route descriptions so a future adapter can be reviewed.
            # Bind the attempt to the explicit unavailable route; a generic
            # public route (especially Search) must never masquerade as one of
            # these 161 no-callable matrix rows.
            _require(
                declared_route == MATRIX_UNAVAILABLE_ROUTE,
                f"readiness case {case_id!r} action {action.get('id')} has no callable frozen route; use {MATRIX_UNAVAILABLE_ROUTE!r} until an external adapter is reviewed",
                RunnerError,
            )
            for field in ("matrix_candidate_route", "matrix_reference_route"):
                _require(
                    isinstance(action.get(field), str)
                    and action[field] == row.get("candidate_route" if field == "matrix_candidate_route" else "reference_route"),
                    f"readiness case {case_id!r} action {action.get('id')} lacks the frozen {field} binding",
                    RunnerError,
                )
        for label in ("track", "profile", "oracle_kind", "oracle_case", "identity"):
            if label in action:
                _require(
                    _json_semantic_equal(action[label], expected[label]),
                    f"readiness case {case_id!r} action {action.get('id')} relabels matrix {label}",
                    RunnerError,
                )
    return dict(expected)


def validate_readiness_selection(
    cases: Sequence[Mapping[str, Any]],
    required_operations: Iterable[str] = (),
    *,
    readiness_mode: str = "comparison",
    readiness_matrix: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Select cases while requiring the complete frozen readiness matrix."""

    _require(isinstance(cases, (list, tuple)), "cases must be a list", RunnerError)
    _require(readiness_mode in READINESS_MODES, f"invalid readiness mode: {readiness_mode!r}")
    _require(
        not isinstance(required_operations, (str, bytes)) and required_operations is not None,
        "required operations must be an iterable of route names",
        RunnerError,
    )
    try:
        required = set(required_operations)
    except (TypeError, ValueError) as error:
        raise RunnerError("required operations must be an iterable of route names") from error
    if readiness_mode == "readiness" and required:
        raise RunnerError(
            "readiness evidence cannot use a filtered operation suite; "
            "run the complete accepted route set without --operation"
        )
    selected = select_relevant_cases(cases, required)
    if readiness_mode != "readiness":
        return selected
    matrix = _validated_readiness_matrix_input(readiness_matrix)
    validated = validate_frozen_readiness_matrix(matrix)
    rows_by_id = {str(row["id"]): row for row in validated["rows"]}
    required_row_ids = set(validated["row_ids"])
    covered: set[str] = set()
    seen_case_ids: set[str] = set()
    for case in selected:
        if not isinstance(case, Mapping):
            continue
        case_id = case.get("id", case.get("case_id"))
        if isinstance(case_id, str):
            _require(case_id not in seen_case_ids, f"duplicate readiness case id: {case_id}", RunnerError)
            seen_case_ids.add(case_id)
        case_row_ids = _case_matrix_row_ids(case) & required_row_ids
        if len(case_row_ids) == 0:
            raise RunnerError(
                f"readiness case {case.get('id', case.get('case_id'))!r} is not bound to a frozen matrix row"
            )
        if len(case_row_ids) > 1:
            raise RunnerError(
                f"readiness case {case.get('id', case.get('case_id'))!r} binds multiple matrix rows"
            )
        for row_id in case_row_ids:
            _validate_readiness_case_binding(case, rows_by_id[row_id])
        covered.update(case_row_ids)
        for action in case.get("actions", ()):
            if not isinstance(action, Mapping):
                continue
            action_row_ids = _case_matrix_row_ids(action) & required_row_ids
            if len(action_row_ids) > 1:
                raise RunnerError(
                    f"readiness case {case.get('id', case.get('case_id'))!r} action binds multiple matrix rows"
                )
            for action_row_id in action_row_ids:
                if case_row_ids and action_row_id not in case_row_ids:
                    raise RunnerError(
                        f"readiness case {case.get('id', case.get('case_id'))!r} action relabels matrix row"
                    )
            covered.update(action_row_ids)
    missing = sorted(required_row_ids - covered)
    if missing:
        raise RunnerError(
            "readiness evidence is incomplete; missing frozen matrix rows: "
            + ", ".join(missing)
        )
    return selected


def _readiness_attempt_population(
    matrix: Mapping[str, Any],
    case_results: Sequence[Mapping[str, Any]],
    *,
    ledger_path: Path | None = None,
    strict: bool | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Report whether executed ledger attempts meet every frozen row count.

    A readiness invocation currently receives one case result for each selected
    fixture.  The frozen matrix can require several attempts for a row, so the
    runner must make that shortfall visible instead of treating the planned
    count as if it had already been executed.  This helper counts immutable
    ledger rows and returns a typed ``blocked`` status until every required
    population is present.
    """

    _require(isinstance(matrix, Mapping), "readiness matrix must be an object", RunnerError)
    _require(isinstance(case_results, (list, tuple)), "case results must be a list", RunnerError)
    if ledger_path is not None:
        _require(isinstance(ledger_path, Path), "readiness ledger path must be a Path", RunnerError)
        _require(ledger_path.name == CANONICAL_ATTEMPT_LEDGER_NAME, "readiness ledger path must be attempts.jsonl", RunnerError)
    if run_id is not None:
        _require(isinstance(run_id, str) and bool(run_id.strip()), "readiness run_id must be non-empty", RunnerError)
    rows = matrix.get("rows")
    _require(isinstance(rows, (list, tuple)), "readiness matrix rows must be a list", RunnerError)
    required: dict[str, int] = {}
    frozen_rows: dict[str, Mapping[str, Any]] = {}
    for index, raw_row in enumerate(rows):
        _require(isinstance(raw_row, Mapping), f"readiness matrix row {index} must be an object", RunnerError)
        row_id = raw_row.get("id")
        runs = raw_row.get("runs")
        _require(
            isinstance(row_id, str) and bool(row_id.strip()),
            f"readiness matrix row {index} lacks a stable id",
            RunnerError,
        )
        _require(isinstance(runs, Mapping), f"readiness matrix row {row_id!r} lacks runs", RunnerError)
        planned = runs.get("planned")
        _require(
            isinstance(planned, int) and not isinstance(planned, bool) and planned > 0,
            f"readiness matrix row {row_id!r} has an invalid planned run count",
            RunnerError,
        )
        _require(row_id not in required, f"readiness matrix has duplicate row id: {row_id}", RunnerError)
        required[row_id] = planned
        frozen_rows[row_id] = raw_row

    # The accepted frozen matrix carries the full identity/evidence shape;
    # small unit fixtures intentionally exercise only row-count arithmetic.
    # Apply the strict ledger identity checks whenever matrix metadata is
    # present, while keeping those focused helper fixtures lightweight.
    inferred_strict_population = any(
        any(field in row for field in ("track", "operation", "oracle_kind", "identity_evidence", "effect_receipt_state"))
        for row in frozen_rows.values()
    )
    strict_population = inferred_strict_population if strict is None else bool(strict)
    canonical_path_required = strict is True
    if canonical_path_required:
        _require(
            isinstance(ledger_path, Path) and ledger_path.is_absolute(),
            "strict readiness population requires the absolute current run_dir/attempts.jsonl path",
            RunnerError,
        )
        _require(
            len(required) == FROZEN_READINESS_ROW_COUNT
            and sum(required.values()) == FROZEN_READINESS_PLANNED_ATTEMPTS,
            "strict readiness population requires the complete frozen 201-row/1381-attempt matrix",
            RunnerError,
        )
    observed: dict[str, int] = {row_id: 0 for row_id in required}
    unexpected: dict[str, int] = {}
    seen_attempt_ids: set[str] = set()
    seen_positions: set[tuple[str, int]] = set()
    seen_row_positions: set[tuple[str, int]] = set()
    seen_receipts: set[tuple[str, int, int, str]] = set()
    row_indexes: dict[str, set[int]] = {row_id: set() for row_id in required}
    for result in case_results:
        if not isinstance(result, Mapping):
            continue
        ledger_rows = result.get("attempt_ledger", ())
        if not isinstance(ledger_rows, Sequence) or isinstance(ledger_rows, (str, bytes)):
            if ledger_rows not in (None, (), []):
                unexpected["<malformed-attempt-ledger>"] = unexpected.get("<malformed-attempt-ledger>", 0) + 1
            continue
        if strict_population:
            receipt = result.get("attempt_ledger_receipt")
            outer_case_id = result.get("case_id")
            outer_id = result.get("id")
            # A missing/invalid receipt is the primary durable-evidence
            # failure.  Classify it before outer identity checks so callers
            # can distinguish an unpersisted attempt from a persisted row
            # whose case id was later relabelled.
            if not _valid_ledger_receipt(
                receipt,
                verify_file=True,
                expected_path=ledger_path if canonical_path_required else None,
                expected_case_id=None,
            ):
                unexpected["<unpersisted-attempt>"] = unexpected.get("<unpersisted-attempt>", 0) + len(ledger_rows)
                continue
            if (
                not isinstance(outer_case_id, str)
                or not outer_case_id.strip()
                or (outer_id is not None and outer_id != outer_case_id)
            ):
                unexpected["<case-id-mismatch>"] = unexpected.get("<case-id-mismatch>", 0) + 1
                continue
            # A result may carry exactly one attempt row.  Re-open the
            # canonical JSONL span and compare its parsed object to that row;
            # a producer supplied receipt tuple or a copied row cannot count
            # as observed population.
            if len(ledger_rows) != 1:
                unexpected["<receipt-row-cardinality>"] = unexpected.get("<receipt-row-cardinality>", 0) + 1
                continue
            receipt_row = _read_ledger_receipt_row(
                receipt,
                expected_path=ledger_path if canonical_path_required else None,
                expected_case_id=result.get("case_id") if isinstance(result.get("case_id"), str) else None,
            )
            receipt_key = (
                str(receipt.get("path")),
                int(receipt.get("offset")),
                int(receipt.get("bytes")),
                str(receipt.get("sha256")),
            )
            if receipt_key in seen_receipts or not isinstance(receipt_row, Mapping):
                unexpected["<duplicate-or-invalid-receipt>"] = unexpected.get("<duplicate-or-invalid-receipt>", 0) + 1
                continue
            seen_receipts.add(receipt_key)
            if not _json_semantic_equal(dict(receipt_row), dict(ledger_rows[0])):
                unexpected["<receipt-row-mismatch>"] = unexpected.get("<receipt-row-mismatch>", 0) + 1
                continue
            if not _ledger_row_shape_valid(
                ledger_rows[0],
                expected_case_id=outer_case_id,
            ):
                unexpected["<incomplete-ledger-row>"] = unexpected.get("<incomplete-ledger-row>", 0) + 1
                continue
        for ledger_row in ledger_rows:
            if not isinstance(ledger_row, Mapping):
                unexpected["<malformed-attempt>"] = unexpected.get("<malformed-attempt>", 0) + 1
                continue
            if strict_population:
                attempt_id = ledger_row.get("attempt_id")
                attempt_index = ledger_row.get("attempt_index")
                if (
                    not isinstance(attempt_id, str)
                    or not attempt_id.strip()
                    or not isinstance(attempt_index, int)
                    or isinstance(attempt_index, bool)
                    or attempt_index < 0
                ):
                    unexpected["<invalid-attempt-identity>"] = unexpected.get("<invalid-attempt-identity>", 0) + 1
                    continue
                position = (attempt_id, attempt_index)
                # Attempt indexes are row-local and must populate the frozen
                # range exactly once.  A large or negative index cannot be
                # used to inflate a readiness count.
                row_hint = ledger_row.get("row_id")
                if not isinstance(row_hint, str) or row_hint not in required or attempt_index >= required[row_hint]:
                    unexpected["<attempt-index-out-of-range>"] = unexpected.get("<attempt-index-out-of-range>", 0) + 1
                    continue
                outer_binding = result.get("runner_attempt_binding")
                # The run orchestrator supplies this outer binding for the
                # canonical frozen campaign.  Lightweight library callers
                # that exercise row-count arithmetic may omit it; once a run
                # id (or the canonical absolute ledger path) is supplied it
                # becomes mandatory and is compared byte-for-byte with the
                # nested row binding.
                outer_binding_required = run_id is not None or canonical_path_required
                if outer_binding_required and (
                    not isinstance(outer_binding, Mapping)
                    or outer_binding.get("run_id") != run_id
                    or outer_binding.get("row_id") != row_hint
                    or outer_binding.get("case_id") != row_hint
                    or outer_binding.get("attempt_id") != attempt_id
                    or outer_binding.get("attempt_index") != attempt_index
                    or not _json_semantic_equal(ledger_row.get("runner_attempt_binding"), outer_binding)
                ):
                    unexpected["<outer-attempt-binding>"] = unexpected.get("<outer-attempt-binding>", 0) + 1
                    continue
                if attempt_id in seen_attempt_ids or position in seen_positions:
                    unexpected["<duplicate-attempt-identity>"] = unexpected.get("<duplicate-attempt-identity>", 0) + 1
                    continue
                seen_attempt_ids.add(attempt_id)
                seen_positions.add(position)
                outcome = ledger_row.get("outcome")
                if not isinstance(outcome, str) or outcome not in OUTCOME_STATUSES:
                    unexpected["<invalid-attempt-outcome>"] = unexpected.get("<invalid-attempt-outcome>", 0) + 1
                    continue
                if "result_digest" not in ledger_row or ledger_row.get("result_digest") is not None:
                    unexpected["<invalid-attempt-result-digest>"] = unexpected.get("<invalid-attempt-result-digest>", 0) + 1
                    continue
                if not _ledger_row_shape_valid(
                    ledger_row,
                    expected_case_id=result.get("case_id") if isinstance(result.get("case_id"), str) else None,
                ):
                    unexpected["<incomplete-ledger-row>"] = unexpected.get("<incomplete-ledger-row>", 0) + 1
                    continue
            matrix_row = ledger_row.get("matrix_row")
            row_id = matrix_row.get("id") if isinstance(matrix_row, Mapping) else None
            declared_row_id = ledger_row.get("row_id")
            if strict_population and (not isinstance(matrix_row, Mapping) or not isinstance(row_id, str) or not row_id.strip()):
                unexpected["<missing-matrix-row>"] = unexpected.get("<missing-matrix-row>", 0) + 1
                continue
            if isinstance(row_id, str) and isinstance(declared_row_id, str) and row_id != declared_row_id:
                key = f"conflict:{row_id}:{declared_row_id}"
                unexpected[key] = unexpected.get(key, 0) + 1
                continue
            if strict_population and (not isinstance(declared_row_id, str) or not declared_row_id.strip()):
                unexpected["<missing-row-id>"] = unexpected.get("<missing-row-id>", 0) + 1
                continue
            if not isinstance(row_id, str):
                row_id = declared_row_id
            if row_id in observed:
                row_position = (row_id, attempt_index) if strict_population else None
                if row_position is not None and row_position in seen_row_positions:
                    unexpected["<duplicate-row-attempt-position>"] = unexpected.get("<duplicate-row-attempt-position>", 0) + 1
                    continue
                if row_position is not None:
                    seen_row_positions.add(row_position)
                frozen_row = frozen_rows[row_id]
                if strict_population:
                    if result.get("case_id") != row_id:
                        unexpected[f"{row_id}:case_id_mismatch"] = unexpected.get(f"{row_id}:case_id_mismatch", 0) + 1
                        continue
                    metadata_error = False
                    expected_metadata = {
                        "track": frozen_row.get("track"),
                        "operation": frozen_row.get("operation"),
                        "oracle_kind": frozen_row.get("oracle_kind"),
                        "oracle_case": (
                            frozen_row.get("oracle", {}).get("case")
                            if isinstance(frozen_row.get("oracle"), Mapping)
                            else None
                        ),
                        "identity": frozen_row.get("identity"),
                        "identity_evidence": frozen_row.get("identity_evidence"),
                        "effect_receipt_state": frozen_row.get("effect_receipt_state"),
                        "profile": (
                            frozen_row.get("runs", {}).get("profile")
                            if isinstance(frozen_row.get("runs"), Mapping)
                            else None
                        ),
                        "planned_runs": (
                            frozen_row.get("runs", {}).get("planned")
                            if isinstance(frozen_row.get("runs"), Mapping)
                            else None
                        ),
                    }
                    # The frozen campaign carries matrix-wide identity
                    # requirements and therefore requires both fields on
                    # every attempt.  Small unit fixtures used by callers of
                    # this population helper may intentionally model only
                    # row/run counts; those fixtures have no identity
                    # authority to compare and are still useful for testing
                    # duplicate/over-count handling.
                    matrix_identity_requirements = matrix.get("identity_requirements")
                    if isinstance(matrix_identity_requirements, Mapping):
                        expected_metadata.update(
                            {
                                "identity_requirements": matrix_identity_requirements,
                                "track_identity_requirements": matrix_identity_requirements.get(
                                    frozen_row.get("track"), {}
                                ),
                            }
                        )
                    for field, expected_value in expected_metadata.items():
                        if (
                            not isinstance(matrix_row, Mapping)
                            or field not in matrix_row
                            or not _json_semantic_equal(matrix_row[field], expected_value)
                        ):
                            unexpected[f"{row_id}:matrix_{field}"] = unexpected.get(f"{row_id}:matrix_{field}", 0) + 1
                            metadata_error = True
                            break
                    if metadata_error:
                        continue
                if strict_population:
                    row_indexes.setdefault(row_id, set()).add(attempt_index)
                observed[row_id] += 1
            else:
                unexpected[row_id] = unexpected.get(row_id, 0) + 1

    over_counted: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    missing: dict[str, int] = {}
    for row_id in sorted(required):
        required_runs = required[row_id]
        observed_runs = observed[row_id]
        missing_runs = max(required_runs - observed_runs, 0)
        if missing_runs:
            missing[row_id] = missing_runs
        over_counted_runs = max(observed_runs - required_runs, 0)
        if over_counted_runs:
            over_counted[row_id] = over_counted_runs
        rows.append(
            {
                "id": row_id,
                "required_runs": required_runs,
                "observed_runs": observed_runs,
                "missing_runs": missing_runs,
                "over_counted_runs": over_counted_runs,
                "status": (
                    "over_counted"
                    if over_counted_runs
                    else "complete"
                    if missing_runs == 0
                    else "blocked"
                ),
            }
        )
    required_total = sum(required.values())
    observed_total = sum(observed.values())
    if strict_population:
        for row_id, expected_runs in required.items():
            expected_indexes = set(range(expected_runs))
            actual_indexes = row_indexes.get(row_id, set())
            if actual_indexes != expected_indexes:
                unexpected[f"{row_id}:attempt_index_set"] = {
                    "expected": sorted(expected_indexes),
                    "observed": sorted(actual_indexes),
                    "missing": sorted(expected_indexes - actual_indexes),
                    "extra": sorted(actual_indexes - expected_indexes),
                }
    complete = bool(required) and not missing and not unexpected and not over_counted
    return {
        "status": "complete" if complete else "blocked",
        "complete": complete,
        "required_rows": len(required),
        "required_attempts": required_total,
        "observed_attempts": observed_total,
        "missing_attempts": sum(missing.values()),
        "required_runs": required,
        "observed_runs": observed,
        "missing_runs": missing,
        "unexpected_rows": unexpected,
        "over_counted_runs": over_counted,
        "rows": rows,
        "strict": strict_population,
        "ledger_path": str(ledger_path) if isinstance(ledger_path, Path) else None,
        "run_id": run_id,
    }


def _case_matrix_row_ids(value: Mapping[str, Any]) -> set[str]:
    """Collect explicit matrix row identities from a case or action."""

    _require(isinstance(value, Mapping), "readiness case/action must be an object", RunnerError)
    covered: set[str] = set()
    for field in ("matrix_row_id", "row_id"):
        candidate = value.get(field)
        if isinstance(candidate, str):
            covered.add(candidate)
        elif isinstance(candidate, (list, tuple)):
            _require(
                all(isinstance(item, str) and bool(item.strip()) for item in candidate),
                f"readiness {field} entries must be non-empty strings",
                RunnerError,
            )
            covered.update(candidate)
        elif candidate is not None:
            _require(False, f"readiness {field} must be a string or list of strings", RunnerError)
    binding = value.get("matrix_binding", value.get("readiness_binding"))
    if isinstance(binding, Mapping) and isinstance(binding.get("matrix_row_id"), str):
        covered.add(binding["matrix_row_id"])
    case_id = value.get("id", value.get("case_id"))
    if isinstance(case_id, str):
        covered.add(case_id)
    return covered


def _render(value: Any, context: Mapping[str, Any]) -> Any:
    _require(isinstance(context, Mapping), "placeholder context must be an object", RunnerError)
    if isinstance(value, str):
        matches = list(_PLACEHOLDER.finditer(value))
        if not matches:
            return value
        rendered = value
        for match in matches:
            key = match.group(1) or match.group(2)
            current: Any = context
            for part in key.split("."):
                if not isinstance(current, Mapping) or part not in current:
                    current = _MISSING
                    break
                current = current[part]
            if current is _MISSING:
                raise ContractError(f"unknown case placeholder: {key}")
            if len(matches) == 1 and match.group(0) == value:
                return copy.deepcopy(current)
            rendered = rendered.replace(match.group(0), str(current))
        return rendered
    if isinstance(value, list):
        return [_render(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _render(item, context) for key, item in value.items()}
    return value


def _redact_render_secrets(value: Any, *, _depth: int = 0) -> Any:
    """Remove runner authentication/protocol evidence from child templates."""

    if _depth > 64:
        raise ContractError("render context is too deeply nested")
    if isinstance(value, Mapping):
        result: dict[Any, Any] = {}
        for key, nested in value.items():
            if isinstance(key, str) and key.lower() in _RENDER_SECRET_KEYS:
                continue
            result[key] = _redact_render_secrets(nested, _depth=_depth + 1)
        return result
    if isinstance(value, list):
        return [_redact_render_secrets(item, _depth=_depth + 1) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_render_secrets(item, _depth=_depth + 1) for item in value)
    return copy.deepcopy(value)


_SIDE_PLACEHOLDER = re.compile(r"(?:\$\{side\}|\{side\})", re.IGNORECASE)

# A daemon action receipt is scoped by the authority's selected project/store
# before ``prepare``.  Accepting a caller-selected session or thread id would
# let a case change the logical request between sides (or claim a scope the
# daemon did not select).  Until the daemon exposes a pre-prepare selected
# scope, receipt-backed readiness actions fail closed on all common spellings.
_RECEIPT_SCOPED_SELECTOR_KEYS = frozenset(
    {
        "session",
        "sessionid",
        "thread",
        "threadid",
        "conversation",
        "conversationid",
        "providersessionid",
        "agentsessionid",
        "tasksessionid",
    }
)


def _normalized_selector_key(key: Any) -> str | None:
    if not isinstance(key, str):
        return None
    return re.sub(r"[^a-z0-9]", "", key.lower())


def _reject_receipt_scoped_selectors(value: Any, path: str = "$", depth: int = 0) -> None:
    """Reject caller-selected session scope before a daemon action prepare."""

    if depth > 64:
        raise ContractError(f"receipt-scoped request at {path} is too deeply nested")
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = _normalized_selector_key(key)
            _require(
                normalized not in _RECEIPT_SCOPED_SELECTOR_KEYS,
                (
                    f"receipt-backed action cannot accept caller-selected scope at {path}.{key}; "
                    "the daemon-selected scope must be obtained before prepare"
                ),
                ContractError,
            )
            _reject_receipt_scoped_selectors(nested, f"{path}.{key}", depth + 1)
        return
    if isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _reject_receipt_scoped_selectors(nested, f"{path}/{index}", depth + 1)


def _reject_side_dependent_logical_input(value: Any, path: str = "$", depth: int = 0) -> None:
    """Reject logical requests whose bytes differ only because of side labels."""

    if depth > 64:
        raise ContractError(f"logical input at {path} is too deeply nested")
    if isinstance(value, str):
        _require(
            _SIDE_PLACEHOLDER.search(value) is None,
            f"side-dependent logical input is not allowed at {path}; use one normalized request for both sides",
            ContractError,
        )
        return
    if isinstance(value, Mapping):
        for key, nested in value.items():
            _reject_side_dependent_logical_input(nested, f"{path}.{key}", depth + 1)
        return
    if isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _reject_side_dependent_logical_input(nested, f"{path}/{index}", depth + 1)


def _pointer_parts(pointer: str) -> list[str]:
    _require(isinstance(pointer, str), "JSON pointer must be a string", ContractError)
    if pointer == "":
        return []
    _require(pointer.startswith("/"), f"JSON pointer must start with '/': {pointer}")
    raw_parts = pointer[1:].split("/")
    _require(len(raw_parts) <= 64, "JSON pointer is too deeply nested", ContractError)
    _require(len(pointer) <= 8192, "JSON pointer is too long", ContractError)
    for part in raw_parts:
        _require(
            not re.search(r"~(?![01])", part),
            f"JSON pointer contains an invalid escape: {pointer}",
            ContractError,
        )
    return [part.replace("~1", "/").replace("~0", "~") for part in raw_parts]


def _list_index(part: str, length: int) -> int | None:
    """Resolve the RFC 6901 array index spelling without coercing bad keys."""

    if not isinstance(part, str) or not part.isdigit():
        return None
    if len(part) > 1 and part.startswith("0"):
        return None
    try:
        index = int(part)
    except (TypeError, ValueError, OverflowError) as error:
        raise RunnerError("JSON pointer array index is invalid or too large") from error
    return index if index < length else None


def json_pointer(value: Any, pointer: str) -> Any:
    current = value
    for part in _pointer_parts(pointer):
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list):
            index = _list_index(part, len(current))
            if index is None:
                return _MISSING
            current = current[index]
        else:
            return _MISSING
    return current


def _drop_pointer(value: Any, pointer: str) -> None:
    parts = _pointer_parts(pointer)
    if not parts:
        return
    current = value
    for part in parts[:-1]:
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list):
            index = _list_index(part, len(current))
            if index is None:
                return
            current = current[index]
        else:
            return
    final = parts[-1]
    if isinstance(current, dict):
        current.pop(final, None)
    elif isinstance(current, list):
        index = _list_index(final, len(current))
        if index is not None:
            current.pop(index)


def _replace_pointer(value: Any, pointer: str, replacement: Any) -> bool:
    parts = _pointer_parts(pointer)
    if not parts:
        return False
    current = value
    for part in parts[:-1]:
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list):
            index = _list_index(part, len(current))
            if index is None:
                return False
            current = current[index]
        else:
            return False
    final = parts[-1]
    if isinstance(current, dict) and final in current:
        current[final] = replacement
        return True
    if isinstance(current, list):
        index = _list_index(final, len(current))
        if index is not None:
            current[index] = replacement
            return True
    return False


class _Missing:
    pass


_MISSING = _Missing()


def semantic_projection(
    response: Any,
    comparison: Mapping[str, Any] | None = None,
    *,
    side: str | None = None,
) -> Any:
    _require(comparison is None or isinstance(comparison, Mapping), "comparison must be an object", RunnerError)
    comparison = comparison or {}
    paths = comparison.get("semantic_json_pointers", ())
    ignored = comparison.get("ignore_json_pointers", ())
    _require(isinstance(paths, (list, tuple)), "semantic_json_pointers must be a list")
    _require(isinstance(ignored, (list, tuple)), "ignore_json_pointers must be a list")
    for pointer in (*paths, *ignored):
        _require(isinstance(pointer, str), "comparison JSON pointers must be strings")
        _pointer_parts(pointer)

    # Apply reviewed nondeterminism rules to a private copy of the complete
    # response before selecting semantic paths.  This keeps pointers rooted
    # in the actual production response even when a case chooses a sparse
    # projection, and leaves the raw response untouched in result.json.
    try:
        source = copy.deepcopy(response)
    except (RecursionError, TypeError, ValueError) as error:
        raise RunnerError(f"semantic response is malformed or too deeply nested: {error}") from error
    for pointer in ignored:
        _drop_pointer(source, pointer)

    # A case may explicitly identify values that are expected to differ
    # because the two stores were seeded independently (for example, a
    # generated fact id).  Replace only the declared response paths with a
    # symbolic identity token; all other fields, including score/order,
    # receipts and omissions, remain exact comparison material.
    identity_mappings = comparison.get("identity_mappings", ())
    _require(isinstance(identity_mappings, (list, tuple)), "identity_mappings must be a list", RunnerError)
    for mapping in identity_mappings:
        _require(isinstance(mapping, Mapping), "identity mapping must be an object", RunnerError)
        _require(
            isinstance(mapping.get("name"), str) and bool(mapping.get("name")),
            "identity mapping name must be a non-empty string",
            RunnerError,
        )
        _require(isinstance(mapping.get("original_pointer"), str), "identity mapping original_pointer must be a string", RunnerError)
        _require(isinstance(mapping.get("product_pointer"), str), "identity mapping product_pointer must be a string", RunnerError)
        _pointer_parts(mapping["original_pointer"])
        _pointer_parts(mapping["product_pointer"])
        if side not in SIDE_NAMES:
            continue
        pointer = mapping["original_pointer"] if side == "original" else mapping["product_pointer"]
        _replace_pointer(source, pointer, {"identity": mapping["name"]})
    if paths:
        result = {}
        for pointer in paths:
            item = json_pointer(source, pointer)
            result[pointer] = {"missing": True} if item is _MISSING else copy.deepcopy(item)
    else:
        result = source
    return result


def _json_semantic_equal(left: Any, right: Any, _depth: int = 0) -> bool:
    """Compare JSON values without Python's bool-as-int coercion."""

    if _depth > 64:
        raise RunnerError("JSON comparison value is too deeply nested")
    if type(left) is not type(right):
        return False
    if isinstance(left, Mapping):
        if set(left) != set(right):
            return False
        return all(_json_semantic_equal(left[key], right[key], _depth + 1) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _json_semantic_equal(a, b, _depth + 1) for a, b in zip(left, right)
        )
    return left == right


@dataclass(frozen=True)
class ProcessTimeouts:
    action_seconds: float = 120.0
    terminate_seconds: float = 2.0
    kill_seconds: float = 2.0

    def __post_init__(self) -> None:
        try:
            values = tuple(asdict(self).values())
            invalid = any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0
                for value in values
            )
        except (TypeError, ValueError, OverflowError) as error:
            raise RunnerError("process timeouts must be positive finite numbers") from error
        if invalid:
            raise RunnerError("process timeouts must be positive finite numbers")


def _daemon_socket_path(side_dir: Path) -> Path:
    """Return a private, bounded-length endpoint for one comparison side."""

    _require(isinstance(side_dir, Path), "daemon side directory must be a Path", RunnerError)
    candidate = side_dir / "runtime" / "daemon.sock"
    # sockaddr_un is shorter on some Unix variants than Linux's 108-byte
    # limit.  Keep enough headroom for all supported runners, while retaining
    # the readable side-local path for normal checkouts.
    if os.name == "nt" or len(os.fsencode(str(candidate))) < 80:
        return candidate
    digest = hashlib.sha256(str(side_dir.resolve()).encode("utf-8")).hexdigest()[:16]
    # Keep the socket under a runner-created private directory.  A socket
    # directly under /tmp would be rejected by the daemon's private-parent
    # check on Linux, even though it is short enough for sockaddr_un.
    basename = f"tdno-{digest}"
    # Do not honor caller-controlled TMPDIR for an authority endpoint.  The
    # child environment is scrubbed separately, but the runner's own socket
    # parent must also remain on the fixed local temporary filesystem.
    temp_root = Path("C:/Windows/Temp") if os.name == "nt" else Path("/tmp")
    candidate = temp_root / basename / "daemon.sock"
    if len(os.fsencode(str(candidate))) >= 80:
        candidate = Path("/tmp") / basename / "daemon.sock"
    return candidate


def _prepare_private_socket_parent(parent: Path) -> None:
    """Create and validate the short Unix socket directory used by a side."""

    _require(isinstance(parent, Path), "daemon socket parent must be a Path", RunnerError)
    try:
        parent.mkdir(mode=0o700, parents=False, exist_ok=True)
        info = parent.lstat()
    except OSError as error:
        raise RunnerError(f"cannot prepare private daemon socket directory {parent}: {error}") from error
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise RunnerError(f"daemon socket parent is not a directory: {parent}")
    if hasattr(os, "getuid") and info.st_uid != os.getuid():
        raise RunnerError(f"daemon socket parent is not owned by the runner user: {parent}")
    if stat.S_IMODE(info.st_mode) & 0o077:
        try:
            parent.chmod(0o700)
            info = parent.lstat()
        except OSError as error:
            raise RunnerError(f"cannot restrict daemon socket directory {parent}: {error}") from error
        if stat.S_IMODE(info.st_mode) & 0o077:
            raise RunnerError(f"daemon socket directory is not private: {parent}")


def _daemon_authority_path(profile_root: Path) -> Path:
    _require(isinstance(profile_root, Path), "daemon profile root must be a Path", RunnerError)
    if os.name == "nt":
        return profile_root / "daemon-authority" / "daemon-authority.json"
    return profile_root / "daemon-authority.json"


def _file_ref(path: Path) -> dict[str, Any]:
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
            size += len(chunk)
    return {"path": str(path), "bytes": size, "sha256": hasher.hexdigest()}


def _endpoint_matches(endpoint: Any, socket_path: Path) -> bool:
    if not isinstance(socket_path, Path):
        return False
    if os.name != "nt":
        if not (
            isinstance(endpoint, Mapping)
            and endpoint.get("kind") == "unix"
            and isinstance(endpoint.get("address"), str)
        ):
            return False
        try:
            address = Path(endpoint["address"])
            return address.is_absolute() and address.resolve() == socket_path.resolve()
        except (OSError, RuntimeError, TypeError, ValueError):
            return False
    return isinstance(endpoint, Mapping) and endpoint.get("kind") == "loopback"


def _endpoint_connectable(endpoint: Any) -> bool:
    if not isinstance(endpoint, Mapping):
        return False
    kind, address = endpoint.get("kind"), endpoint.get("address")
    try:
        if kind == "unix" and os.name != "nt" and isinstance(address, str):
            import socket

            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
                stream.settimeout(0.25)
                stream.connect(address)
            return True
        if kind == "loopback" and isinstance(address, str):
            import socket

            host, port = address.rsplit(":", 1)
            with socket.create_connection((host.strip("[]"), int(port)), timeout=0.25):
                return True
    except (OSError, ValueError):
        return False
    return False


def _owned_daemon_rpc_exchange(
    daemon: Any,
    request: Mapping[str, Any],
    *,
    timeout_seconds: float,
    label: str,
) -> tuple[dict[str, Any], bytes, bytes, dict[str, Any]]:
    """Exchange one authenticated JSON-RPC request with an owned daemon.

    The preparation receipt protocol is deliberately separate from child
    stdout.  This helper owns the socket, framing, and authority token, and
    returns only hashes of the wire bytes to its callers.  In particular, a
    proof key included in a preparation request never enters a case result or
    a render context.
    """

    _require(isinstance(request, Mapping), f"{label} request must be an object", RunnerError)
    _require(
        isinstance(timeout_seconds, (int, float))
        and not isinstance(timeout_seconds, bool)
        and math.isfinite(float(timeout_seconds))
        and float(timeout_seconds) > 0,
        f"{label} timeout must be positive and finite",
        RunnerError,
    )
    identity = daemon.current_identity() if daemon is not None else None
    _require(isinstance(identity, Mapping), f"owned daemon identity disappeared before {label}", RunnerError)
    authority = getattr(daemon, "authority", None)
    _require(isinstance(authority, Mapping), f"owned daemon authority record is unavailable before {label}", RunnerError)
    token = authority.get("auth_token")
    _require(
        isinstance(token, str) and re.fullmatch(r"[0-9a-fA-F]{64}", token) is not None,
        f"owned daemon authority token is malformed before {label}",
        RunnerError,
    )
    handshake_id = request.get("id")
    _require(isinstance(handshake_id, str) and handshake_id, f"{label} request id is invalid", RunnerError)
    params = request.get("params")
    _require(isinstance(params, Mapping), f"{label} request params must be an object", RunnerError)
    metadata = params.get("_meta")
    _require(isinstance(metadata, Mapping), f"{label} request metadata must be an object", RunnerError)
    prepare_meta = metadata.get("nativeOriginalActionPrepare")
    _require(
        isinstance(prepare_meta, Mapping)
        and isinstance(prepare_meta.get("scope"), str)
        and bool(prepare_meta.get("scope", "").strip()),
        f"{label} request is missing its runner-owned action scope",
        RunnerError,
    )
    handshake = {
        # Bind the authenticated connection to the same project scope that
        # the daemon will check when preparing the action.  A profile-only
        # handshake would leave project routing unspecified and could produce
        # an acknowledgement from the wrong mounted store.
        "project_path": prepare_meta["scope"],
        "scope_prefix": None,
        "timings": False,
        "allow_init": False,
        "allow_initialize_root_routing": False,
        "client_identity": {
            "profile_root": str(daemon.profile_root),
            "global_db_path": str(daemon.profile_root / "global.db"),
        },
        "client_version": "native-original-runner",
        "client_instance_id": handshake_id,
        "tool_list_changed_capable": False,
        "catalog_version": "",
        "moved_store_adoption": "never",
    }
    preface = {"protocol": "tracedecay-daemon-v1", "auth_token": token}
    request_bytes = b"".join(
        _json_bytes(item) + b"\n" for item in (preface, handshake, request)
    )
    response_bytes = bytearray()
    try:
        import socket

        endpoint = identity.get("endpoint")
        if isinstance(endpoint, Mapping) and endpoint.get("kind") == "loopback":
            address = endpoint.get("address")
            _require(
                isinstance(address, str) and ":" in address,
                f"owned daemon loopback endpoint is malformed for {label}",
                RunnerError,
            )
            host, port_text = address.rsplit(":", 1)
            stream = socket.create_connection(
                (host.strip("[]"), int(port_text)), timeout=float(timeout_seconds)
            )
        else:
            stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            stream.settimeout(float(timeout_seconds))
            stream.connect(str(daemon.socket_path))
        with stream:
            stream.sendall(request_bytes)
            try:
                stream.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            while len(response_bytes) <= 16 * 1024 * 1024:
                chunk = stream.recv(64 * 1024)
                if not chunk:
                    break
                response_bytes.extend(chunk)
                if len(response_bytes) > 16 * 1024 * 1024:
                    break
    except RunnerError:
        raise
    except (OSError, ValueError, TypeError) as error:
        raise RunnerError(f"owned daemon {label} transport failed: {error}") from error
    wire = bytes(response_bytes)
    _require(
        wire.strip() and wire.endswith(b"\n") and wire.count(b"\n") == 1,
        f"owned daemon {label} must return exactly one newline-terminated response",
        RunnerError,
    )
    try:
        response = _strict_json_loads(wire.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError) as error:
        raise RunnerError(f"owned daemon {label} returned malformed JSON: {error}") from error
    _require(
        isinstance(response, Mapping)
        and response.get("jsonrpc") == "2.0"
        and response.get("id") == handshake_id
        and "method" not in response
        and "error" not in response
        and isinstance(response.get("result"), Mapping),
        f"owned daemon {label} response was not one JSON-RPC success result",
        RunnerError,
    )
    return (
        dict(response),
        request_bytes,
        wire,
        copy.deepcopy(dict(identity)),
    )


class _OwnedDaemon:
    """Own one foreground daemon and retain identity/cleanup evidence."""

    def __init__(
        self,
        *,
        side: str,
        binary: Path,
        side_dir: Path,
        profile_root: Path,
        process_root: Path,
        socket_path: Path,
        authority_path: Path,
        artifact_dir: Path,
        process: subprocess.Popen[bytes],
        stdout_file: Any,
        stderr_file: Any,
        started_wall: int,
        started_monotonic: int,
    ) -> None:
        self.side = side
        self.binary = binary
        self.binary_sha256 = verify_binary(binary, side)["sha256"]
        self.side_dir = side_dir
        self.profile_root = profile_root
        self.process_root = process_root
        self.socket_path = socket_path
        self.authority_path = authority_path
        self.artifact_dir = artifact_dir
        self.process = process
        self._stdout_file = stdout_file
        self._stderr_file = stderr_file
        self.started_wall = started_wall
        self.started_monotonic = started_monotonic
        self.authority: dict[str, Any] | None = None
        self.identity: dict[str, Any] | None = None
        self.cleanup: dict[str, Any] | None = None

    @classmethod
    def start(
        cls,
        *,
        side: str,
        binary: Path,
        side_dir: Path,
        profile_root: Path,
        artifact_dir: Path,
        timeouts: ProcessTimeouts,
    ) -> "_OwnedDaemon":
        profile_root = profile_root.resolve()
        process_root = (side_dir / "process").resolve()
        socket_path = _daemon_socket_path(side_dir)
        authority_path = _daemon_authority_path(profile_root)
        profile_root.mkdir(parents=True, exist_ok=True)
        process_root.mkdir(parents=True, exist_ok=True)
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        _prepare_private_socket_parent(socket_path.parent)
        if _endpoint_connectable({"kind": "unix", "address": str(socket_path)}) if os.name != "nt" else False:
            raise RunnerError(f"{side} daemon endpoint is already connectable: {socket_path}")
        if os.name != "nt" and socket_path.exists():
            try:
                socket_path.unlink()
            except OSError as error:
                raise RunnerError(f"cannot remove stale {side} daemon socket {socket_path}: {error}") from error

        artifact_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = artifact_dir / "stdout.bin"
        stderr_path = artifact_dir / "stderr.bin"
        stdout_file = stdout_path.open("wb")
        stderr_file = stderr_path.open("wb")
        environment = _environment(
            side_dir,
            {},
            {
                "profile_root": str(profile_root),
                "process_root": str(process_root),
                "daemon_socket": str(socket_path),
            },
        )
        argv = [
            str(binary),
            "daemon",
            "run",
            "--socket",
            str(socket_path),
            "--profile-root",
            str(profile_root),
        ]
        kwargs: dict[str, Any] = {
            "args": argv,
            "cwd": str(side_dir),
            "env": environment,
            "stdin": subprocess.DEVNULL,
            "stdout": stdout_file,
            "stderr": stderr_file,
        }
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            kwargs["start_new_session"] = True
        started_wall = time.time_ns()
        started_monotonic = time.monotonic_ns()
        try:
            process = subprocess.Popen(**kwargs)
        except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError) as error:
            stdout_file.close()
            stderr_file.close()
            raise RunnerError(f"{side} foreground daemon could not be spawned: {error}") from error
        daemon = cls(
            side=side,
            binary=binary,
            side_dir=side_dir,
            profile_root=profile_root,
            process_root=process_root,
            socket_path=socket_path,
            authority_path=authority_path,
            artifact_dir=artifact_dir,
            process=process,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            started_wall=started_wall,
            started_monotonic=started_monotonic,
        )
        try:
            daemon._wait_ready(timeouts.action_seconds)
        except BaseException:
            daemon.stop(timeouts, reason="startup failure")
            raise
        return daemon

    def _wait_ready(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        last_error = "authority record not published"
        while time.monotonic() < deadline:
            returncode = self.process.poll()
            if returncode is not None:
                self._close_logs()
                stderr = self._read_log_tail(self.artifact_dir / "stderr.bin")
                raise RunnerError(
                    f"{self.side} foreground daemon exited before readiness ({returncode}): {stderr}"
                )
            try:
                authority_bytes, _ = _read_stable_file(
                    self.authority_path,
                    label=f"{self.side} daemon authority record",
                )
                record = _strict_json_loads(authority_bytes.decode("utf-8", errors="strict"))
                if (
                    isinstance(record, Mapping)
                    and isinstance(record.get("endpoint"), Mapping)
                    and record.get("pid") == self.process.pid
                    and _endpoint_matches(record.get("endpoint"), self.socket_path)
                    and isinstance(record.get("profile_root"), str)
                    and bool(record["profile_root"].strip())
                    and Path(record["profile_root"]).resolve() == self.profile_root
                    and isinstance(record.get("process_run_id"), str)
                    and bool(record["process_run_id"])
                    and isinstance(record.get("epoch"), int)
                    and not isinstance(record["epoch"], bool)
                    and record["epoch"] > 0
                    and isinstance(record.get("version"), str)
                    and bool(record["version"])
                    and isinstance(record.get("auth_token"), str)
                    and re.fullmatch(r"[0-9a-fA-F]{64}", record["auth_token"]) is not None
                ):
                    if _endpoint_connectable(record.get("endpoint")):
                        self.authority = dict(record)
                        self.identity = self._identity(record)
                        return
                    last_error = "authority identity published but endpoint is not connectable"
                else:
                    last_error = "authority identity did not match the owned process/profile/endpoint"
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError, AttributeError, RuntimeError, RunnerError) as error:
                last_error = f"authority record is not ready: {error}"
            time.sleep(0.05)
        raise RunnerError(f"{self.side} foreground daemon readiness timed out: {last_error}")

    def issue_authority_challenge(
        self,
        challenge: str,
        *,
        timeout_seconds: float = 2.0,
    ) -> dict[str, Any]:
        """Ask this owned daemon to answer a runner-issued wire challenge.

        The challenge response is deliberately obtained from a fresh,
        authenticated daemon connection.  It is never derived from the
        authority token in runner code and it is never placed in the child
        render context/environment.  The returned hashes authenticate the
        exact request/response bytes observed on that socket.
        """

        _require(
            isinstance(challenge, str)
            and re.fullmatch(r"[0-9a-fA-F]{64}", challenge) is not None,
            "daemon authority challenge must be a 64-character hex nonce",
            RunnerError,
        )
        _require(
            isinstance(timeout_seconds, (int, float))
            and not isinstance(timeout_seconds, bool)
            and math.isfinite(float(timeout_seconds))
            and float(timeout_seconds) > 0,
            "daemon authority challenge timeout must be positive and finite",
            RunnerError,
        )
        identity = self.current_identity()
        _require(isinstance(identity, Mapping), "owned daemon identity disappeared before challenge", RunnerError)
        authority = self.authority
        _require(isinstance(authority, Mapping), "owned daemon authority record is unavailable", RunnerError)
        token = authority.get("auth_token")
        _require(
            isinstance(token, str) and re.fullmatch(r"[0-9a-fA-F]{64}", token) is not None,
            "owned daemon authority token is malformed",
            RunnerError,
        )
        request_id = f"native-original-authority/{self.side}/{challenge.lower()}"
        handshake = {
            "project_path": None,
            "scope_prefix": None,
            "timings": False,
            "allow_init": False,
            "allow_initialize_root_routing": False,
            "client_identity": {
                "profile_root": str(self.profile_root),
                "global_db_path": str(self.profile_root / "global.db"),
            },
            "client_version": "native-original-runner",
            "client_instance_id": request_id,
            "tool_list_changed_capable": False,
            "catalog_version": "",
            "moved_store_adoption": "never",
        }
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "tracedecay-native-original-runner-authority-probe",
                    "version": "1",
                },
                "_meta": {
                    "nativeOriginalAuthorityChallenge": challenge.lower(),
                },
            },
        }
        preface = {
            "protocol": "tracedecay-daemon-v1",
            "auth_token": token,
        }
        request_bytes = b"".join(
            _json_bytes(item) + b"\n" for item in (preface, handshake, request)
        )
        response_bytes = bytearray()
        try:
            import socket

            endpoint = identity.get("endpoint")
            if isinstance(endpoint, Mapping) and endpoint.get("kind") == "loopback":
                address = endpoint.get("address")
                _require(isinstance(address, str) and ":" in address, "owned daemon loopback endpoint is malformed", RunnerError)
                host, port_text = address.rsplit(":", 1)
                stream = socket.create_connection(
                    (host.strip("[]"), int(port_text)),
                    timeout=float(timeout_seconds),
                )
            else:
                stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                stream.settimeout(float(timeout_seconds))
                stream.connect(str(self.socket_path))
            with stream:
                stream.sendall(request_bytes)
                # This probe is a one-request exchange.  Half-close stdin so
                # an owned daemon has a framing boundary and then consume the
                # complete response; accepting the first newline would allow
                # a second parseable response to hide after an apparently
                # valid answer.
                try:
                    stream.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
                while len(response_bytes) <= 16 * 1024 * 1024:
                    chunk = stream.recv(64 * 1024)
                    if not chunk:
                        break
                    response_bytes.extend(chunk)
                    if len(response_bytes) > 16 * 1024 * 1024:
                        break
        except RunnerError:
            raise
        except (OSError, ValueError, TypeError) as error:
            raise RunnerError(f"owned daemon authority challenge transport failed: {error}") from error
        response_line = bytes(response_bytes)
        _require(
            response_line.strip()
            and response_line.endswith(b"\n")
            and response_line.count(b"\n") == 1,
            "owned daemon authority challenge must return exactly one newline-terminated response",
            RunnerError,
        )
        try:
            response = _strict_json_loads(response_line.decode("utf-8", errors="strict"))
        except (UnicodeDecodeError, ValueError, TypeError, RecursionError) as error:
            raise RunnerError(f"owned daemon authority challenge returned malformed JSON: {error}") from error
        _require(isinstance(response, Mapping), "owned daemon authority challenge response is not an object", RunnerError)
        _require(response.get("jsonrpc") == "2.0", "owned daemon authority challenge response is not JSON-RPC 2.0", RunnerError)
        _require(response.get("id") == request_id, "owned daemon authority challenge response id mismatched", RunnerError)
        _require("error" not in response and isinstance(response.get("result"), Mapping), "owned daemon authority challenge was not successful", RunnerError)
        return {
            "protocol_observed": True,
            "source": "runner_owned_daemon_challenge",
            "challenge": challenge.lower(),
            "challenge_response": bytes_digest(response_line),
            "challenge_response_scheme": "sha256_exact_daemon_response_bytes",
            "request_sha256": bytes_digest(request_bytes),
            "response_sha256": bytes_digest(response_line),
            "response_bytes": len(response_line),
            "request_id": request_id,
            "response_jsonrpc": response.get("jsonrpc"),
            "authority_digest": _authority_identity_digest(identity),
            "endpoint": copy.deepcopy(identity.get("endpoint")),
            "profile_root": identity.get("profile_root"),
            "process_root": identity.get("process_root"),
            "binary_root": str(self.binary.resolve().parent),
            "store_root": identity.get("store_root"),
            "daemon_pid": identity.get("pid"),
            "daemon_binary_sha256": identity.get("binary_sha256"),
        }

    def prepare_action(
        self,
        *,
        route_id: str,
        entrypoint: str,
        action_id: str,
        scope: str,
        tool: str,
        arguments: Mapping[str, Any],
        timeout_seconds: float = 2.0,
    ) -> dict[str, Any]:
        """Reserve one action through the daemon's authenticated prepare wire.

        The proof key is returned only to the runner's in-memory caller so it
        can verify the terminal receipt HMAC.  The child receives the nonce in
        its MCP call metadata, never the key or the prepare acknowledgement.
        """

        _require(isinstance(route_id, str) and bool(route_id.strip()), "action prepare route is invalid", RunnerError)
        _require(isinstance(entrypoint, str) and bool(entrypoint.strip()), "action prepare entrypoint is invalid", RunnerError)
        _require(isinstance(action_id, str) and bool(action_id.strip()), "action prepare id is invalid", RunnerError)
        _require(isinstance(scope, str) and bool(scope.strip()), "action prepare scope is invalid", RunnerError)
        try:
            scope_path = Path(scope)
            _require(scope_path.is_absolute(), "action prepare scope must be an absolute project path", RunnerError)
        except (TypeError, ValueError, OSError) as error:
            raise RunnerError(f"action prepare scope is not a valid project path: {error}") from error
        _require(isinstance(tool, str) and bool(tool.strip()), "action prepare tool is invalid", RunnerError)
        _require(isinstance(arguments, Mapping), "action prepare arguments must be an object", RunnerError)
        _require(
            isinstance(timeout_seconds, (int, float))
            and not isinstance(timeout_seconds, bool)
            and math.isfinite(float(timeout_seconds))
            and float(timeout_seconds) > 0,
            "action prepare timeout must be positive and finite",
            RunnerError,
        )
        action_digest = _daemon_action_digest(
            scope=scope,
            tool=tool,
            entrypoint=entrypoint,
            arguments=arguments,
        )
        nonce = os.urandom(32).hex()
        proof_key = os.urandom(32).hex()
        expires_at = time.time_ns() // 1_000 + max(
            1,
            min(int(float(timeout_seconds) * 1_000_000), 5 * 60 * 1_000_000),
        )
        request_id = f"native-original/action-prepare/{self.side}/{action_id}/{nonce}"
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {
                    "name": "tracedecay-native-original-runner",
                    "version": "1",
                },
                "_meta": {
                    "nativeOriginalActionPrepare": {
                        "format": DAEMON_ACTION_RECEIPT_FORMAT,
                        "revision": 1,
                        "action_id": action_id,
                        "route": route_id,
                        "nonce": nonce,
                        "proof_key": proof_key,
                        "action_digest": action_digest,
                        "scope": scope,
                        "tool": tool,
                        "entrypoint": entrypoint,
                        "expires_at": expires_at,
                    }
                },
            },
        }
        response, request_bytes, wire, identity = _owned_daemon_rpc_exchange(
            self,
            request,
            timeout_seconds=timeout_seconds,
            label="action prepare",
        )
        initialize_result = response.get("result")
        acknowledgement = None
        if isinstance(initialize_result, Mapping):
            metadata = initialize_result.get("_meta")
            if isinstance(metadata, Mapping):
                acknowledgement = metadata.get("nativeOriginalActionPrepare")
            if not isinstance(acknowledgement, Mapping):
                acknowledgement = initialize_result.get("nativeOriginalActionPrepare")
            if not isinstance(acknowledgement, Mapping) and initialize_result.get("format") == DAEMON_ACTION_RECEIPT_FORMAT:
                # The daemon connection-serving adapter serializes the
                # ActionPrepareResponse directly as the JSON-RPC result;
                # tolerate a metadata wrapper as well for future transports,
                # while requiring the exact signed protocol fields below.
                acknowledgement = initialize_result
        _require(
            isinstance(acknowledgement, Mapping),
            "owned daemon action prepare acknowledgement is missing",
            RunnerError,
        )
        _require(
            acknowledgement.get("format") == DAEMON_ACTION_RECEIPT_FORMAT
            and acknowledgement.get("revision") == 1
            and acknowledgement.get("action_id") == action_id
            and acknowledgement.get("route") == route_id
            and acknowledgement.get("nonce") == nonce
            and acknowledgement.get("action_digest") == action_digest
            and acknowledgement.get("scope") == scope
            and acknowledgement.get("tool") == tool
            and acknowledgement.get("entrypoint") == entrypoint,
            "owned daemon action prepare acknowledgement does not match the runner request",
            RunnerError,
        )
        generation = acknowledgement.get("daemon_generation")
        _require(
            isinstance(generation, Mapping)
            and generation.get("epoch") == identity.get("epoch")
            and generation.get("process_run_id") == identity.get("process_run_id"),
            "owned daemon action prepare acknowledgement is not bound to the live daemon generation",
            RunnerError,
        )
        acknowledged_expiry = acknowledgement.get("expires_at")
        _require(
            isinstance(acknowledged_expiry, int)
            and not isinstance(acknowledged_expiry, bool)
            and acknowledged_expiry == expires_at
            and acknowledged_expiry > time.time_ns() // 1_000,
            "owned daemon action prepare acknowledgement has an invalid or expired lifetime",
            RunnerError,
        )
        acknowledged_store = _daemon_store_value(acknowledgement.get("store_identity"))
        _require(
            acknowledged_store is not None
            and acknowledged_store.get("project_root") == scope,
            "owned daemon action prepare acknowledgement lacks the live store identity",
            RunnerError,
        )
        return {
            "observed": True,
            "source": "runner_owned_daemon_action_prepare",
            "format": DAEMON_ACTION_RECEIPT_FORMAT,
            "revision": 1,
            "side": self.side,
            "route": route_id,
            "entrypoint": entrypoint,
            "action_id": action_id,
            "scope": scope,
            "tool": tool,
            "nonce": nonce,
            "action_digest": action_digest,
            "expires_at": expires_at,
            "store_identity": copy.deepcopy(acknowledged_store),
            "authority_digest": _authority_identity_digest(identity),
            "daemon_identity": copy.deepcopy(identity),
            "daemon_generation": copy.deepcopy(dict(generation)),
            "wire_request_sha256": bytes_digest(request_bytes),
            "wire_response_sha256": bytes_digest(wire),
            # Internal-only verification material.  _execute_side_action
            # removes it before serializing any result/artifact.
            "_runner_proof_key": bytes.fromhex(proof_key),
        }

    def _identity(self, record: Mapping[str, Any]) -> dict[str, Any]:
        observed_incarnation = _process_incarnation(self.process.pid)
        # ``status`` and the platform source label are observational detail;
        # keep only the stable pid/parent/uid/start token in the daemon
        # authority identity so a normal running/sleeping transition does not
        # invalidate an otherwise unchanged action receipt.
        process_incarnation = (
            {
                key: copy.deepcopy(value)
                for key, value in observed_incarnation.items()
                if key not in {"status", "source"}
            }
            if isinstance(observed_incarnation, Mapping)
            else None
        )
        return {
            "side": self.side,
            "pid": self.process.pid,
            "process_group_id": self.process.pid if os.name != "nt" else None,
            "process_run_id": record.get("process_run_id"),
            "epoch": record.get("epoch"),
            "version": record.get("version"),
            "endpoint": copy.deepcopy(record.get("endpoint")),
            "profile_root": str(self.profile_root),
            "store_root": str(self.profile_root),
            "process_root": str(self.process_root),
            "binary_path": str(self.binary),
            "binary_sha256": self.binary_sha256,
            "authority_path": str(self.authority_path),
            "process_incarnation": process_incarnation,
        }

    def current_identity(self) -> dict[str, Any] | None:
        if self.process.poll() is not None:
            return None
        try:
            authority_bytes, _ = _read_stable_file(
                self.authority_path,
                label=f"{self.side} daemon authority record",
            )
            record = _strict_json_loads(authority_bytes.decode("utf-8", errors="strict"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError, RunnerError):
            return None
        if not isinstance(record, Mapping) or record.get("pid") != self.process.pid:
            return None
        if not _endpoint_matches(record.get("endpoint"), self.socket_path):
            return None
        try:
            profile_matches = (
                isinstance(record.get("profile_root"), str)
                and bool(record["profile_root"].strip())
                and Path(record["profile_root"]).resolve() == self.profile_root
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            profile_matches = False
        if not profile_matches:
            return None
        if (
            not isinstance(record.get("process_run_id"), str)
            or not record["process_run_id"]
            or not isinstance(record.get("epoch"), int)
            or isinstance(record["epoch"], bool)
            or record["epoch"] <= 0
            or not isinstance(record.get("version"), str)
            or not record["version"]
            or not isinstance(record.get("auth_token"), str)
            or re.fullmatch(r"[0-9a-fA-F]{64}", record["auth_token"]) is None
        ):
            return None
        identity = self._identity(record)
        # A changed authority record with the same PID/socket/profile is still
        # a different daemon epoch.  Keep the original record as the binding
        # and reject such a replacement instead of treating it as owned.
        if self.identity is not None:
            for field in (
                "pid",
                "process_run_id",
                "epoch",
                "version",
                "endpoint",
                "profile_root",
                "store_root",
                "process_root",
                "binary_sha256",
                "authority_path",
            ):
                if identity.get(field) != self.identity.get(field):
                    return None
            # Process status can change while the daemon remains the same
            # incarnation.  Compare the stable pid/parent/uid/start-time
            # token separately so a normal status transition does not look
            # like PID reuse, while an actual replacement is rejected.
            if not _same_process_incarnation(
                identity.get("process_incarnation"),
                self.identity.get("process_incarnation"),
            ):
                return None
        return identity

    def _read_log_tail(self, path: Path) -> str:
        try:
            return path.read_bytes()[-4096:].decode("utf-8", errors="replace").strip()
        except OSError:
            return ""

    def _close_logs(self) -> None:
        for stream in (self._stdout_file, self._stderr_file):
            if not stream.closed:
                stream.flush()
                try:
                    os.fsync(stream.fileno())
                except OSError:
                    pass
                stream.close()

    def wait_for_exit(self, seconds: float) -> bool:
        try:
            if self.process.poll() is not None:
                return True
        except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
            return False
        try:
            self.process.wait(timeout=seconds)
        except (subprocess.TimeoutExpired, OSError, ValueError):
            return False
        return True

    def stop(self, timeouts: ProcessTimeouts, *, reason: str) -> dict[str, Any]:
        _require(isinstance(timeouts, ProcessTimeouts), "process timeout configuration is invalid", RunnerError)
        _require(isinstance(reason, str), "cleanup reason must be a string", RunnerError)
        if self.cleanup is not None:
            return self.cleanup
        cleanup_errors: list[str] = []
        sent = None
        try:
            running = self.process.poll() is None
        except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError) as error:
            running = True
            cleanup_errors.append(f"cannot inspect daemon process before cleanup: {error}")
        if running:
            sent = "SIGTERM" if os.name != "nt" else "terminate"
            _signal_group(self.process, signal.SIGTERM, self.process.pid if os.name != "nt" else None)
            if os.name == "nt":
                try:
                    self.process.terminate()
                except (OSError, ValueError) as error:
                    cleanup_errors.append(f"terminate raced with daemon exit: {error}")
            if not self.wait_for_exit(timeouts.terminate_seconds):
                sent = "SIGKILL" if os.name != "nt" else "kill"
                _signal_group(self.process, signal.SIGKILL, self.process.pid if os.name != "nt" else None)
                if os.name == "nt":
                    try:
                        self.process.kill()
                    except (OSError, ValueError) as error:
                        cleanup_errors.append(f"kill raced with daemon exit: {error}")
                if not self.wait_for_exit(timeouts.kill_seconds):
                    cleanup_errors.append("daemon did not exit after the bounded kill timeout")
        try:
            still_running = self.process.poll() is None
        except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError) as error:
            still_running = True
            cleanup_errors.append(f"cannot inspect daemon process after bounded cleanup: {error}")
        if still_running:
            try:
                self.process.kill()
            except (OSError, ValueError) as error:
                cleanup_errors.append(f"final daemon kill raced with process exit: {error}")
            if not self.wait_for_exit(timeouts.kill_seconds):
                cleanup_errors.append("daemon remained alive after final bounded kill")
        try:
            returncode = self.process.returncode
        except (OSError, ValueError) as error:
            returncode = None
            cleanup_errors.append(f"cannot read daemon return code: {error}")
        members = _group_members(self.process.pid) if os.name != "nt" else []
        group_observation = members is not None
        if members is None:
            members = []
        if members:
            _signal_group(self.process, signal.SIGKILL, self.process.pid)
            deadline = time.monotonic() + timeouts.kill_seconds
            while (_group_members(self.process.pid) or []) and time.monotonic() < deadline:
                time.sleep(0.01)
            observed_members = _group_members(self.process.pid)
            if observed_members is None:
                group_observation = False
                members = []
            else:
                members = observed_members
        self._close_logs()
        if os.name != "nt":
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass
            except OSError as error:
                cleanup_errors.append(f"cannot remove daemon socket {self.socket_path}: {error}")
        end_monotonic = time.monotonic_ns()
        process_record = {
            "argv": [
                str(self.binary),
                "daemon",
                "run",
                "--socket",
                str(self.socket_path),
                "--profile-root",
                str(self.profile_root),
            ],
            "cwd": str(self.side_dir),
            "phase": "daemon",
            "pid": self.process.pid,
            "process_group_id": self.process.pid if os.name != "nt" else None,
            "start_wall_time_ns": self.started_wall,
            "end_wall_time_ns": time.time_ns(),
            "elapsed_ns": end_monotonic - self.started_monotonic,
            "returncode": returncode,
            "status": "completed" if returncode == 0 else "error",
            "reason": None if returncode == 0 else f"daemon exited with return code {returncode}",
            "remaining_owned_children": len(members),
            "process_group_observed": group_observation,
        }
        process_ref = _write_json(self.artifact_dir / "process.json", process_record)
        status = (
            "completed"
            if returncode == 0 and not members and group_observation and not cleanup_errors
            else "unknown"
        )
        self.cleanup = {
            "status": status,
            "reason": (
                None
                if status == "completed"
                else (
                    "; ".join(cleanup_errors)
                    if cleanup_errors
                    else "owned foreground daemon or child process did not settle"
                )
            ),
            "requested_reason": reason,
            "signal": sent,
            "returncode": returncode,
            "identity": copy.deepcopy(self.identity),
            "process": process_record,
            "process_artifact": process_ref,
            "stdout": _file_ref(self.artifact_dir / "stdout.bin"),
            "stderr": _file_ref(self.artifact_dir / "stderr.bin"),
            "remaining_owned_children": len(members),
            "process_group_observed": group_observation,
            "cleanup_errors": cleanup_errors,
        }
        _write_json(self.artifact_dir / "cleanup.json", self.cleanup)
        return self.cleanup


def _group_members(process_group_id: int) -> list[int] | None:
    """Return observed members of an owned process group when ps is available."""

    if os.name == "nt":
        return []
    if isinstance(process_group_id, bool) or not isinstance(process_group_id, int) or process_group_id <= 0:
        return None
    try:
        result = subprocess.run(
            ["ps", "-axo", "pid=,pgid="],
            check=False,
            capture_output=True,
            text=True,
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    if result.returncode != 0 or not isinstance(result.stdout, str):
        return None
    members = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) == 2:
            try:
                pid, pgid = (int(item) for item in fields)
            except ValueError:
                continue
            if pgid == process_group_id:
                members.append(pid)
    return members


def _signal_group(process: subprocess.Popen[bytes], sig: signal.Signals, group_id: int | None) -> bool:
    if (
        group_id is None
        or isinstance(group_id, bool)
        or not isinstance(group_id, int)
        or group_id <= 0
        or os.name == "nt"
        or group_id == os.getpgrp()
    ):
        return False
    try:
        os.killpg(group_id, sig)
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return False


def _spawn(argv: Sequence[str], cwd: Path, environment: Mapping[str, str]) -> subprocess.Popen[bytes]:
    kwargs: dict[str, Any] = {
        "args": list(argv),
        "cwd": str(cwd),
        "env": dict(environment),
        "stdin": subprocess.PIPE,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
    }
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(**kwargs)


def _captured_bytes(value: Any, stream_name: str) -> tuple[bytes, str | None]:
    """Normalize subprocess output without allowing a kill race to crash evidence."""

    if value is None:
        return b"", None
    if isinstance(value, bytes):
        return value, None
    if isinstance(value, bytearray):
        return bytes(value), None
    return b"", f"{stream_name} capture had an invalid type: {type(value).__name__}"


def run_process(
    argv: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    artifact_dir: Path,
    timeout: ProcessTimeouts = ProcessTimeouts(),
    input_bytes: bytes | None = None,
    phase: str = "operation",
    detached_process_possible: bool = False,
) -> dict[str, Any]:
    """Run one owned process, preserving raw streams and bounded cleanup."""

    _require(
        isinstance(argv, (list, tuple))
        and not isinstance(argv, (str, bytes))
        and all(isinstance(item, (str, os.PathLike)) for item in argv),
        "process argv must be a list of path/string arguments",
        RunnerError,
    )
    _require(isinstance(cwd, Path), "process cwd must be a Path", RunnerError)
    _require(isinstance(environment, Mapping), "process environment must be an object", RunnerError)
    _require(
        all(isinstance(key, str) and isinstance(value, str) for key, value in environment.items()),
        "process environment must contain string keys and values",
        RunnerError,
    )
    _require(isinstance(artifact_dir, Path), "process artifact directory must be a Path", RunnerError)
    _require(isinstance(timeout, ProcessTimeouts), "process timeout configuration is invalid", RunnerError)
    _require(input_bytes is None or isinstance(input_bytes, (bytes, bytearray)), "process input must be bytes", RunnerError)
    try:
        artifact_dir.mkdir(parents=True, exist_ok=False)
    except OSError as error:
        raise RunnerError(f"cannot create process artifact directory {artifact_dir}: {error}") from error
    stdin_ref: dict[str, Any] | None = None
    if input_bytes is not None:
        try:
            stdin_ref = _write_bytes(artifact_dir / "stdin.bin", bytes(input_bytes))
        except (OSError, TypeError, ValueError) as error:
            raise RunnerError(f"cannot persist process stdin under {artifact_dir}: {error}") from error
    started_wall = time.time_ns()
    started_monotonic = time.monotonic_ns()
    process: subprocess.Popen[bytes] | None = None
    stdout = b""
    stderr = b""
    timed_out = False
    spawn_error: str | None = None
    process_group_id: int | None = None
    returncode: int | None = None
    members: list[int] = []
    group_observation = False
    process_error: str | None = None
    try:
        process = _spawn(argv, cwd, environment)
        process_group_id = process.pid if os.name != "nt" else None
        try:
            stdout, stderr = process.communicate(input=input_bytes, timeout=timeout.action_seconds)
            returncode = process.returncode
        except subprocess.TimeoutExpired as error:
            timed_out = True
            stdout, stdout_error = _captured_bytes(error.output, "stdout")
            stderr, stderr_error = _captured_bytes(error.stderr, "stderr")
            process_error = stdout_error or stderr_error
            try:
                _signal_group(process, signal.SIGTERM, process_group_id)
            except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError) as signal_error:
                process_error = process_error or f"timeout termination signal failed: {signal_error}"
            try:
                stdout, stderr = process.communicate(timeout=timeout.terminate_seconds)
            except subprocess.TimeoutExpired:
                try:
                    _signal_group(process, signal.SIGKILL, process_group_id)
                except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError) as signal_error:
                    process_error = process_error or f"timeout kill signal failed: {signal_error}"
                try:
                    stdout, stderr = process.communicate(timeout=timeout.kill_seconds)
                except subprocess.TimeoutExpired:
                    try:
                        process.kill()
                    except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError) as kill_error:
                        process_error = process_error or f"timeout kill raced with process exit: {kill_error}"
                    try:
                        stdout, stderr = process.communicate(timeout=timeout.kill_seconds)
                    except (subprocess.TimeoutExpired, OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError) as kill_wait_error:
                        process_error = process_error or f"timeout process reap failed: {kill_wait_error}"
                        stdout, stderr = b"", b""
            returncode = process.returncode
        except (OSError, ValueError) as error:
            process_error = f"process execution failed: {type(error).__name__}: {error}"
            try:
                _signal_group(process, signal.SIGKILL, process_group_id)
            except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
                pass
            try:
                process.kill()
            except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
                pass
            try:
                stdout, stderr = process.communicate(timeout=timeout.kill_seconds)
            except (subprocess.TimeoutExpired, OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
                stdout, stderr = b"", b""
            returncode = process.returncode
    except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError) as error:
        spawn_error = f"{type(error).__name__}: {error}"
    if process is not None:
        try:
            still_running = process.poll() is None
        except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
            still_running = True
        if still_running:
            process_error = process_error or "process remained alive after bounded timeout cleanup"
            try:
                _signal_group(process, signal.SIGKILL, process_group_id)
            except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
                pass
            try:
                process.kill()
            except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
                pass
            try:
                process.wait(timeout=timeout.kill_seconds)
            except (subprocess.TimeoutExpired, OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError) as wait_error:
                process_error = process_error or f"process kill race was not reaped: {wait_error}"
    ended_monotonic = time.monotonic_ns()
    if process is not None and process_group_id is not None:
        members = _group_members(process_group_id)
        group_observation = members is not None
        if members is None:
            members = []
        if members:
            _signal_group(process, signal.SIGTERM, process_group_id)
            deadline = time.monotonic() + timeout.terminate_seconds
            while (_group_members(process_group_id) or []) and time.monotonic() < deadline:
                time.sleep(0.01)
            observed_members = _group_members(process_group_id)
            if observed_members is None:
                group_observation = False
                members = []
            else:
                members = observed_members
            if members:
                _signal_group(process, signal.SIGKILL, process_group_id)
                deadline = time.monotonic() + timeout.kill_seconds
                while (_group_members(process_group_id) or []) and time.monotonic() < deadline:
                    time.sleep(0.01)
                observed_members = _group_members(process_group_id)
                if observed_members is None:
                    group_observation = False
                    members = []
                else:
                    members = observed_members
        else:
            members = []
    else:
        # Windows has no portable process-group enumeration in this runner;
        # the owned Popen child has still been waited on, which is the
        # strongest available cleanup observation there.
        group_observation = os.name == "nt"
    stdout, stdout_error = _captured_bytes(stdout, "stdout")
    stderr, stderr_error = _captured_bytes(stderr, "stderr")
    process_error = process_error or stdout_error or stderr_error
    try:
        stdout_ref = _write_bytes(artifact_dir / "stdout.bin", stdout)
        stderr_ref = _write_bytes(artifact_dir / "stderr.bin", stderr)
    except OSError as error:
        raise RunnerError(f"cannot persist process streams under {artifact_dir}: {error}") from error
    if spawn_error:
        status = "unknown"
        reason = spawn_error
    elif process_error:
        status = "unknown"
        reason = process_error
    elif timed_out:
        status = "censored"
        reason = "process exceeded the action cutoff; terminal outcome is unknown"
    elif returncode == 0:
        status = "completed"
        reason = None
    else:
        status = "error"
        reason = f"process exited with return code {returncode}"
    cleanup_status = (
        "unverified"
        if detached_process_possible or not group_observation
        else ("completed" if not members else "failed")
    )
    if cleanup_status != "completed" and status in ("completed", "error"):
        status = "unknown"
        reason = (
            "process completed but runner-owned cleanup was not verified"
            if cleanup_status == "unverified"
            else "process result is unknown because owned process-group cleanup failed"
        )
    process_pid = getattr(process, "pid", None) if process is not None else None
    process_record = {
        "argv": list(argv),
        "cwd": str(cwd),
        "phase": phase,
        "pid": process_pid,
        "process_group_id": process_group_id,
        "start_wall_time_ns": started_wall,
        "end_wall_time_ns": time.time_ns(),
        "elapsed_ns": ended_monotonic - started_monotonic,
        "returncode": returncode,
        "status": status,
        "reason": reason,
        "failure_class": (
            "spawn_failure"
            if spawn_error
            else "process_failure"
            if process_error
            else None
        ),
        "spawn_error": spawn_error,
        "detached_process_possible": detached_process_possible,
        "process_root": environment.get("TRACEDECAY_COMPARISON_PROCESS_ROOT"),
        "remaining_owned_children": len(members),
        "process_group_observed": group_observation,
        "stdin": stdin_ref,
        "stdin_exchange_sha256": stdin_ref.get("sha256") if isinstance(stdin_ref, Mapping) else None,
    }
    try:
        process_ref = _write_json(artifact_dir / "process.json", process_record)
    except OSError as error:
        raise RunnerError(f"cannot persist process evidence under {artifact_dir}: {error}") from error
    return {
        "status": status,
        "reason": reason,
        "returncode": returncode,
        "stdout": stdout_ref,
        "stderr": stderr_ref,
        "process": process_record,
        "process_artifact": process_ref,
        "stdin": stdin_ref,
        "stdin_exchange_sha256": stdin_ref.get("sha256") if isinstance(stdin_ref, Mapping) else None,
        "cleanup": {
            "status": "not_started" if spawn_error else cleanup_status,
            "reason": (
                spawn_error
                if spawn_error
                else "CLI may have contacted a daemon outside this process group"
                if detached_process_possible
                else (None if not members else "owned process-group members remained after cleanup")
            ),
            "failure_class": (
                "spawn_failure"
                if spawn_error
                else "process_failure"
                if process_error
                else None
            ),
            "remaining_owned_children": len(members),
            "process_group_id": process_group_id,
        },
        "timing": {"start_monotonic_ns": started_monotonic, "end_monotonic_ns": ended_monotonic, "elapsed_ns": ended_monotonic - started_monotonic},
    }


def _parse_json_bytes(value: bytes) -> tuple[Any | None, str | None]:
    if not isinstance(value, (bytes, bytearray)):
        return None, "stdout is not bytes"
    if not value.strip():
        return None, "empty stdout"
    try:
        return _strict_json_loads(value.decode("utf-8", errors="strict")), None
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        return None, f"stdout is not one strict JSON response: {type(error).__name__}: {error}"


_PARSED_TERMINAL_STATUS = {
    "aborted": "cancelled",
    "budget_exhausted": "blocked",
    "busy": "blocked",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "complete_zero": "completed",
    "cursor_manifest_limit_exceeded": "blocked",
    "deadline_exceeded": "censored",
    "deleted": "unsupported",
    "denied": "unknown",
    "partial": "partial",
    "effect_unknown": "effect_unknown",
    "effect-unknown": "effect_unknown",
    "blocked": "blocked",
    "unavailable": "unsupported",
    "not_available": "unsupported",
    "not-available": "unsupported",
    "unsupported": "unsupported",
    "unknown": "unknown",
    "invalid": "invalid",
    "censored": "censored",
    "completed": "completed",
    "complete": "completed",
    "success": "completed",
    "ok": "completed",
    "error": "error",
    "failed": "error",
    "failure": "error",
    "joined": "partial",
    "locked": "blocked",
    "not_found": "unsupported",
    "recorded": "completed",
    "redacted": "unsupported",
    "running": "partial",
    "stale": "unknown",
    "started": "partial",
    "unsupported_filter": "unsupported",
    "wrong_scope": "unknown",
}
_TERMINAL_STATUS_KEYS = (
    "status",
    "terminal_status",
    "terminal",
    "outcome",
    "availability",
    "availability_status",
    "error",
    "failure",
    "result",
    # MCP tool-level terminal failures are successful JSON-RPC responses with
    # a boolean result flag.  Keep this field in the explicit status scan so
    # an isError response cannot be treated as an ordinary completed result.
    "isError",
)
_TERMINAL_STATUS_SCALAR_KEYS = frozenset(
    {
        "status",
        "terminal_status",
        "terminal",
        "outcome",
        "availability",
        "availability_status",
        "isError",
    }
)


def _terminal_status_values(value: Any, _depth: int = 0) -> list[tuple[str, str]]:
    """Collect every explicit terminal declaration for conflict detection."""

    if _depth > 64:
        raise RunnerError("terminal status payload is too deeply nested")
    found: list[tuple[str, str]] = []
    if isinstance(value, Mapping):
        for key in _TERMINAL_STATUS_KEYS:
            if key not in value:
                continue
            nested = value[key]
            location = f"/{key}"
            if key == "isError":
                if isinstance(nested, bool):
                    if nested:
                        found.append(("error", location))
                else:
                    # A non-boolean isError marker is malformed.  It must
                    # not be truth-tested into a terminal success/failure.
                    found.append(("unknown", location))
                continue
            if isinstance(nested, str):
                normalized = nested.strip().lower().replace(" ", "_")
                mapped = _PARSED_TERMINAL_STATUS.get(normalized)
                found.append((mapped or "unknown", location))
            elif key in _TERMINAL_STATUS_SCALAR_KEYS and not isinstance(nested, (Mapping, list)):
                # A terminal declaration with a non-string scalar is a
                # malformed terminal result.  Treat it as unknown so a
                # process-completed transport cannot turn malformed output
                # into a semantic pass.
                found.append(("unknown", location))
            else:
                for mapped, nested_location in _terminal_status_values(nested, _depth + 1):
                    found.append((mapped, location + nested_location))
        if value.get("available") is False:
            found.append(("unsupported", "/available"))
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            for mapped, location in _terminal_status_values(nested, _depth + 1):
                found.append((mapped, f"/{index}{location}"))
    return found


def _recognized_production_error(value: Any, _depth: int = 0) -> bool:
    """Return true only for an explicit typed production error payload."""

    if _depth > 64:
        return False
    if not isinstance(value, Mapping):
        return False
    error_values = []
    for key in ("error", "failure", "problem"):
        candidate = value.get(key)
        if isinstance(candidate, Mapping):
            if any(candidate.get(field) not in (None, "") for field in ("code", "kind", "type", "message", "reason")):
                error_values.append(candidate)
    if error_values:
        return True
    nested = value.get("outcome")
    if isinstance(nested, Mapping):
        return _recognized_production_error(nested, _depth + 1)
    return False


def _parsed_terminal_status(value: Any, path: str = "") -> tuple[str | None, str | None]:
    """Map a production response's explicit terminal state to runner status."""

    try:
        found = _terminal_status_values(value)
    except RunnerError:
        return "unknown", "terminal_status_payload_invalid"
    # A typed production error is itself a comparable terminal result even
    # when the producer omitted a redundant ``status`` field.  If it also
    # declares a successful status, retain the conflict as unknown rather than
    # allowing the error payload to become a pass.
    if _recognized_production_error(value):
        found.append(("error", "/error"))
    if not found:
        return None, None
    mapped_values = {item[0] for item in found}
    if len(mapped_values) > 1:
        return "unknown", "terminal_status_conflict"
    return found[0]


def _mcp_response(
    value: bytes,
    request_id: str,
    *,
    allow_terminal_error: bool = False,
) -> tuple[Any | None, str | None]:
    if not isinstance(value, (bytes, bytearray)) or not isinstance(request_id, str) or not request_id:
        return None, "MCP stdout or request id has an invalid shape"
    responses: list[dict[str, Any]] = []
    initialize_responses: list[dict[str, Any]] = []
    malformed: list[str] = []
    allowed_ids = {request_id, f"{request_id}/initialize"}
    for line in value.splitlines():
        if not line.strip():
            continue
        try:
            row = _strict_json_loads(line.decode("utf-8", errors="strict"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
            malformed.append(str(error))
            continue
        if not isinstance(row, dict):
            malformed.append("MCP stdout contained a non-object JSON value")
            continue
        if row.get("jsonrpc") != "2.0":
            malformed.append("MCP stdout response did not use JSON-RPC 2.0")
            continue
        if "method" in row:
            # A server notification/request is not the response to the
            # runner-owned exchange.  Accepting it as a response lets a
            # producer put a method-bearing self-assertion beside a valid
            # result and obscure the actual protocol boundary.
            malformed.append("MCP stdout contained a method-bearing response")
            continue
        # The runner sends one initialize request followed by one tools/call.
        # Keep the protocol envelope narrow: unrelated parseable stdout is
        # trailing output and cannot be silently ignored as if the requested
        # response were the only exchange.
        response_id = row.get("id")
        if not isinstance(response_id, str) or response_id not in allowed_ids:
            malformed.append("MCP stdout contained an unexpected response id")
            continue
        if "error" in row:
            error = row.get("error")
            if response_id != request_id or not allow_terminal_error or not isinstance(error, Mapping):
                malformed.append("MCP response contained a JSON-RPC error")
                continue
            # An owned daemon may attach the signed action receipt below
            # error.data for a typed terminal failure.  Keep this branch
            # opt-in; ordinary comparison MCP parsing still requires a
            # successful target result and therefore cannot turn arbitrary
            # JSON-RPC errors into a comparable response.
            if _extract_daemon_action_receipt(row) is None:
                malformed.append("MCP terminal error lacked a signed action receipt")
                continue
            responses.append(row)
            continue
        if not isinstance(row.get("result"), Mapping):
            malformed.append("MCP response did not contain a result object")
            continue
        if response_id == request_id:
            target_result = row["result"]
            content = target_result.get("content")
            if not isinstance(content, list) or any(
                not isinstance(item, Mapping)
                or not isinstance(item.get("type"), str)
                or not item.get("type", "").strip()
                or (
                    item.get("type") == "text"
                    and not isinstance(item.get("text"), str)
                )
                for item in content
            ):
                malformed.append("MCP target result lacks a typed content array")
                continue
            if "isError" in target_result and not isinstance(target_result.get("isError"), bool):
                malformed.append("MCP target result isError must be boolean")
                continue
            if target_result.get("isError") is True:
                # MCP tool failures are terminal result envelopes rather
                # than JSON-RPC transport errors.  They are comparable only
                # when the owned daemon attached its signed one-shot receipt;
                # an unsigned isError flag remains an arbitrary child claim.
                if not allow_terminal_error or _extract_daemon_action_receipt(row) is None:
                    malformed.append("MCP terminal isError result lacked a signed action receipt")
                    continue
            responses.append(row)
        else:
            initialize_result = row["result"]
            if (
                not isinstance(initialize_result.get("protocolVersion"), str)
                or not initialize_result.get("protocolVersion", "").strip()
                or not isinstance(initialize_result.get("capabilities"), Mapping)
                or not isinstance(initialize_result.get("serverInfo"), Mapping)
            ):
                malformed.append("MCP initialize result lacks protocolVersion/capabilities/serverInfo")
                continue
            initialize_responses.append(row)
    if malformed:
        return None, "MCP stdout contained malformed or unexpected lines"
    if len(initialize_responses) != 1:
        return None, f"MCP initialize response id {request_id}/initialize was not observed exactly once"
    if len(responses) != 1:
        return None, f"MCP response id {request_id} was not observed exactly once"
    return responses[0], None


def _validate_mcp_stdin_exchange(
    value: bytes,
    *,
    side: str,
    case_id: str,
    action_id: str,
    request: Mapping[str, Any],
    tool: str,
    action_prepare: Mapping[str, Any] | None = None,
) -> bool:
    """Bind an MCP stdin capture to the exact runner-generated request.

    Hashing an arbitrary ``stdin.bin`` proves only that the file was retained;
    it does not prove that the exchange called this case's published tool with
    this case's canonical arguments.  Reparse all three protocol lines and
    compare the final ``tools/call`` envelope to the runner-owned request.
    """

    if (
        not isinstance(value, (bytes, bytearray))
        or side not in SIDE_NAMES
        or not isinstance(case_id, str)
        or not case_id.strip()
        or not isinstance(action_id, str)
        or not action_id.strip()
        or not isinstance(request, Mapping)
        or not isinstance(tool, str)
        or not tool.strip()
        or (action_prepare is not None and not isinstance(action_prepare, Mapping))
    ):
        return False
    request_id = f"native-original/{side}/{case_id}/{action_id}"
    try:
        rows = [
            _strict_json_loads(line.decode("utf-8", errors="strict"))
            for line in bytes(value).splitlines()
            if line.strip()
        ]
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError, TypeError):
        return False
    if len(rows) != 3 or not all(isinstance(row, Mapping) for row in rows):
        return False
    initialize, notification, call = rows
    call_params = dict(call).get("params")
    expected_meta = None
    if isinstance(action_prepare, Mapping):
        expected_meta = {
            "nativeOriginalActionNonce": action_prepare.get("nonce"),
            "nativeOriginalActionDigest": action_prepare.get("action_digest"),
            "nativeOriginalActionRoute": action_prepare.get("route"),
            "nativeOriginalActionScope": action_prepare.get("scope"),
            "nativeOriginalActionEntrypoint": action_prepare.get("entrypoint"),
        }
        observed_meta = call_params.get("_meta") if isinstance(call_params, Mapping) else None
        if not isinstance(observed_meta, Mapping) or not _json_semantic_equal(dict(observed_meta), expected_meta):
            return False
    elif isinstance(call_params, Mapping) and "_meta" in call_params:
        return False
    return (
        dict(initialize).get("jsonrpc") == "2.0"
        and dict(initialize).get("id") == f"{request_id}/initialize"
        and dict(initialize).get("method") == "initialize"
        and dict(notification).get("jsonrpc") == "2.0"
        and "id" not in notification
        and dict(notification).get("method") == "notifications/initialized"
        and dict(call).get("jsonrpc") == "2.0"
        and dict(call).get("id") == request_id
        and dict(call).get("method") == "tools/call"
        and isinstance(call_params, Mapping)
        and call_params.get("name") == tool
        and isinstance(call_params.get("arguments"), Mapping)
        and _json_semantic_equal(call_params["arguments"], request)
    )


def _response_status(process_result: Mapping[str, Any], response: Any | None, parse_error: str | None) -> str:
    process_status = process_result.get("status") if isinstance(process_result, Mapping) else None
    if process_status == "censored":
        return "censored"
    if process_status == "unknown":
        return "unknown"
    if parse_error:
        return "unknown"
    if response is None:
        return "unknown"
    if not isinstance(response, (Mapping, list)):
        return "unknown"
    parsed_status, _ = _parsed_terminal_status(response)
    if process_status == "completed":
        return parsed_status or "completed"
    if process_status == "error":
        # A nonzero process is comparable only when the payload itself
        # carries a recognized typed production error.  A status string such
        # as ``cancelled`` or ``unsupported`` without that envelope is still
        # an arbitrary transport payload and cannot become a terminal result.
        if _recognized_production_error(response):
            return "error"
        return "unknown"
    return "unknown"


def _availability(route: Mapping[str, Any], side: str) -> str:
    _require(isinstance(route, Mapping), "route must be an object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    raw = route.get("availability")
    if isinstance(raw, str):
        return raw
    if isinstance(raw, Mapping):
        value = raw.get(side, "unknown")
        return value if isinstance(value, str) else "unknown"
    return "unknown"


def _route_context(
    *,
    side: str,
    binary: Path,
    case: Mapping[str, Any],
    action: Mapping[str, Any],
    side_dir: Path,
    artifact_dir: Path,
    request_file: Path,
    source_root: Path | None = None,
    bindings: Mapping[str, Any] | None = None,
    authority_identity: Mapping[str, Any] | None = None,
    authority_challenge: str | None = None,
) -> dict[str, Any]:
    project_root = _owned_relative_path(
        case.get("project_root"), side_dir=side_dir, field="project_root", default="project"
    )
    profile_root = _owned_relative_path(
        case.get("profile_root"), side_dir=side_dir, field="profile_root", default="profile"
    )
    state_root = _owned_relative_path(
        case.get("state_root"), side_dir=side_dir, field="state_root", default="state"
    )
    try:
        for path in (project_root, profile_root, state_root):
            path.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise RunnerError(f"cannot create private case roots: {error}") from error
    process_root = (side_dir / "process").resolve()
    try:
        process_root.mkdir(parents=True, exist_ok=True)
        resolved_source_root = source_root.resolve() if source_root is not None else None
        binary_root = binary.resolve().parent
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise RunnerError(f"cannot resolve private case roots: {error}") from error
    daemon_socket = _daemon_socket_path(side_dir)
    authority_digest = _authority_identity_digest(authority_identity)
    authority_binding = {
        "authority_digest": authority_digest,
        "endpoint": copy.deepcopy(authority_identity.get("endpoint"))
        if isinstance(authority_identity, Mapping)
        else None,
        "profile_root": str(profile_root),
        "process_root": str(process_root),
        "binary_root": str(binary_root),
        "store_root": str(profile_root),
    }
    return {
        "binary": str(binary),
        "side": side,
        "case_id": case.get("id", case.get("case_id")),
        "action_id": action.get("id", action.get("action_id")),
        "project_root": str(project_root),
        "profile_root": str(profile_root),
        "state_root": str(state_root),
        "source_root": str(resolved_source_root) if resolved_source_root is not None else None,
        "binary_root": str(binary_root),
        "process_root": str(process_root),
        "store_root": str(profile_root),
        "daemon_socket": str(daemon_socket),
        "artifact_root": str(artifact_dir),
        "request_file": str(request_file),
        # Output bindings are convenient request inputs, but protocol/auth
        # envelopes are runner-owned evidence.  Redact those keys before any
        # request/argv/environment template is rendered for a child.
        "bindings": _redact_render_secrets(dict(bindings or {})),
        "authority_identity": _redact_render_secrets(dict(authority_identity or {})) if isinstance(authority_identity, Mapping) else None,
        "authority_digest": authority_digest,
        "authority_binding": authority_binding,
        "authority_challenge": authority_challenge,
        "reference_revision": REFERENCE_REVISION,
        "product_revision": PRODUCT_REVISION,
    }


def _environment(
    side_dir: Path,
    action: Mapping[str, Any],
    context: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    _require(isinstance(action, Mapping), "action must be an object", RunnerError)
    _require(context is None or isinstance(context, Mapping), "action context must be an object", RunnerError)
    home = side_dir / "home"
    data = side_dir / "data"
    config = side_dir / "config"
    runtime = side_dir / "runtime"
    try:
        for path in (home, data, config, runtime):
            path.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise RunnerError(f"cannot create isolated environment roots: {error}") from error
    # Start from a scrubbed environment.  A caller-controlled PATH, loader
    # variable, Git selector, or TRACEDECAY override can otherwise redirect
    # the executable or make a child claim a different authority/store.
    environment = {
        key: value
        for key, value in os.environ.items()
        if not _unsafe_environment_key(key)
    }
    environment["PATH"] = os.defpath
    environment.update(
        {
            "HOME": str(home),
            "USERPROFILE": str(home),
            "XDG_DATA_HOME": str(data),
            "XDG_CONFIG_HOME": str(config),
            "XDG_RUNTIME_DIR": str(runtime),
            # The foreground daemon and every client in this side share the
            # case-owned profile root.  A separate XDG data root remains in
            # place for unrelated library caches, but it must never become an
            # ambient daemon authority for comparison traffic.
            "TRACEDECAY_DATA_DIR": str(
                (context or {}).get("profile_root", data / "tracedecay")
            ),
            "TRACEDECAY_GLOBAL_DB": str(
                Path(str((context or {}).get("profile_root", data / "tracedecay")))
                / "global.db"
            ),
            "TRACEDECAY_COMPARISON_SIDE_ROOT": str(side_dir),
            "TRACEDECAY_COMPARISON_PROCESS_ROOT": str(
                (context or {}).get("process_root", side_dir / "process")
            ),
            "TRACEDECAY_DAEMON_SOCKET": str(
                (context or {}).get("daemon_socket", _daemon_socket_path(side_dir))
            ),
            "TRACEDECAY_COMPARISON_AUTHORITY_DIGEST": str(
                (context or {}).get("authority_digest", "")
            ),
            "TRACEDECAY_COMPARISON_AUTHORITY_CHALLENGE": str(
                (context or {}).get("authority_challenge", "")
            ),
        }
    )
    overrides = action.get("environment", {})
    _require(isinstance(overrides, dict), "action environment must be an object")
    rendered = _render(overrides, context or {})
    _require(
        all(isinstance(key, str) and isinstance(value, str) for key, value in rendered.items()),
        "action environment keys and values must be strings",
        RunnerError,
    )
    forbidden = sorted((repr(key) for key in rendered if _unsafe_environment_key(key)))
    _require(
        not forbidden,
        "action cannot override runner-owned isolation variables: " + ", ".join(forbidden),
    )
    environment.update({str(key): str(value) for key, value in rendered.items()})
    return environment


def _argv_for(
    route: Mapping[str, Any],
    action: Mapping[str, Any],
    entrypoint: str,
    context: Mapping[str, Any],
) -> list[str]:
    _require(isinstance(route, Mapping), "route must be an object", RunnerError)
    _require(isinstance(action, Mapping), "action must be an object", RunnerError)
    _require(isinstance(context, Mapping), "action context must be an object", RunnerError)
    for key in ("binary", "project_root", "request_file"):
        _require(
            isinstance(context.get(key), str) and bool(context[key]),
            f"action context is missing {key}",
            RunnerError,
        )
    _require(entrypoint in ("cli_tool", "mcp_stdio", "command"), "action entrypoint is invalid", RunnerError)
    entrypoints = route.get("entrypoints", {})
    _require(isinstance(entrypoints, Mapping), f"route {route.get('id')!r} entrypoints are malformed", RunnerError)
    endpoint = entrypoints.get(entrypoint, {})
    _require(
        isinstance(endpoint, Mapping),
        f"route {route.get('id')!r} entrypoint is malformed",
        RunnerError,
    )
    if entrypoint == "cli_tool":
        tool = endpoint.get("tool", route.get("tool"))
        _require(isinstance(tool, str) and tool, f"route {route['id']} has no CLI tool name")
        return [
            context["binary"],
            "tool",
            "--project",
            context["project_root"],
            tool,
            "--args",
            "@" + context["request_file"],
            "--json",
        ]
    if entrypoint == "mcp_stdio":
        template = endpoint.get("argv_template", ["${binary}", "serve", "--path", "${project_root}"])
        return [str(item) for item in _render(template, context)]
    argv = action.get("argv", endpoint.get("argv_template"))
    _require(isinstance(argv, list) and argv, f"route {route['id']} command entrypoint requires argv")
    rendered = [str(item) for item in _render(argv, context)]
    if action.get("includes_binary") is True or any(item in ("{binary}", "${binary}") for item in argv):
        return rendered
    return [context["binary"], *rendered]


def _mcp_input(
    action: Mapping[str, Any],
    context: Mapping[str, Any],
    request: Mapping[str, Any],
    route: Mapping[str, Any] | None = None,
    action_prepare: Mapping[str, Any] | None = None,
) -> bytes:
    _require(isinstance(action, Mapping), "MCP action must be an object", RunnerError)
    _require(isinstance(context, Mapping), "MCP context must be an object", RunnerError)
    _require(isinstance(request, Mapping), "MCP request must be an object", RunnerError)
    _require(route is None or isinstance(route, Mapping), "MCP route must be an object", RunnerError)
    _require(
        action_prepare is None or isinstance(action_prepare, Mapping),
        "MCP action prepare evidence must be an object",
        RunnerError,
    )
    for key in ("side", "case_id", "action_id"):
        _require(
            isinstance(context.get(key), str) and bool(context[key]),
            f"MCP context is missing {key}",
            RunnerError,
        )
    request_id = f"native-original/{context['side']}/{context['case_id']}/{context['action_id']}"
    initialize = {
        "jsonrpc": "2.0",
        "id": f"{request_id}/initialize",
        "method": "initialize",
        "params": {
            "protocolVersion": action.get("protocol_version", "2025-06-18"),
            "capabilities": {},
            "clientInfo": {"name": "tracedecay-native-original-runner", "version": "1"},
        },
    }
    entrypoints = (route or {}).get("entrypoints", {})
    _require(isinstance(entrypoints, Mapping), "MCP route entrypoints must be an object", RunnerError)
    endpoint = entrypoints.get("mcp_stdio", {})
    _require(isinstance(endpoint, Mapping), "MCP route entrypoint is malformed", RunnerError)
    tool_name = action.get("tool")
    _require(isinstance(tool_name, str) and tool_name, "mcp_stdio action requires tool")
    published_tool = endpoint.get("tool") if isinstance(endpoint, Mapping) else None
    _require(
        tool_name == published_tool,
        f"mcp_stdio action tool {tool_name!r} does not match published tool {published_tool!r}",
    )
    call = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": request},
    }
    if action_prepare is not None:
        nonce = action_prepare.get("nonce")
        action_digest = action_prepare.get("action_digest")
        route_id = action_prepare.get("route")
        scope = action_prepare.get("scope")
        entrypoint = action_prepare.get("entrypoint")
        _require(
            isinstance(nonce, str) and re.fullmatch(r"[0-9a-f]{64}", nonce) is not None
            and isinstance(action_digest, str) and re.fullmatch(r"[0-9a-f]{64}", action_digest) is not None
            and isinstance(route_id, str) and bool(route_id.strip())
            and isinstance(scope, str) and bool(scope.strip())
            and isinstance(entrypoint, str) and bool(entrypoint.strip()),
            "MCP action prepare evidence is incomplete",
            RunnerError,
        )
        call["params"]["_meta"] = {
            "nativeOriginalActionNonce": nonce,
            "nativeOriginalActionDigest": action_digest,
            "nativeOriginalActionRoute": route_id,
            "nativeOriginalActionScope": scope,
            "nativeOriginalActionEntrypoint": entrypoint,
        }
    notification = {"jsonrpc": "2.0", "method": "notifications/initialized"}
    return b"".join(_json_bytes(item) + b"\n" for item in (initialize, notification, call))


_STORE_IDENTITY_KEYS = frozenset(
    {
        "store_identity",
        "store_id",
        "store_digest",
        "database_identity",
        "database_id",
        "db_identity",
        "db_path",
    }
)


def _usable_store_identity(value: Any, _depth: int = 0) -> bool:
    if _depth > 64:
        return False
    if value is None or value is _MISSING:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        # A bare number is not a physical store identity.  Accepting it would
        # let an arbitrary counter or status code stand in for the database
        # root/digest that the comparison must distinguish.
        return False
    if isinstance(value, Mapping):
        if "observed" in value:
            return value.get("observed") is True and _usable_store_identity(value.get("value"), _depth + 1)
        return bool(value) and any(_usable_store_identity(item, _depth + 1) for item in value.values())
    if isinstance(value, (list, tuple)):
        return bool(value) and any(_usable_store_identity(item, _depth + 1) for item in value)
    return False


def _same_physical_store(left: Any, right: Any) -> bool:
    """Compare store identity values, resolving path aliases when present."""

    if _json_semantic_equal(left, right):
        return True
    path_keys = {"path", "db_path", "database_path", "store_path", "root"}

    def paths(value: Any, _depth: int = 0) -> list[Path]:
        if _depth > 64:
            raise RunnerError("store identity is too deeply nested")
        found: list[Path] = []
        if isinstance(value, str) and value.strip():
            try:
                candidate = Path(value).expanduser()
            except (TypeError, ValueError):
                candidate = None
            if candidate is not None and candidate.is_absolute():
                try:
                    found.append(candidate.resolve())
                except (OSError, RuntimeError, ValueError):
                    pass
            return found
        if isinstance(value, Mapping):
            for key, nested in value.items():
                if str(key).lower() in path_keys and isinstance(nested, str) and nested:
                    try:
                        candidate = Path(nested).expanduser()
                    except (TypeError, ValueError):
                        continue
                    if candidate.is_absolute():
                        try:
                            found.append(candidate.resolve())
                        except (OSError, RuntimeError, ValueError):
                            pass
                found.extend(paths(nested, _depth + 1))
        elif isinstance(value, (list, tuple)):
            for nested in value:
                found.extend(paths(nested, _depth + 1))
        return found

    left_paths = paths(left)
    right_paths = paths(right)
    for left_path in left_paths:
        for right_path in right_paths:
            if left_path == right_path:
                return True
            try:
                if left_path.exists() and right_path.exists() and left_path.samefile(right_path):
                    return True
            except OSError:
                continue
    return False


def _extract_store_identity(value: Any, path: str = "", _depth: int = 0) -> dict[str, Any] | None:
    """Find an explicit store/database identity published by a route response."""

    if _depth > 64:
        return None
    if isinstance(value, Mapping):
        for key, nested in value.items():
            key_text = str(key).lower()
            if (
                (key_text in _STORE_IDENTITY_KEYS or key_text.endswith("_store_identity"))
                and _usable_store_identity(nested)
                and not (isinstance(nested, Mapping) and nested.get("observed") is False)
            ):
                return {
                    "observed": True,
                    "source": f"{path}/{key}" if path else f"/{key}",
                    "value": copy.deepcopy(nested),
                }
            if key_text == "store" and isinstance(nested, Mapping):
                for identity_key in ("identity", "id", "digest", "path"):
                    if identity_key in nested and _usable_store_identity(nested[identity_key]):
                        return {
                            "observed": True,
                            "source": f"{path}/{key}/{identity_key}" if path else f"/{key}/{identity_key}",
                            "value": copy.deepcopy(nested[identity_key]),
                        }
        for key, nested in value.items():
            if key in ("memory", "result", "outcome", "data", "metadata", "state", "store"):
                found = _extract_store_identity(nested, f"{path}/{key}" if path else f"/{key}", _depth + 1)
                if found is not None:
                    return found
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            found = _extract_store_identity(nested, f"{path}/{index}", _depth + 1)
            if found is not None:
                return found
    return None


def _observed_store_identity(result: Mapping[str, Any], side: str) -> dict[str, Any]:
    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    observations: list[dict[str, Any]] = []
    authority_protocols: list[Mapping[str, Any]] = []

    def accepted(identity: Any, side_result: Mapping[str, Any] | None = None) -> bool:
        if not isinstance(identity, Mapping):
            return False
        if identity.get("observed") is not True or not _usable_store_identity(identity.get("value")):
            return False
        if identity.get("authority_correlated") is not True:
            return False
        reachability = side_result.get("operation_reachability") if isinstance(side_result, Mapping) else None
        if isinstance(reachability, Mapping) and _authority_evidence_observed(side_result):
            binding = reachability.get("authority_binding")
            protocol = identity.get("protocol_evidence")
            if (
                isinstance(protocol, Mapping)
                and protocol.get("source") == "runner_owned_daemon_action_receipt"
            ):
                return bool(
                    _action_receipt_wrapper_valid(
                        protocol,
                        side=side,
                        route_id=reachability.get("route")
                        if isinstance(reachability.get("route"), str)
                        else None,
                        entrypoint=reachability.get("entrypoint")
                        if isinstance(reachability.get("entrypoint"), str)
                        else None,
                    )
                    and _verified_receipt_proof(protocol)
                    and isinstance(protocol.get("store_identity"), Mapping)
                    and _json_semantic_equal(protocol.get("store_identity"), identity)
                )
            action_protocol = side_result.get("authority_evidence") if isinstance(side_result, Mapping) else None
            if (
                isinstance(action_protocol, Mapping)
                and action_protocol.get("source") == "runner_owned_daemon_action_receipt"
            ):
                return bool(
                    _action_receipt_wrapper_valid(
                        action_protocol,
                        side=side,
                        route_id=reachability.get("route")
                        if isinstance(reachability.get("route"), str)
                        else None,
                        entrypoint=reachability.get("entrypoint")
                        if isinstance(reachability.get("entrypoint"), str)
                        else None,
                    )
                    and _verified_receipt_proof(action_protocol)
                    and _json_semantic_equal(action_protocol.get("store_identity"), identity)
                )
            return (
                isinstance(identity.get("authority_digest"), str)
                and identity.get("authority_digest") == reachability.get("authority_digest")
                and isinstance(identity.get("authority_source"), str)
                and identity.get("authority_source") == reachability.get("authority_source")
                and isinstance(binding, Mapping)
                and isinstance(protocol, Mapping)
                and protocol.get("protocol_observed") is True
                and all(
                    key in protocol and _json_semantic_equal(protocol.get(key), binding.get(key))
                    for key in (
                        "authority_digest",
                        "endpoint",
                        "profile_root",
                        "process_root",
                        "binary_root",
                        "store_root",
                    )
                )
            )
        return False

    direct = result.get("store_identity")
    if isinstance(direct, Mapping):
        direct_side = direct.get(side) if side in direct else direct
        if accepted(direct_side, result):
            observations.append(copy.deepcopy(dict(direct_side)))
            protocol = result.get("authority_evidence")
            if isinstance(protocol, Mapping):
                authority_protocols.append(protocol)
    for _, _, side_result in _iter_side_observations(result, side):
        identity = side_result.get("store_identity")
        if accepted(identity, side_result):
            observations.append(copy.deepcopy(dict(identity)))
            protocol = side_result.get("authority_evidence")
            if isinstance(protocol, Mapping):
                authority_protocols.append(protocol)
    if not observations:
        return {
            "observed": False,
            "reason": "no authority-correlated operation response published an explicit store identity",
        }
    return {
        "observed": True,
        "observations": observations,
        # Keep a representative observed value at the aggregate level.  The
        # aggregate itself is what the ledger carries; without this field a
        # valid operation observation was later rejected as an opaque wrapper
        # by track identity and ledger consumers.
        "value": copy.deepcopy(observations[0].get("value")),
        "values": [copy.deepcopy(item.get("value")) for item in observations],
        "digest": json_digest(observations),
        "authority_correlated": all(
            item.get("authority_correlated") is True for item in observations
        ),
        "authority_evidence": copy.deepcopy(
            authority_protocols[0]
            if authority_protocols
            else observations[0].get("protocol_evidence")
            if observations and isinstance(observations[0].get("protocol_evidence"), Mapping)
            else None
        ),
    }


def _authority_identity_digest(value: Any) -> str | None:
    """Hash only a complete runner-owned daemon identity.

    A response echoing a digest is useful evidence only when the digest was
    derived from the identity the runner actually observed.  Accepting an
    arbitrary mapping here would let a caller manufacture a self-asserted
    authority.  ``_OwnedDaemon.current_identity`` supplies this exact shape;
    malformed injected identities simply produce unavailable evidence.
    """

    if not isinstance(value, Mapping):
        return None
    pid = value.get("pid")
    epoch = value.get("epoch")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or isinstance(epoch, bool)
        or not isinstance(epoch, int)
        or epoch <= 0
    ):
        return None
    for field in (
        "side",
        "process_run_id",
        "version",
        "profile_root",
        "store_root",
        "process_root",
        "binary_sha256",
        "authority_path",
    ):
        item = value.get(field)
        if not isinstance(item, str) or not item.strip():
            return None
    if re.fullmatch(r"[0-9a-f]{64}", value["binary_sha256"]) is None:
        return None
    endpoint = value.get("endpoint")
    if not isinstance(endpoint, Mapping):
        return None
    kind = endpoint.get("kind")
    address = endpoint.get("address")
    if kind == "unix":
        if os.name == "nt" or not isinstance(address, str) or not address.strip():
            return None
        try:
            if not Path(address).is_absolute():
                return None
        except (OSError, RuntimeError, TypeError, ValueError):
            return None
    elif kind == "loopback":
        if not isinstance(address, str) or not address.strip():
            return None
    else:
        return None
    return json_digest(value)


def _observe_owned_authority(
    daemon: Any,
    initial_identity: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Read the live daemon identity independently of the child response.

    The action response is untrusted application output.  It may repeat the
    runner's endpoint or authority digest, but it cannot establish that the
    request reached the owned daemon.  The runner therefore re-reads the
    runner-owned authority record after every operation and requires the same
    live process, endpoint, profile, and identity epoch that was observed
    before rendering the request.
    """

    if daemon is None or not isinstance(initial_identity, Mapping):
        return {
            "observed": False,
            "reason": "no runner-owned daemon identity was available before the action",
        }
    try:
        final_identity = daemon.current_identity()
    except (OSError, RuntimeError, TypeError, ValueError, AttributeError):
        final_identity = None
    initial_digest = _authority_identity_digest(initial_identity)
    final_digest = _authority_identity_digest(final_identity)
    endpoint = final_identity.get("endpoint") if isinstance(final_identity, Mapping) else None
    observed = bool(
        initial_digest
        and final_digest
        and initial_digest == final_digest
        and isinstance(final_identity, Mapping)
        and _json_semantic_equal(dict(initial_identity), dict(final_identity))
        and _endpoint_connectable(endpoint)
    )
    return {
        "observed": observed,
        "source": "runner_owned_authority_record",
        "authority_digest": final_digest,
        "before": copy.deepcopy(dict(initial_identity)),
        "after": copy.deepcopy(dict(final_identity)) if isinstance(final_identity, Mapping) else None,
        "reason": None if observed else "runner-owned authority identity changed or was not independently readable after the action",
    }


def _usable_process_identity(value: Any) -> bool:
    """Accept only a complete runner-observed daemon identity."""

    return _authority_identity_digest(value) is not None


def _operation_reachability(
    *,
    side: str,
    route_id: Any,
    entrypoint: str,
    process: Mapping[str, Any],
    response: Any | None,
    status: str,
    authority_identity: Mapping[str, Any] | None = None,
    authority_binding: Mapping[str, Any] | None = None,
    authority_challenge: str | None = None,
    authority_challenge_response: str | None = None,
    independent_protocol_evidence: Mapping[str, Any] | None = None,
    independent_authority_observation: Mapping[str, Any] | None = None,
    action_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    _require(isinstance(process, Mapping), "process evidence must be an object", RunnerError)
    authority_digest = _authority_identity_digest(authority_identity)
    if _action_receipt_wrapper_valid(
        action_receipt,
        side=side,
        route_id=route_id if isinstance(route_id, str) else None,
        entrypoint=entrypoint,
    ):
        correlation = {
            "observed": True,
            "source": "runner_owned_daemon_action_receipt",
            "evidence": copy.deepcopy(dict(action_receipt)),
        }
    else:
        correlation = _response_authority_correlation(
            response,
            authority_digest,
            expected_binding=authority_binding,
            expected_challenge=authority_challenge,
            expected_challenge_response=authority_challenge_response,
            independent_protocol_evidence=independent_protocol_evidence,
        )
    independent_observed = bool(
        isinstance(independent_authority_observation, Mapping)
        and independent_authority_observation.get("observed") is True
        and independent_authority_observation.get("authority_digest") == authority_digest
    )
    observed = bool(
        response is not None
        and process.get("status") in ("completed", "error")
        and correlation["observed"] is True
        and independent_observed
    )
    return {
        "observed": observed,
        "side": side,
        "route": route_id,
        "entrypoint": entrypoint,
        "process_status": process.get("status"),
        "response_observed": response is not None,
        "authority_correlated": correlation["observed"],
        "authority_digest": authority_digest,
        "authority_source": correlation.get("source"),
        "authority_binding": copy.deepcopy(dict(authority_binding or {})),
        "authority_protocol_evidence": copy.deepcopy(correlation.get("evidence")),
        "independent_authority_observation": copy.deepcopy(
            dict(independent_authority_observation or {})
        ),
        "independent_authority_observed": independent_observed,
        "challenge_required": authority_challenge is not None,
        "challenge": authority_challenge,
        "challenge_response": authority_challenge_response,
        "status": status,
        "action_receipt": copy.deepcopy(dict(action_receipt))
        if isinstance(action_receipt, Mapping)
        else None,
    }


def _response_authority_correlation(
    response: Any | None,
    expected_digest: str | None,
    path: str = "",
    _depth: int = 0,
    *,
    expected_binding: Mapping[str, Any] | None = None,
    expected_challenge: str | None = None,
    expected_challenge_response: str | None = None,
    independent_protocol_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Require protocol evidence for the runner-bound daemon authority.

    A bare digest in a child response is only a self-assertion.  When a
    binding is supplied (the normal execution path), the response must carry
    a dedicated ``authority_evidence`` object that repeats the endpoint,
    binary/profile/store roots and marks the evidence as protocol-observed.
    """

    if expected_digest is None:
        return {"observed": False, "source": None, "reason": "runner authority was not available"}
    if _depth > 64:
        return {"observed": False, "source": None, "reason": "response authority payload is too deeply nested"}
    if isinstance(response, Mapping):
        if expected_binding is not None:
            candidate = response.get("authority_evidence")
            if isinstance(candidate, Mapping) and not _authority_protocol_matches(
                candidate,
                expected_digest=expected_digest,
                expected_binding=expected_binding,
                expected_challenge=expected_challenge,
                expected_challenge_response=expected_challenge_response,
                allow_legacy=False,
            ):
                return {
                    "observed": False,
                    "source": f"{path}/authority_evidence" if path else "/authority_evidence",
                    "reason": "response authority evidence conflicted with runner-owned protocol evidence",
                    "evidence": copy.deepcopy(dict(candidate)),
                }
            # A child response is application data and can repeat every
            # expected field.  Only the independently captured daemon wire
            # exchange establishes authority correlation.
            if _authority_protocol_matches(
                independent_protocol_evidence,
                expected_digest=expected_digest,
                expected_binding=expected_binding,
                expected_challenge=expected_challenge,
                expected_challenge_response=expected_challenge_response,
                allow_legacy=False,
            ):
                return {
                    "observed": True,
                    "source": "runner_owned_daemon_challenge",
                    "evidence": copy.deepcopy(dict(independent_protocol_evidence)),
                }
            return {
                "observed": False,
                "source": "/runner_owned_daemon_challenge",
                "reason": "authority correlation requires an independently observed daemon challenge response",
            }
        for key, value in response.items():
            key_text = str(key).lower()
            if key_text in ("authority_digest", "daemon_authority_digest", "authority_identity_digest"):
                return {
                    "observed": False,
                    "source": f"{path}/{key}" if path else f"/{key}",
                    "reason": "bare response authority digest is self-asserted and cannot establish ownership",
                }
        for key, nested in response.items():
            if key in ("result", "outcome", "data", "metadata", "memory", "state", "authority"):
                found = _response_authority_correlation(
                    nested,
                    expected_digest,
                    f"{path}/{key}" if path else f"/{key}",
                    _depth + 1,
                    expected_binding=expected_binding,
                    expected_challenge=expected_challenge,
                    expected_challenge_response=expected_challenge_response,
                    independent_protocol_evidence=independent_protocol_evidence,
                )
                if found["observed"] or found.get("source") is not None:
                    return found
    elif isinstance(response, list):
        for index, nested in enumerate(response):
            found = _response_authority_correlation(
                nested,
                expected_digest,
                f"{path}/{index}",
                _depth + 1,
                expected_binding=expected_binding,
                expected_challenge=expected_challenge,
                expected_challenge_response=expected_challenge_response,
                independent_protocol_evidence=independent_protocol_evidence,
            )
            if found["observed"] or found.get("source") is not None:
                return found
    return {"observed": False, "source": None, "reason": "response did not carry runner-owned authority protocol evidence"}


def _authority_protocol_matches(
    evidence: Any,
    *,
    expected_digest: str | None,
    expected_binding: Mapping[str, Any] | None,
    expected_challenge: str | None,
    expected_challenge_response: str | None,
    allow_legacy: bool = False,
) -> bool:
    """Validate an authority exchange captured by the runner itself.

    ``authority_evidence`` nested in a child result is never sufficient.  The
    strict path requires the runner-owned daemon challenge source together
    with hashes of the exact wire request and response; the legacy shape is
    retained only for old focused fixtures that explicitly opt in.
    """

    if not isinstance(evidence, Mapping) or evidence.get("protocol_observed") is not True:
        return False
    if expected_digest is None or expected_binding is None:
        return False
    required = {
        "authority_digest": expected_digest,
        "endpoint": expected_binding.get("endpoint"),
        "profile_root": expected_binding.get("profile_root"),
        "process_root": expected_binding.get("process_root"),
        "binary_root": expected_binding.get("binary_root"),
        "store_root": expected_binding.get("store_root"),
    }
    if not all(
        key in evidence and _json_semantic_equal(evidence.get(key), expected)
        for key, expected in required.items()
    ):
        return False
    if expected_challenge is not None:
        if (
            evidence.get("challenge") != expected_challenge
            or evidence.get("challenge_response") != expected_challenge_response
            or not isinstance(expected_challenge_response, str)
            or re.fullmatch(r"[0-9a-f]{64}", expected_challenge_response) is None
        ):
            return False
    if allow_legacy:
        return True
    if evidence.get("source") != "runner_owned_daemon_challenge":
        return False
    if evidence.get("challenge_response_scheme") != "sha256_exact_daemon_response_bytes":
        return False
    for key in ("request_sha256", "response_sha256"):
        if not isinstance(evidence.get(key), str) or re.fullmatch(r"[0-9a-f]{64}", evidence[key]) is None:
            return False
    if evidence.get("response_sha256") != evidence.get("challenge_response"):
        return False
    if (
        isinstance(evidence.get("response_bytes"), bool)
        or not isinstance(evidence.get("response_bytes"), int)
        or evidence.get("response_bytes") <= 0
        or not isinstance(evidence.get("request_id"), str)
        or not evidence.get("request_id")
        or evidence.get("response_jsonrpc") != "2.0"
    ):
        return False
    return True


def _action_receipt_digest(value: Mapping[str, Any]) -> str:
    """Hash a daemon one-shot receipt without a self-referential field."""

    payload = copy.deepcopy(dict(value))
    payload["receipt_sha256"] = None
    return json_digest(payload)


def _length_prefixed_digest(parts: Iterable[bytes]) -> str:
    """Hash the daemon protocol's canonical length-prefixed byte sequence."""

    digest = hashlib.sha256()
    for part in parts:
        _require(isinstance(part, bytes), "length-prefixed digest parts must be bytes", RunnerError)
        digest.update(len(part).to_bytes(8, "big", signed=False))
        digest.update(part)
    return digest.hexdigest()


def _reject_digest_floats(value: Any, path: str = "$", depth: int = 0) -> None:
    """Reject floats from action/result JSON until Rust number parity exists."""

    if depth > 64:
        raise RunnerError(f"digest-bearing JSON at {path} is too deeply nested")
    # ``bool`` is an integer subclass, so test it first.  Floats are not
    # admitted even when finite: Python and serde_json do not currently share
    # an explicit canonical lexical-number contract for digest inputs.
    if isinstance(value, float):
        raise RunnerError(f"digest-bearing JSON cannot contain floating-point value at {path}")
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if isinstance(key, float):
                raise RunnerError(f"digest-bearing JSON cannot contain floating-point key at {path}")
            _reject_digest_floats(nested, f"{path}.{key}", depth + 1)
        return
    if isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _reject_digest_floats(nested, f"{path}/{index}", depth + 1)


def _daemon_action_digest(
    *,
    scope: str,
    tool: str,
    entrypoint: str,
    arguments: Mapping[str, Any],
) -> str:
    """Compute the action digest defined by daemon-protocol/action_receipt.rs."""

    _reject_digest_floats(arguments)
    return _length_prefixed_digest(
        (
            DAEMON_ACTION_RECEIPT_FORMAT.encode("utf-8"),
            b"1",
            scope.encode("utf-8"),
            tool.encode("utf-8"),
            entrypoint.encode("utf-8"),
            _json_bytes(arguments),
        )
    )


def _canonical_wire_arguments(value: Any) -> dict[str, Any]:
    """Round-trip the exact MCP argument envelope before preparing an action.

    ``tools/call`` is serialized with :func:`_json_bytes`.  Hashing this
    parsed round-trip makes the preparation digest refer to the same object
    and canonical bytes that `_mcp_input` writes to child stdin, before the
    daemon applies its structural-identifier protection step.  A malformed
    scalar, non-object, or non-canonical value is rejected at the runner
    boundary instead of being allowed to drift between prepare and dispatch.
    """

    _reject_digest_floats(value)
    encoded = _json_bytes(value)
    try:
        parsed = _strict_json_loads(encoded.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError) as error:
        raise RunnerError(f"MCP action arguments cannot be canonicalized: {error}") from error
    _require(isinstance(parsed, Mapping), "MCP action arguments must be a JSON object", RunnerError)
    _require(
        _json_bytes(parsed) == encoded,
        "MCP action arguments changed during canonical wire serialization",
        RunnerError,
    )
    return dict(parsed)


def _daemon_result_payload(value: Any) -> Any:
    """Remove only daemon receipt metadata from a terminal JSON-RPC result."""

    try:
        payload = copy.deepcopy(value)
    except (RecursionError, TypeError, ValueError):
        return _MISSING
    if isinstance(payload, Mapping) and isinstance(payload.get("result"), Mapping):
        payload = copy.deepcopy(payload["result"])
    elif isinstance(payload, Mapping) and isinstance(payload.get("error"), Mapping):
        # ``McpServer::terminal_result_value`` hashes this compact terminal
        # error object before the receipt is attached under error.data.
        error = payload["error"]
        payload = {
            "code": error.get("code"),
            "message": error.get("message"),
            "data": copy.deepcopy(error.get("data")),
        }
    if isinstance(payload, Mapping):
        payload = dict(payload)
        metadata = payload.get("_meta")
        if isinstance(metadata, Mapping):
            metadata = dict(metadata)
            metadata.pop("nativeOriginalActionReceipt", None)
            if metadata:
                payload["_meta"] = metadata
            else:
                payload.pop("_meta", None)
        data = payload.get("data")
        if isinstance(data, Mapping):
            data = dict(data)
            data.pop("nativeOriginalActionReceipt", None)
            if data:
                payload["data"] = data
            else:
                payload["data"] = None
    return payload


def _daemon_result_digest(value: Any) -> str | None:
    payload = _daemon_result_payload(value)
    if payload is _MISSING:
        return None
    try:
        _reject_digest_floats(payload)
        return _length_prefixed_digest((_json_bytes(payload),))
    except (RunnerError, RecursionError, TypeError, ValueError):
        return None


def _daemon_receipt_mac(
    candidate: Mapping[str, Any],
) -> str | None:
    """Recompute the Rust ActionReceipt HMAC material for a prepared key."""

    key = candidate.get("_runner_proof_key")
    if not isinstance(key, (bytes, bytearray)):
        return None
    generation = candidate.get("daemon_generation")
    store = candidate.get("store_identity")
    if not isinstance(generation, Mapping) or not isinstance(store, Mapping):
        return None
    epoch = generation.get("epoch")
    process_run_id = generation.get("process_run_id")
    expires_at = candidate.get("expires_at")
    issued_at = candidate.get("issued_at")
    if (
        isinstance(epoch, bool)
        or not isinstance(epoch, int)
        or epoch <= 0
        or not isinstance(process_run_id, str)
        or isinstance(expires_at, bool)
        or not isinstance(expires_at, int)
        or expires_at <= 0
        or not isinstance(issued_at, int)
        or isinstance(issued_at, bool)
    ):
        return None
    values = (
        candidate.get("action_id"),
        candidate.get("route"),
        candidate.get("nonce"),
        candidate.get("action_digest"),
        candidate.get("scope"),
        candidate.get("tool"),
        candidate.get("entrypoint"),
        str(epoch),
        process_run_id,
        store.get("project_id") or "",
        store.get("project_root"),
        store.get("data_root"),
        store.get("graph_db_path"),
        store.get("serving_branch") or "",
        candidate.get("result_digest"),
        str(expires_at),
        str(issued_at),
    )
    if not all(isinstance(value, str) for value in values):
        return None
    material = b""
    for value in (DAEMON_ACTION_RECEIPT_FORMAT, "1", *values):
        value = value.encode("utf-8")
        material += len(value).to_bytes(8, "big") + value
    return hmac.new(bytes(key), material, hashlib.sha256).hexdigest()


def _daemon_receipt_sha256(candidate: Mapping[str, Any]) -> str | None:
    generation = candidate.get("daemon_generation")
    store = candidate.get("store_identity")
    receipt_mac = candidate.get("receipt_mac")
    if not isinstance(generation, Mapping) or not isinstance(store, Mapping) or not isinstance(receipt_mac, str):
        return None
    epoch = generation.get("epoch")
    process_run_id = generation.get("process_run_id")
    expires_at = candidate.get("expires_at")
    issued_at = candidate.get("issued_at")
    if (
        isinstance(epoch, bool)
        or not isinstance(epoch, int)
        or epoch <= 0
        or not isinstance(process_run_id, str)
        or isinstance(expires_at, bool)
        or not isinstance(expires_at, int)
        or expires_at <= 0
        or not isinstance(issued_at, int)
        or isinstance(issued_at, bool)
    ):
        return None
    values = (
        candidate.get("action_id"),
        candidate.get("route"),
        candidate.get("nonce"),
        candidate.get("action_digest"),
        candidate.get("scope"),
        candidate.get("tool"),
        candidate.get("entrypoint"),
        str(epoch),
        process_run_id,
        store.get("project_id") or "",
        store.get("project_root"),
        store.get("data_root"),
        store.get("graph_db_path"),
        store.get("serving_branch") or "",
        candidate.get("result_digest"),
        str(expires_at),
        str(issued_at),
        receipt_mac,
    )
    if not all(isinstance(value, str) for value in values):
        return None
    # Keep this byte-for-byte aligned with
    # ``action_receipt.rs::receipt_sha256``.  Every discriminator and field,
    # including the format, is an unsigned big-endian length-prefixed value.
    # Framing the format is part of the protocol domain separation; accepting
    # an unframed prefix would make the runner disagree with the daemon while
    # still producing a superficially valid 64-hex digest.
    return _length_prefixed_digest(
        (DAEMON_ACTION_RECEIPT_FORMAT.encode("utf-8"), b"1", *(value.encode("utf-8") for value in values))
    )


def _remember_verified_receipt(
    candidate: Mapping[str, Any],
    *,
    proof_key: bytes | bytearray,
) -> None:
    """Retain the runner-held proof binding for one accepted receipt.

    The proof key must never be serialized.  The retained record contains
    only hashes and public receipt fields, which is enough for later
    same-process artifact revalidation to distinguish a receipt authenticated
    by this runner from a child response that merely copied the shape.
    """

    if not isinstance(candidate, Mapping) or not isinstance(proof_key, (bytes, bytearray)):
        return
    receipt_sha = candidate.get("receipt_sha256")
    receipt_mac = candidate.get("receipt_mac")
    computed_sha = _daemon_receipt_sha256(candidate)
    if (
        not isinstance(receipt_sha, str)
        or re.fullmatch(r"[0-9a-f]{64}", receipt_sha) is None
        or not isinstance(receipt_mac, str)
        or re.fullmatch(r"[0-9a-f]{64}", receipt_mac) is None
        or computed_sha != receipt_sha
    ):
        return
    try:
        receipt_digest = json_digest(dict(candidate))
    except (RunnerError, TypeError, ValueError, RecursionError):
        return
    # Keep the map bounded for long readiness runs while preserving the
    # receipt proof for every artifact that can still be revalidated in this
    # process.  The most recent entries are the useful ones for the current
    # run; eviction causes a conservative unknown result on stale artifacts.
    if len(_VERIFIED_RECEIPT_PROOFS) >= 32_768:
        for key in tuple(_VERIFIED_RECEIPT_PROOFS)[:8_192]:
            _VERIFIED_RECEIPT_PROOFS.pop(key, None)
    _VERIFIED_RECEIPT_PROOFS[receipt_sha] = {
        "receipt_sha256": receipt_sha,
        "receipt_mac": receipt_mac,
        "receipt_canonical_sha256": receipt_digest,
        "proof_key_sha256": bytes_digest(bytes(proof_key)),
        "result_digest": str(candidate.get("result_digest")),
    }


def _verified_receipt_proof(
    wrapper: Any,
    *,
    response: Any | None = None,
) -> bool:
    """Check a public wrapper against a receipt MAC proof retained in memory."""

    if not isinstance(wrapper, Mapping):
        return False
    receipt = wrapper.get("receipt")
    if not isinstance(receipt, Mapping):
        # Compatibility fixtures that predate the signed daemon endpoint have
        # no MAC proof to revalidate.  They remain usable only for the small
        # shape-level helpers; production receipt wrappers always carry the
        # nested signed receipt and therefore take the strict branch.
        return True
    receipt_sha = receipt.get("receipt_sha256")
    if not isinstance(receipt_sha, str):
        return False
    proof = _VERIFIED_RECEIPT_PROOFS.get(receipt_sha)
    if not isinstance(proof, Mapping):
        return False
    try:
        canonical_sha = json_digest(dict(receipt))
    except (RunnerError, TypeError, ValueError, RecursionError):
        return False
    if (
        proof.get("receipt_sha256") != receipt_sha
        or proof.get("receipt_mac") != receipt.get("receipt_mac")
        or proof.get("receipt_canonical_sha256") != canonical_sha
        or proof.get("receipt_sha256") != _daemon_receipt_sha256(receipt)
        or wrapper.get("receipt_sha256") != receipt_sha
        or proof.get("result_digest") != receipt.get("result_digest")
    ):
        return False
    if response is not None:
        result_digest = _daemon_result_digest(response)
        if result_digest is None or result_digest != receipt.get("result_digest"):
            return False
    return True


def _validate_daemon_action_receipt(
    value: Any,
    *,
    side: str,
    route_id: str,
    entrypoint: str,
    action_id: str,
    request_sha256: str | None = None,
    daemon_identity: Mapping[str, Any] | None,
    action_digest: str | None = None,
    scope: str | None = None,
    tool: str | None = None,
    expires_at: int | None = None,
    store_identity: Mapping[str, Any] | None = None,
    result: Any | None = None,
    proof_key: bytes | bytearray | None = None,
) -> dict[str, Any] | None:
    """Validate a receipt returned by the owned daemon action endpoint.

    This is intentionally separate from the child operation response.  A
    child may repeat every field in its own JSON, but it cannot manufacture a
    receipt that the runner fetched over the authenticated daemon socket and
    matched to the exact request bytes.
    """

    if (
        not isinstance(value, Mapping)
        or side not in SIDE_NAMES
        or not isinstance(route_id, str)
        or not isinstance(entrypoint, str)
        or not isinstance(action_id, str)
        or not isinstance(daemon_identity, Mapping)
    ):
        return None
    if request_sha256 is not None and (
        not isinstance(request_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", request_sha256.lower()) is None
    ):
        return None
    if action_digest is not None and (
        not isinstance(action_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", action_digest.lower()) is None
    ):
        return None
    if expires_at is not None and (
        isinstance(expires_at, bool)
        or not isinstance(expires_at, int)
        or expires_at <= 0
    ):
        return None
    expected_store = None
    if store_identity is not None:
        expected_store = _daemon_store_value(store_identity)
        if expected_store is None:
            return None
    candidate = value.get("action_receipt") if isinstance(value.get("action_receipt"), Mapping) else value
    if not isinstance(candidate, Mapping):
        return None
    # The daemon protocol's normative receipt is emitted in the terminal MCP
    # result metadata.  Validate its signed generation/store/action fields
    # before accepting the runner wrapper.  A legacy runner-generated envelope
    # is intentionally not accepted: it had no daemon proof key and therefore
    # could be manufactured by a child response.
    if candidate.get("revision") == 1 and "receipt_mac" in candidate:
        if not isinstance(daemon_identity, Mapping):
            return None
        required = {
            "format",
            "revision",
            "action_id",
            "route",
            "nonce",
            "action_digest",
            "scope",
            "tool",
            "entrypoint",
            "daemon_generation",
            "store_identity",
            "result_digest",
            "expires_at",
            "issued_at",
            "receipt_mac",
            "receipt_sha256",
        }
        if not required.issubset(candidate):
            return None
        if (
            candidate.get("format") != DAEMON_ACTION_RECEIPT_FORMAT
            or not isinstance(candidate.get("action_id"), str)
            or not candidate.get("action_id", "").strip()
            or not isinstance(candidate.get("route"), str)
            or not candidate.get("route", "").strip()
            or candidate.get("action_id") != action_id
            or candidate.get("route") != route_id
            or candidate.get("entrypoint") != entrypoint
            or (action_digest is not None and candidate.get("action_digest") != action_digest)
            or (scope is not None and candidate.get("scope") != scope)
            or (tool is not None and candidate.get("tool") != tool)
        ):
            return None
        nonce = candidate.get("nonce")
        if not isinstance(nonce, str) or re.fullmatch(r"[0-9a-f]{64}", nonce) is None:
            return None
        if (
            not isinstance(candidate.get("action_digest"), str)
            or re.fullmatch(r"[0-9a-f]{64}", candidate["action_digest"]) is None
            or not isinstance(candidate.get("scope"), str)
            or not candidate["scope"].strip()
            or not isinstance(candidate.get("tool"), str)
            or not candidate["tool"].strip()
            or not isinstance(candidate.get("entrypoint"), str)
            or not candidate["entrypoint"].strip()
        ):
            return None
        generation = candidate.get("daemon_generation")
        if not isinstance(generation, Mapping):
            return None
        if (
            generation.get("epoch") != daemon_identity.get("epoch")
            or generation.get("process_run_id") != daemon_identity.get("process_run_id")
        ):
            return None
        store = _daemon_store_value(candidate.get("store_identity"))
        if store is None:
            return None
        if scope is not None and store.get("project_root") != scope:
            return None
        if expected_store is not None and not _json_semantic_equal(store, expected_store):
            return None
        candidate_expires_at = candidate.get("expires_at")
        if (
            isinstance(candidate_expires_at, bool)
            or not isinstance(candidate_expires_at, int)
            or candidate_expires_at <= 0
            or (expires_at is not None and candidate_expires_at != expires_at)
        ):
            return None
        if (
            not isinstance(candidate.get("issued_at"), int)
            or isinstance(candidate.get("issued_at"), bool)
            or candidate.get("issued_at") >= candidate_expires_at
        ):
            return None
        if (
            not isinstance(candidate.get("result_digest"), str)
            or re.fullmatch(r"[0-9a-f]{64}", candidate["result_digest"]) is None
        ):
            return None
        if (
            not isinstance(candidate.get("receipt_mac"), str)
            or re.fullmatch(r"[0-9a-f]{64}", candidate["receipt_mac"]) is None
            or not isinstance(candidate.get("receipt_sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", candidate["receipt_sha256"]) is None
        ):
            return None
        signed = dict(candidate)
        if proof_key is not None:
            signed["_runner_proof_key"] = bytes(proof_key)
            expected_mac = _daemon_receipt_mac(signed)
            if expected_mac is None or not hmac.compare_digest(expected_mac, candidate["receipt_mac"]):
                return None
        else:
            # Without the key held from prepare, receipt MAC verification is
            # impossible and a child-supplied metadata copy is only an
            # assertion.  Execution therefore fails closed.
            return None
        expected_receipt_sha = _daemon_receipt_sha256(candidate)
        if expected_receipt_sha != candidate.get("receipt_sha256"):
            return None
        if result is not None:
            expected_result_digest = _daemon_result_digest(result)
            if expected_result_digest is None or candidate.get("result_digest") != expected_result_digest:
                return None
        _remember_verified_receipt(candidate, proof_key=bytes(proof_key))
        return copy.deepcopy(dict(candidate))
    return None


def _extract_daemon_action_receipt(value: Any, _depth: int = 0) -> dict[str, Any] | None:
    """Find the daemon's terminal receipt in a JSON-RPC result or error.

    The receipt is attached by the daemon under the MCP result ``_meta``
    object.  A JSON-RPC error may carry the same metadata under ``error.data``.
    This helper deliberately recognises only the signed protocol shape; an
    arbitrary ``action_receipt`` object from a child is not evidence by itself.
    """

    if _depth > 64:
        return None
    if isinstance(value, Mapping):
        for key in ("nativeOriginalActionReceipt", "action_receipt"):
            candidate = value.get(key)
            if (
                isinstance(candidate, Mapping)
                and candidate.get("format") == DAEMON_ACTION_RECEIPT_FORMAT
                and candidate.get("revision") == 1
                and isinstance(candidate.get("receipt_mac"), str)
            ):
                return copy.deepcopy(dict(candidate))
        if (
            value.get("format") == DAEMON_ACTION_RECEIPT_FORMAT
            and value.get("revision") == 1
            and isinstance(value.get("receipt_mac"), str)
        ):
            return copy.deepcopy(dict(value))
        # Keep traversal bounded and deterministic.  The expected locations
        # are visited first so a copied child field cannot shadow a receipt
        # observed in the daemon result metadata.
        preferred = ("result", "error", "data", "_meta", "action_receipt")
        visited: set[int] = set()
        for key in preferred:
            if key not in value:
                continue
            nested = value[key]
            visited.add(id(nested))
            found = _extract_daemon_action_receipt(nested, _depth + 1)
            if found is not None:
                return found
        for nested in value.values():
            if id(nested) in visited:
                continue
            found = _extract_daemon_action_receipt(nested, _depth + 1)
            if found is not None:
                return found
    elif isinstance(value, (list, tuple)):
        for nested in value:
            found = _extract_daemon_action_receipt(nested, _depth + 1)
            if found is not None:
                return found
    return None


def _daemon_store_value(value: Any) -> dict[str, Any] | None:
    """Validate and copy the closed live-store identity from a receipt."""

    if not isinstance(value, Mapping):
        return None
    allowed = {"project_id", "project_root", "data_root", "graph_db_path", "serving_branch"}
    if set(value) - allowed:
        return None
    for field in ("project_root", "data_root", "graph_db_path"):
        item = value.get(field)
        if not isinstance(item, str) or not item.strip():
            return None
        try:
            if not Path(item).is_absolute():
                return None
        except (TypeError, ValueError, OSError):
            return None
    for field in ("project_id", "serving_branch"):
        item = value.get(field)
        if item is not None and (not isinstance(item, str) or not item.strip()):
            return None
    if _same_physical_store(value.get("data_root"), value.get("graph_db_path")):
        return None
    return copy.deepcopy(dict(value))


def _action_receipt_wrapper_valid(
    value: Any,
    *,
    side: str,
    route_id: str | None = None,
    entrypoint: str | None = None,
    action_id: str | None = None,
) -> bool:
    """Validate runner-owned wrapping around one signed daemon receipt.

    The terminal receipt itself is signed by the daemon using a proof key
    delivered only to the runner.  This wrapper records the runner's live
    identity/store observation and route dispatch.  Revalidation accepts the
    wrapper only when all of those bindings remain internally consistent; a
    response that merely echoes these fields never reaches this path.
    """

    if (
        not isinstance(value, Mapping)
        or side not in SIDE_NAMES
        or value.get("observed") is not True
        or value.get("source") != "runner_owned_daemon_action_receipt"
        or value.get("protocol_observed") is not True
        or value.get("format") != DAEMON_ACTION_RECEIPT_FORMAT
        or value.get("revision") != 1
    ):
        return False
    expected = {
        "route": route_id,
        "entrypoint": entrypoint,
        "action_id": action_id,
    }
    for key, wanted in expected.items():
        if wanted is not None and value.get(key) != wanted:
            return False
    receipt = value.get("receipt")
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("format") != DAEMON_ACTION_RECEIPT_FORMAT
        or receipt.get("revision") != 1
        or value.get("receipt_sha256") != receipt.get("receipt_sha256")
        or not isinstance(receipt.get("receipt_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", receipt["receipt_sha256"]) is None
    ):
        return False
    if any(
        value.get(key) != receipt.get(key)
        for key in (
            "route",
            "entrypoint",
            "action_id",
            "scope",
            "tool",
            "nonce",
            "action_digest",
        )
    ):
        return False
    expires_at = value.get("expires_at")
    if (
        isinstance(expires_at, bool)
        or not isinstance(expires_at, int)
        or expires_at <= 0
        or not isinstance(receipt.get("issued_at"), int)
        or isinstance(receipt.get("issued_at"), bool)
        or receipt.get("issued_at") >= expires_at
        or receipt.get("expires_at") != expires_at
    ):
        return False
    if (
        value.get("daemon_observed_route") != receipt.get("route")
        or value.get("daemon_observed_entrypoint") != receipt.get("entrypoint")
    ):
        return False
    daemon_identity = value.get("daemon_identity")
    authority_digest = value.get("authority_digest")
    if (
        not isinstance(daemon_identity, Mapping)
        or _authority_identity_digest(daemon_identity) != authority_digest
        or not isinstance(authority_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", authority_digest) is None
    ):
        return False
    generation = receipt.get("daemon_generation")
    if (
        not isinstance(generation, Mapping)
        or generation.get("epoch") != daemon_identity.get("epoch")
        or generation.get("process_run_id") != daemon_identity.get("process_run_id")
    ):
        return False
    store_identity = value.get("store_identity")
    if (
        not isinstance(store_identity, Mapping)
        or store_identity.get("observed") is not True
        or store_identity.get("authority_correlated") is not True
        or store_identity.get("authority_digest") != authority_digest
        or store_identity.get("authority_source") != value.get("source")
        or not isinstance(store_identity.get("value"), Mapping)
        or store_identity.get("digest") != json_digest(store_identity.get("value"))
    ):
        return False
    store_value = _daemon_store_value(store_identity.get("value"))
    receipt_store_value = _daemon_store_value(receipt.get("store_identity"))
    if store_value is None or receipt_store_value is None or not _json_semantic_equal(store_value, receipt_store_value):
        return False
    if receipt.get("scope") != receipt_store_value.get("project_root"):
        return False
    live_store_identity = _daemon_store_value(value.get("live_store_identity"))
    if live_store_identity is None or not _json_semantic_equal(live_store_identity, receipt_store_value):
        return False
    # Keep the authenticated prepare acknowledgement in the final wrapper's
    # evidence.  A receipt store that merely agrees with a child response is
    # insufficient: the runner must prove that the live store observed at
    # prepare, terminal issuance, and wrapper finalization is the same.
    prepared_store_identity = _daemon_store_value(value.get("prepared_store_identity"))
    if (
        prepared_store_identity is None
        or not _json_semantic_equal(prepared_store_identity, receipt_store_value)
    ):
        return False
    route_identity = value.get("route_identity")
    if (
        not isinstance(route_identity, Mapping)
        or route_identity.get("route") != value.get("route")
        or route_identity.get("entrypoint") != value.get("entrypoint")
        or route_identity.get("observed") is not True
        or route_identity.get("runner_owned") is not True
    ):
        return False
    for key in ("endpoint", "profile_root", "process_root", "binary_root", "store_root"):
        if key not in value or value.get(key) in (None, ""):
            return False
    # A serialized wrapper is not proof of MAC verification.  During an
    # actual run the validator registers the receipt proof before this
    # wrapper is emitted; reloads in this process must find that exact proof
    # or fail closed.  Shape-only compatibility fixtures without a nested
    # signed receipt remain covered by the legacy branch in
    # _verified_receipt_proof and never establish authenticated identity.
    if not _verified_receipt_proof(value):
        return False
    return True


def _authority_evidence_observed(side_result: Mapping[str, Any]) -> bool:
    """Return whether a side result carries runner-bound authority evidence."""

    if not isinstance(side_result, Mapping):
        return False
    reachability = side_result.get("operation_reachability")
    if not isinstance(reachability, Mapping):
        return False
    digest = reachability.get("authority_digest")
    source = reachability.get("authority_source")
    evidence = side_result.get("authority_evidence")
    binding = reachability.get("authority_binding")
    if not isinstance(evidence, Mapping):
        return False
    expected_roots = (
        "endpoint",
        "profile_root",
        "process_root",
        "binary_root",
        "store_root",
    )
    # The action-receipt protocol is the current execution authority.  Its
    # terminal receipt is independently MAC-checked before this wrapper is
    # emitted, and the wrapper records the live authority/store observations
    # used for artifact revalidation.  Keep the challenge branch below for
    # compatibility fixtures that do not execute a daemon action.
    if evidence.get("source") == "runner_owned_daemon_action_receipt":
        if not _action_receipt_wrapper_valid(
            evidence,
            side=side_result.get("side", reachability.get("side", "")),
            route_id=reachability.get("route"),
            entrypoint=reachability.get("entrypoint"),
        ):
            return False
        observation = reachability.get("independent_authority_observation")
        return bool(
            reachability.get("observed") is True
            and reachability.get("authority_correlated") is True
            and isinstance(digest, str)
            and digest == evidence.get("authority_digest")
            and reachability.get("authority_source") == evidence.get("source")
            and reachability.get("independent_authority_observed") is True
            and isinstance(observation, Mapping)
            and observation.get("observed") is True
            and observation.get("authority_digest") == digest
        )
    basic = (
        reachability.get("observed") is True
        and reachability.get("authority_correlated") is True
        and isinstance(digest, str)
        and re.fullmatch(r"[0-9a-f]{64}", digest) is not None
        and isinstance(source, str)
        and bool(source)
        and isinstance(binding, Mapping)
        and isinstance(evidence, Mapping)
        and evidence.get("protocol_observed") is True
        and reachability.get("independent_authority_observed") is True
        and isinstance(reachability.get("independent_authority_observation"), Mapping)
        and reachability["independent_authority_observation"].get("observed") is True
        and reachability["independent_authority_observation"].get("authority_digest") == digest
        and all(
            key in evidence and _json_semantic_equal(evidence.get(key), binding.get(key))
            for key in ("authority_digest", *expected_roots)
        )
    )
    if not basic:
        return False
    if not _authority_protocol_matches(
        evidence,
        expected_digest=digest,
        expected_binding=binding,
        expected_challenge=reachability.get("challenge")
        if reachability.get("challenge_required") is True
        else None,
        expected_challenge_response=reachability.get("challenge_response")
        if reachability.get("challenge_required") is True
        else None,
        allow_legacy=False,
    ):
        return False
    if reachability.get("challenge_required") is True:
        challenge = reachability.get("challenge")
        challenge_response = reachability.get("challenge_response")
        return (
            isinstance(challenge, str)
            and re.fullmatch(r"[0-9a-f]{64}", challenge) is not None
            and isinstance(challenge_response, str)
            and re.fullmatch(r"[0-9a-f]{64}", challenge_response) is not None
            and evidence.get("challenge") == challenge
            and evidence.get("challenge_response") == challenge_response
        )
    return True


def _authenticated_result_observation(side_result: Mapping[str, Any]) -> bool:
    """Require identity claims to be tied to a verified daemon result.

    Challenge/reachability records prove that a process was contacted, but
    they do not authenticate fields copied from that process's response.  A
    readiness identity is eligible only when the runner retained a complete
    signed action-receipt wrapper and the receipt's result digest recomputes
    from the exact response bytes after receipt metadata is removed.
    """

    if not isinstance(side_result, Mapping) or not _authority_evidence_observed(side_result):
        return False
    receipt_wrapper = side_result.get("action_receipt")
    if not isinstance(receipt_wrapper, Mapping):
        return False
    reachability = side_result.get("operation_reachability")
    side = side_result.get("side")
    if side is None and isinstance(reachability, Mapping):
        side = reachability.get("side")
    route = side_result.get("route")
    if route is None and isinstance(reachability, Mapping):
        route = reachability.get("route")
    entrypoint = side_result.get("entrypoint")
    if entrypoint is None and isinstance(reachability, Mapping):
        entrypoint = reachability.get("entrypoint")
    action_id = receipt_wrapper.get("action_id")
    if not (
        isinstance(side, str)
        and side in SIDE_NAMES
        and isinstance(route, str)
        and isinstance(entrypoint, str)
        and isinstance(action_id, str)
        and _action_receipt_wrapper_valid(
            receipt_wrapper,
            side=side,
            route_id=route,
            entrypoint=entrypoint,
            action_id=action_id,
        )
    ):
        return False
    response = side_result.get("response")
    receipt = receipt_wrapper.get("receipt")
    if response is None or not isinstance(receipt, Mapping):
        return False
    if not _verified_receipt_proof(receipt_wrapper, response=response):
        return False
    expected_result_digest = _daemon_result_digest(response)
    return bool(
        isinstance(expected_result_digest, str)
        and expected_result_digest == receipt.get("result_digest")
    )


def _route_identity(
    route: Mapping[str, Any],
    action: Mapping[str, Any],
    entrypoint: str | None,
    side: str = "original",
) -> dict[str, Any]:
    _require(isinstance(route, Mapping), "route identity requires a route object", RunnerError)
    _require(isinstance(action, Mapping), "route identity requires an action object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    entrypoints = route.get("entrypoints", {})
    _require(isinstance(entrypoints, Mapping), "route entrypoints must be an object", RunnerError)
    normalized_entrypoint = entrypoint
    if normalized_entrypoint is None and not entrypoints:
        # An unavailable matrix row still needs a typed, frozen route
        # identity.  ``None`` is ambiguous during ledger reload (it can mean
        # the runner never resolved the route), so use an explicit boundary
        # label for routes that publish no callable entrypoint.
        normalized_entrypoint = "unavailable"
    if normalized_entrypoint is not None:
        _require(isinstance(normalized_entrypoint, str) and bool(normalized_entrypoint), "route entrypoint must be a non-empty string", RunnerError)
        if normalized_entrypoint != "unavailable":
            _require(normalized_entrypoint in entrypoints, f"route does not publish entrypoint {normalized_entrypoint!r}", RunnerError)
    endpoint = entrypoints.get(normalized_entrypoint, {}) if normalized_entrypoint != "unavailable" else {}
    _require(isinstance(endpoint, Mapping), "route entrypoint must be an object", RunnerError)
    tool = endpoint.get("tool")
    if entrypoint == "mcp_stdio":
        _require(isinstance(tool, str) and bool(tool), "route MCP entrypoint does not publish a tool", RunnerError)
    route_digest = json_digest(
        {
            "route": route.get("id"),
            "entrypoint": normalized_entrypoint,
            "tool": tool,
            "operation_kind": route.get("operation_kind"),
            "classification": route.get("classification"),
            "availability": _availability(route, side),
        }
    )
    return {
        "route": route.get("id"),
        "entrypoint": normalized_entrypoint,
        "tool": tool,
        "published_tool": tool,
        "operation_kind": route.get("operation_kind"),
        "classification": route.get("classification"),
        "availability": _availability(route, side),
        "runner_owned": True,
        "contract_digest": route_digest,
        "observed": True,
    }


def _runner_identity_binding(
    *,
    side: str,
    binary: Path,
    route_identity: Mapping[str, Any],
    reachability: Mapping[str, Any],
    authority_evidence: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Create identity binding material measured by the runner.

    Provider responses can describe their own provider/store identity, but
    that description is not an independent observation.  Bind every such
    value to the runner's immutable executable, route contract, daemon
    protocol exchange, and post-action authority read before it is eligible
    for a readiness identity field.
    """

    _require(side in SIDE_NAMES, "identity binding side is invalid", RunnerError)
    _require(isinstance(route_identity, Mapping), "identity binding route identity is malformed", RunnerError)
    _require(isinstance(reachability, Mapping), "identity binding reachability is malformed", RunnerError)
    binary_ref = verify_binary(binary, side)
    authority_observation = reachability.get("independent_authority_observation")
    protocol = authority_evidence if isinstance(authority_evidence, Mapping) else {}
    observed = bool(
        route_identity.get("observed") is True
        and route_identity.get("runner_owned") is True
        and isinstance(route_identity.get("contract_digest"), str)
        and reachability.get("observed") is True
        and reachability.get("authority_correlated") is True
        and isinstance(authority_observation, Mapping)
        and authority_observation.get("observed") is True
        and isinstance(protocol, Mapping)
        and (
            protocol.get("protocol_observed") is True
            or protocol.get("source") == "runner_owned_daemon_action_receipt"
        )
    )
    return {
        "observed": observed,
        "source": "runner_identity_binding",
        "side": side,
        "binary_sha256": binary_ref.get("sha256"),
        "route_digest": route_identity.get("contract_digest"),
        "authority_digest": reachability.get("authority_digest"),
        "authority_observation_digest": json_digest(authority_observation) if isinstance(authority_observation, Mapping) else None,
        "protocol_evidence_digest": json_digest(protocol) if isinstance(protocol, Mapping) else None,
        "scope_digest": json_digest(
            {
                key: protocol.get(key)
                for key in (
                    "endpoint",
                    "profile_root",
                    "process_root",
                    "binary_root",
                    "store_root",
                )
            }
        ) if isinstance(protocol, Mapping) else None,
    }


def _runner_identity_binding_observed(
    side_result: Mapping[str, Any],
    *,
    side: str,
    binary_digest: str | None = None,
) -> bool:
    """Validate a response's identity binding against runner-owned evidence."""

    if not isinstance(side_result, Mapping):
        return False
    binding = side_result.get("runner_identity_binding")
    reachability = side_result.get("operation_reachability")
    route_identity = side_result.get("route_identity")
    protocol = side_result.get("authority_evidence")
    if not isinstance(binding, Mapping) or not isinstance(reachability, Mapping) or not isinstance(route_identity, Mapping):
        return False
    authority_observation = reachability.get("independent_authority_observation")
    if binary_digest is not None and binding.get("binary_sha256") != binary_digest:
        return False
    return bool(
        binding.get("observed") is True
        and binding.get("source") == "runner_identity_binding"
        and binding.get("side") == side
        and binding.get("route_digest") == route_identity.get("contract_digest")
        and binding.get("authority_digest") == reachability.get("authority_digest")
        and isinstance(binding.get("authority_observation_digest"), str)
        and isinstance(authority_observation, Mapping)
        and binding.get("authority_observation_digest") == json_digest(authority_observation)
        and isinstance(binding.get("protocol_evidence_digest"), str)
        and isinstance(protocol, Mapping)
        and binding.get("protocol_evidence_digest") == json_digest(protocol)
        and isinstance(binding.get("scope_digest"), str)
        and binding.get("scope_digest") == json_digest(
            {
                key: protocol.get(key)
                for key in (
                    "endpoint",
                    "profile_root",
                    "process_root",
                    "binary_root",
                    "store_root",
                )
            }
        )
    )


def _execute_side_action(
    *,
    side: str,
    binary: Path,
    contract: Mapping[str, Any],
    case: Mapping[str, Any],
    action: Mapping[str, Any],
    side_dir: Path,
    artifact_dir: Path,
    timeouts: ProcessTimeouts,
    bindings: Mapping[str, Any] | None = None,
    owned_daemon: _OwnedDaemon | None = None,
    source_root: Path | None = None,
) -> dict[str, Any]:
    _require(isinstance(action, Mapping), "action must be an object", RunnerError)
    _require(isinstance(case, Mapping), "case must be an object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    route_id = action.get("route", action.get("operation"))
    _require(isinstance(route_id, str) and bool(route_id.strip()), "action route must be a non-empty string", RunnerError)
    route = _route_map(contract).get(route_id)
    if route is None:
        artifact_dir.mkdir(parents=True, exist_ok=False)
        result = {
            "status": "invalid",
            "reason": f"unknown route {route_id!r}",
            "input": copy.deepcopy(dict(action)),
            "provider_contacted": False,
            "composition_reached": False,
            "operation_reachability": {
                "observed": False,
                "side": side,
                "route": route_id,
                "reason": "route was not published",
                "authority_correlated": False,
            },
            "route_identity": {
                "observed": False,
                "route": route_id,
            },
            "store_identity": {
                "observed": False,
                "reason": "route was not published",
            },
            "request_sent": False,
            "route": route_id,
            "response": None,
        }
        _write_json(artifact_dir / "result.json", result)
        return result
    availability = _availability(route, side)
    if availability in ("unsupported", "unknown"):
        status = availability
        artifact_dir.mkdir(parents=True, exist_ok=False)
        request = action.get("request", {})
        request_ref = _write_json(artifact_dir / "request.json", request)
        result = {
            "status": status,
            "reason": route.get(f"{side}_reason", route.get("reason", f"{side} route is {availability}")),
            "input": copy.deepcopy(dict(action)),
            "provider_contacted": False,
            "composition_reached": False,
            "operation_reachability": {
                "observed": False,
                "side": side,
                "route": route_id,
                "reason": f"route availability is {availability}",
                "authority_correlated": False,
            },
            "route_identity": _route_identity(route, action, None, side),
            "store_identity": {
                "observed": False,
                "reason": f"route availability is {availability}",
            },
            "request_sent": False,
            "route": route_id,
            "operation_kind": route.get("operation_kind"),
            "effect_policy": _route_effect_policy(route),
            "request": request,
            "request_sha256": request_ref["sha256"],
            "request_artifact": request_ref,
            "response": None,
        }
        _write_json(artifact_dir / "result.json", result)
        return result
    request = action.get("request", {})
    _require(isinstance(request, dict), f"action request must be an object: {case.get('id')}/{action.get('id', action.get('action_id'))}")
    request_path = artifact_dir / "request.json"
    # The daemon action receipt is the sole authority boundary for an owned
    # execution.  Render only runner-owned paths and the logical request here;
    # the prepare acknowledgement (especially its proof key) never enters the
    # child context or environment.  The expected terminal receipt is obtained
    # from the daemon's signed MCP response below.
    initial_authority_identity = (
        owned_daemon.current_identity() if owned_daemon is not None else None
    )
    if owned_daemon is not None:
        # Reject selectors on the action envelope as well as inside its request.
        # An ignored top-level session/thread field would still be an
        # ambiguous caller-selected scope for a receipt-backed action.
        _reject_receipt_scoped_selectors(action, "$action")
    context = _route_context(
        side=side,
        binary=binary,
        case=case,
        action=action,
        side_dir=side_dir,
        artifact_dir=artifact_dir,
        request_file=request_path,
        source_root=source_root,
        bindings=bindings,
        authority_identity=initial_authority_identity,
        authority_challenge=None,
    )
    rendered_request = _canonical_wire_arguments(_render(request, context))
    if owned_daemon is not None:
        # The signed receipt is bound to the daemon-selected project/store
        # scope.  There is no pre-prepare selector exchange in this runner, so
        # reject explicit session/thread aliases before reserving an action.
        _reject_receipt_scoped_selectors(rendered_request)
    request_ref = _write_json(request_path, rendered_request)
    entrypoint = action.get("entrypoint")
    if entrypoint is None:
        entrypoint = "command" if availability == "external_harness" else "cli_tool"
    if availability == "external_harness":
        entrypoint = "command"
    route_identity = _route_identity(route, action, entrypoint, side)
    published_tool = route_identity.get("published_tool")
    if entrypoint == "mcp_stdio":
        published_tool = _validate_mcp_tool(route, action, f"{side} action")
    elif not isinstance(published_tool, str) or not published_tool.strip():
        action_tool = action.get("tool")
        published_tool = action_tool if isinstance(action_tool, str) and action_tool.strip() else None

    action_prepare: dict[str, Any] | None = None
    action_prepare_error: str | None = None
    if owned_daemon is not None:
        if entrypoint != "mcp_stdio":
            action_prepare_error = (
                "owned daemon action receipt requires the published MCP tools/call entrypoint"
            )
        elif not isinstance(published_tool, str) or not published_tool.strip():
            action_prepare_error = "owned daemon action receipt requires a published tool identity"
        else:
            try:
                action_prepare = owned_daemon.prepare_action(
                    route_id=route_id,
                    entrypoint=entrypoint,
                    action_id=str(action.get("id", action.get("action_id", "action"))),
                    scope=context["project_root"],
                    tool=published_tool,
                    arguments=rendered_request,
                    timeout_seconds=min(timeouts.action_seconds, 5.0),
                )
            except (RunnerError, OSError, ValueError, TypeError, AttributeError) as error:
                action_prepare_error = f"owned daemon action prepare failed: {error}"
    if action_prepare_error is not None:
        # Do not execute a callable action after the runner failed to reserve
        # its one-shot daemon receipt.  Preserve the request and a typed,
        # fail-closed result so a side effect cannot be mistaken for a pass.
        artifact_dir.mkdir(parents=True, exist_ok=True)
        result = {
            "status": "unknown",
            "process_status": "not_started",
            "reason": action_prepare_error,
            "input": copy.deepcopy(dict(action)),
            "provider_contacted": False,
            "composition_reached": False,
            "operation_reachability": {
                "observed": False,
                "side": side,
                "route": route_id,
                "entrypoint": entrypoint,
                "process_status": "not_started",
                "response_observed": False,
                "authority_correlated": False,
                "authority_digest": _authority_identity_digest(initial_authority_identity),
                "authority_source": None,
                "authority_binding": copy.deepcopy(context.get("authority_binding", {})),
                "authority_protocol_evidence": None,
                "independent_authority_observation": {
                    "observed": False,
                    "reason": "action prepare failed before a callable process was started",
                },
                "independent_authority_observed": False,
                "challenge_required": False,
                "challenge": None,
                "challenge_response": None,
                "action_receipt": None,
                "status": "unknown",
            },
            "authority_evidence": None,
            "store_identity": {
                "observed": False,
                "authority_correlated": False,
                "reason": "daemon action receipt was not prepared",
            },
            "request_sent": False,
            "route": route_id,
            "route_identity": route_identity,
            "operation_kind": route.get("operation_kind"),
            "effect_policy": _route_effect_policy(route),
            "entrypoint": entrypoint,
            "logical_request": copy.deepcopy(request),
            "request": rendered_request,
            "request_sha256": request_ref["sha256"],
            "request_artifact": request_ref,
            "response": None,
            "response_parse_error": None,
            "output_bindings": copy.deepcopy(action.get("output_bindings", {})),
            "bindings": {},
            "binding_errors": [],
            "action_prepare": None,
            "action_prepare_error": action_prepare_error,
            "action_receipt": None,
            "action_receipt_error": "action receipt was unavailable because prepare failed",
            "daemon_identity": copy.deepcopy(initial_authority_identity),
            "roots": {
                "source_root": context.get("source_root"),
                "binary_root": context.get("binary_root"),
                "process_root": context.get("process_root"),
                "store_root": context.get("store_root"),
                "artifact_root": str(artifact_dir.resolve()),
                "project_root": context.get("project_root"),
                "profile_root": context.get("profile_root"),
                "state_root": context.get("state_root"),
                "socket_root": str(Path(context["daemon_socket"]).parent)
                if context.get("daemon_socket")
                else None,
            },
            "process": None,
            "cleanup": None,
        }
        result["runner_identity_binding"] = _runner_identity_binding(
            side=side,
            binary=binary,
            route_identity=route_identity,
            reachability=result["operation_reachability"],
            authority_evidence=None,
        )
        _write_json(artifact_dir / "result.json", result)
        return result
    argv = _argv_for(route, action, entrypoint, context)
    environment = _environment(side_dir, action, context)
    process_input = (
        _mcp_input(action, context, rendered_request, route, action_prepare=action_prepare)
        if entrypoint == "mcp_stdio"
        else None
    )
    cwd_value = _render(action.get("cwd", context["project_root"]), context)
    cwd = _owned_relative_path(cwd_value, side_dir=side_dir, field="cwd", default="project")
    process = run_process(
        argv,
        cwd=cwd,
        environment=environment,
        artifact_dir=artifact_dir / "process",
        timeout=timeouts,
        input_bytes=process_input,
        phase=action.get("phase", "operation"),
        # A CLI child normally contacts a daemon outside its process group.
        # The comparison wrapper owns a foreground daemon explicitly, so its
        # authority and process group are then part of the evidence and the
        # child is no longer a detached-runtime guess.
        detached_process_possible=entrypoint == "cli_tool" and owned_daemon is None,
    )
    if entrypoint == "mcp_stdio":
        response_id = f"native-original/{side}/{context['case_id']}/{context['action_id']}"
        response, parse_error = _mcp_response(
            Path(process["stdout"]["path"]).read_bytes(),
            response_id,
            allow_terminal_error=owned_daemon is not None and action_prepare is not None,
        )
    else:
        response, parse_error = _parse_json_bytes(Path(process["stdout"]["path"]).read_bytes())
    status = _response_status(process, response, parse_error)
    response_terminal_status, response_terminal_source = _parsed_terminal_status(response)
    if response_terminal_status is not None and process.get("status") == "completed":
        status = response_terminal_status
    route_identity = _route_identity(route, action, entrypoint, side)
    independent_authority_observation = _observe_owned_authority(
        owned_daemon,
        initial_authority_identity,
    )
    final_authority_identity = (
        owned_daemon.current_identity() if owned_daemon is not None else None
    )
    validated_action_receipt: dict[str, Any] | None = None
    action_receipt_error: str | None = None
    if owned_daemon is not None:
        if action_prepare is None:
            action_receipt_error = "owned daemon action receipt was not prepared"
        else:
            raw_action_receipt = _extract_daemon_action_receipt(response)
            if raw_action_receipt is None:
                action_receipt_error = (
                    "owned daemon terminal response did not carry a signed action receipt"
                )
            else:
                receipt_identity = (
                    final_authority_identity
                    if isinstance(final_authority_identity, Mapping)
                    else initial_authority_identity
                )
                try:
                    validated_action_receipt = _validate_daemon_action_receipt(
                        raw_action_receipt,
                        side=side,
                        route_id=route_id,
                        entrypoint=entrypoint,
                        action_id=str(action.get("id", action.get("action_id", "action"))),
                        daemon_identity=receipt_identity,
                        action_digest=action_prepare.get("action_digest"),
                        scope=action_prepare.get("scope"),
                        tool=action_prepare.get("tool"),
                        expires_at=action_prepare.get("expires_at"),
                        store_identity=action_prepare.get("store_identity"),
                        result=response,
                        proof_key=action_prepare.get("_runner_proof_key"),
                    )
                except (RunnerError, OSError, ValueError, TypeError, AttributeError) as error:
                    action_receipt_error = f"owned daemon action receipt validation failed: {error}"
                if validated_action_receipt is None and action_receipt_error is None:
                    action_receipt_error = "owned daemon action receipt was invalid"
        if validated_action_receipt is None and status in ("completed", "error"):
            status = "unknown"
            action_receipt_error = action_receipt_error or "owned daemon action receipt was unavailable"

    action_receipt_wrapper: dict[str, Any] | None = None
    route_store_identity: dict[str, Any] | None = None
    if validated_action_receipt is not None:
        observed_identity = (
            final_authority_identity
            if isinstance(final_authority_identity, Mapping)
            else initial_authority_identity
        )
        store_value = _daemon_store_value(validated_action_receipt.get("store_identity"))
        authority_digest = _authority_identity_digest(observed_identity)
        if store_value is None or authority_digest is None or not isinstance(observed_identity, Mapping):
            action_receipt_error = "owned daemon action receipt lacked an independently usable store identity"
            validated_action_receipt = None
            if status in ("completed", "error"):
                status = "unknown"
        else:
            route_store_identity = {
                "observed": True,
                "source": "runner_owned_daemon_action_receipt",
                "value": copy.deepcopy(store_value),
                "digest": json_digest(store_value),
                "authority_correlated": True,
                "authority_digest": authority_digest,
                "authority_source": "runner_owned_daemon_action_receipt",
            }
            action_receipt_wrapper = {
                "observed": True,
                "protocol_observed": True,
                "source": "runner_owned_daemon_action_receipt",
                "format": DAEMON_ACTION_RECEIPT_FORMAT,
                "revision": 1,
                "side": side,
                "route": route_id,
                "entrypoint": entrypoint,
                "action_id": str(action.get("id", action.get("action_id", "action"))),
                "scope": validated_action_receipt.get("scope"),
                "tool": validated_action_receipt.get("tool"),
                "nonce": validated_action_receipt.get("nonce"),
                "action_digest": validated_action_receipt.get("action_digest"),
                # These fields are copied from the signed terminal receipt,
                # rather than asserted by the child.  They make route and
                # live-store continuity explicit when artifacts are reloaded.
                "daemon_observed_route": validated_action_receipt.get("route"),
                "daemon_observed_entrypoint": validated_action_receipt.get("entrypoint"),
                "expires_at": action_prepare.get("expires_at"),
                "authority_digest": authority_digest,
                "receipt_sha256": validated_action_receipt.get("receipt_sha256"),
                "receipt": copy.deepcopy(validated_action_receipt),
                "prepared_store_identity": copy.deepcopy(action_prepare.get("store_identity")),
                "store_identity": route_store_identity,
                "live_store_identity": copy.deepcopy(store_value),
                "daemon_identity": copy.deepcopy(dict(observed_identity)),
                "endpoint": copy.deepcopy(observed_identity.get("endpoint")),
                "profile_root": observed_identity.get("profile_root"),
                "process_root": observed_identity.get("process_root"),
                "binary_root": str(binary.resolve().parent),
                "store_root": observed_identity.get("store_root"),
                "route_identity": copy.deepcopy(route_identity),
                "wire_request_sha256": action_prepare.get("wire_request_sha256"),
                "wire_response_sha256": action_prepare.get("wire_response_sha256"),
            }
            if not _action_receipt_wrapper_valid(
                action_receipt_wrapper,
                side=side,
                route_id=route_id,
                entrypoint=entrypoint,
                action_id=action_receipt_wrapper["action_id"],
            ):
                action_receipt_error = "runner action receipt wrapper failed independent binding checks"
                action_receipt_wrapper = None
                route_store_identity = None
                if status in ("completed", "error"):
                    status = "unknown"
    operation_reachability = _operation_reachability(
        side=side,
        route_id=route_id,
        entrypoint=entrypoint,
        process=process,
        response=response,
        status=status,
        authority_identity=context.get("authority_identity"),
        authority_binding=context.get("authority_binding"),
        authority_challenge=None,
        authority_challenge_response=None,
        independent_protocol_evidence=None,
        independent_authority_observation=independent_authority_observation,
        action_receipt=action_receipt_wrapper,
    )
    authority_protocol = operation_reachability.get("authority_protocol_evidence")
    if owned_daemon is not None:
        # Never use a child response to establish store identity on an owned
        # daemon path.  The signed receipt's live-store coordinates are the
        # only accepted source.
        store_identity = route_store_identity or {
            "observed": False,
            "reason": action_receipt_error or "owned daemon store identity receipt was unavailable",
            "authority_correlated": False,
        }
    else:
        raw_store_identity = _extract_store_identity(response)
        if isinstance(raw_store_identity, Mapping):
            store_identity = {
                **dict(raw_store_identity),
                "observed": bool(
                    raw_store_identity.get("observed") is True
                    and operation_reachability.get("observed") is True
                    and isinstance(authority_protocol, Mapping)
                    and authority_protocol.get("protocol_observed") is True
                ),
                "raw_value": copy.deepcopy(raw_store_identity.get("value")),
                "authority_correlated": operation_reachability.get("authority_correlated") is True,
                "authority_digest": operation_reachability.get("authority_digest"),
                "authority_source": operation_reachability.get("authority_source"),
                "protocol_evidence": copy.deepcopy(authority_protocol),
            }
        else:
            store_identity = {
                "observed": False,
                "reason": "operation response did not publish protocol-correlated store identity",
                "raw_value": None,
                "authority_correlated": False,
                "protocol_evidence": copy.deepcopy(authority_protocol),
            }
    output_bindings = action.get("output_bindings", {})
    _require(isinstance(output_bindings, dict), "output_bindings must be an object")
    captured_bindings: dict[str, Any] = {}
    binding_errors: list[str] = []
    for name, pointer in output_bindings.items():
        value = json_pointer(response, pointer) if response is not None else _MISSING
        if value is _MISSING:
            binding_errors.append(f"{name} ({pointer})")
        else:
            captured_bindings[name] = copy.deepcopy(value)
    result = {
        "side": side,
        "status": status,
        "process_status": process["status"],
        "reason": process.get("reason") or parse_error,
        "input": copy.deepcopy(dict(action)),
        "provider_contacted": response is not None,
        "composition_reached": (
            response is not None
            and process.get("status") in ("completed", "error")
            and (owned_daemon is None or action_receipt_wrapper is not None)
        ),
        "operation_reachability": operation_reachability,
        "authority_evidence": copy.deepcopy(authority_protocol),
        "store_identity": store_identity,
        "request_sent": True,
        "route": route_id,
        "route_identity": route_identity,
        "operation_kind": route.get("operation_kind"),
        "effect_policy": _route_effect_policy(route),
        "entrypoint": entrypoint,
        "logical_request": copy.deepcopy(request),
        "request": rendered_request,
        "request_sha256": request_ref["sha256"],
        "request_artifact": request_ref,
        "response": response,
        "response_parse_error": parse_error,
        "response_terminal_status": response_terminal_status,
        "response_terminal_source": response_terminal_source,
        "output_bindings": copy.deepcopy(output_bindings),
        "bindings": captured_bindings,
        "binding_errors": binding_errors,
        "composition_proof": action.get("composition_proof"),
        "action_prepare": {
            key: copy.deepcopy(value)
            for key, value in action_prepare.items()
            if not str(key).startswith("_")
        }
        if isinstance(action_prepare, Mapping)
        else None,
        "action_prepare_error": action_prepare_error,
        "action_receipt": copy.deepcopy(action_receipt_wrapper),
        "action_receipt_error": action_receipt_error,
        "request_exchange_sha256": process.get("stdin_exchange_sha256")
        if entrypoint == "mcp_stdio"
        else None,
        "roots": {
            "source_root": context.get("source_root"),
            "binary_root": context.get("binary_root"),
            "process_root": context.get("process_root"),
            "store_root": context.get("store_root"),
            "artifact_root": str(artifact_dir.resolve()),
            "project_root": context.get("project_root"),
            "profile_root": context.get("profile_root"),
            "state_root": context.get("state_root"),
            "socket_root": str(Path(context["daemon_socket"]).parent)
            if context.get("daemon_socket")
            else None,
        },
        "daemon_identity": copy.deepcopy(final_authority_identity)
        if isinstance(final_authority_identity, Mapping)
        else copy.deepcopy(initial_authority_identity),
        "process": process,
        "cleanup": copy.deepcopy(process.get("cleanup")),
    }
    result["runner_identity_binding"] = _runner_identity_binding(
        side=side,
        binary=binary,
        route_identity=result["route_identity"],
        reachability=operation_reachability,
        authority_evidence=authority_protocol if isinstance(authority_protocol, Mapping) else None,
    )
    if binding_errors and status in ("completed", "error"):
        result["status"] = "unknown"
        result["reason"] = "declared output binding was absent: " + ", ".join(binding_errors)
    _write_json(artifact_dir / "result.json", result)
    return result


def _effect_pointers(comparison: Mapping[str, Any]) -> dict[str, list[str]]:
    _require(isinstance(comparison, Mapping), "effect comparison must be an object", RunnerError)
    result: dict[str, list[str]] = {}
    for field in (
        "effect_json_pointers",
        "receipt_json_pointers",
        "state_json_pointers",
        "no_effect_json_pointers",
    ):
        values = comparison.get(field, ())
        _require(isinstance(values, (list, tuple)), f"{field} must be a list", RunnerError)
        result[field] = []
        for pointer in values:
            _require(isinstance(pointer, str), f"{field} entries must be strings", RunnerError)
            _pointer_parts(pointer)
            result[field].append(pointer)
    return result


_TYPED_EFFECT_KEYS = {
    "effect": frozenset(
        {
            "effect",
            "effect_digest",
            "kind",
            "type",
            "operation",
            "status",
            "changed",
            "applied",
            "committed",
            "digest",
            "sha256",
        }
    ),
    "receipt": frozenset(
        {
            "receipt",
            "receipt_id",
            "id",
            "sequence",
            "offset",
            "digest",
            "receipt_digest",
            "sha256",
            "committed",
            "status",
        }
    ),
    "state": frozenset(
        {
            "state",
            "state_digest",
            "generation",
            "revision",
            "version",
            "current",
            "status",
            "digest",
            "sha256",
        }
    ),
}
_TYPED_EFFECT_CANONICAL_KEYS = {
    # ``effect``, ``receipt``, ``state`` and ``status`` are envelope labels;
    # by themselves they are not durable semantics.  Each strict observable
    # must carry at least one field from the corresponding route-specific set.
    "effect": frozenset(
        {
            "effect_digest",
            "kind",
            "type",
            "operation",
            "changed",
            "applied",
            "committed",
            "digest",
            "sha256",
        }
    ),
    "receipt": frozenset(
        {
            "receipt_id",
            "id",
            "sequence",
            "offset",
            "digest",
            "receipt_digest",
            "sha256",
            "committed",
        }
    ),
    "state": frozenset(
        {
            "state_digest",
            "generation",
            "revision",
            "version",
            "current",
            "digest",
            "sha256",
        }
    ),
}
_RETAINED_STATUS_VALUES = frozenset(RETAINED_OUTCOME_STATUS_V1) | frozenset(
    {"applied", "committed", "unchanged", "stable", "absent", "empty", "ok"}
)
_EFFECT_POINTER_FIELDS = (
    "effect_json_pointers",
    "receipt_json_pointers",
    "state_json_pointers",
    "no_effect_json_pointers",
)


def _typed_effect_observable(field: str, value: Any, *, strict: bool = True, _depth: int = 0) -> bool:
    """Accept only non-empty typed effect/receipt/state evidence.

    A pointer to an arbitrary response label is not a mutation proof.  The
    strict path requires either a canonical digest or a structured envelope
    with a field appropriate to the declared evidence kind.  Read-only
    no-effect probes use the looser path because ``false`` and zero are valid
    observations there.
    """

    kind = field[:-len("_json_pointers")] if field.endswith("_json_pointers") else field
    if _depth > 32 or value is _MISSING or value is None:
        return False
    if isinstance(value, float):
        return math.isfinite(value) if not strict else False
    if isinstance(value, bool):
        return not strict
    if isinstance(value, int):
        return not strict or kind == "state"
    if isinstance(value, str):
        if not value.strip():
            return False
        # A bare digest is a producer assertion with no durable payload to
        # authenticate.  Strict mutation evidence must be a typed object
        # whose digest is recomputed from the exact canonical fields.
        return not strict
    if isinstance(value, Mapping):
        if not value:
            return False
        if not strict:
            return all(
                isinstance(key, str)
                and _typed_effect_observable(field, item, strict=False, _depth=_depth + 1)
                for key, item in value.items()
            )
        if not all(isinstance(key, str) for key in value):
            return False
        keys = {str(key).lower() for key in value}
        canonical_keys = _TYPED_EFFECT_CANONICAL_KEYS.get(kind, frozenset())
        if not keys & canonical_keys:
            return False
        if not keys <= _TYPED_EFFECT_KEYS.get(kind, frozenset()):
            return False
        # Matrix rows name three independent durable artifacts.  A generic
        # ``id``, ``kind``, generation, or status label can be copied from a
        # response without proving the corresponding artifact existed.  The
        # strict path therefore requires the route-specific digest key (or a
        # direct 64-hex scalar handled above) for each evidence kind.
        required_digest_key = {
            "effect": "effect_digest",
            "receipt": "receipt_digest",
            "state": "state_digest",
        }.get(kind)
        if required_digest_key is not None and required_digest_key not in keys:
            return False
        # A declared digest must itself be a digest; an id/status/kind may be
        # an opaque but non-empty string, while null markers never establish
        # an observed effect.
        for key, item in value.items():
            key_text = str(key).lower()
            if key_text in {"effect", "receipt", "state"} and not isinstance(item, Mapping):
                return False
            if key_text in {"digest", "sha256", "effect_digest", "receipt_digest", "state_digest"}:
                if not isinstance(item, str) or re.fullmatch(r"[0-9a-f]{64}", item.lower()) is None:
                    return False
            elif key_text == "status":
                if not isinstance(item, str) or item.lower() not in _RETAINED_STATUS_VALUES:
                    return False
            elif key_text in {"changed", "applied", "committed", "current"}:
                if not isinstance(item, bool):
                    return False
            elif key_text in {"sequence", "offset", "generation", "revision", "version"}:
                if not isinstance(item, int) or isinstance(item, bool) or item < 0:
                    return False
            elif item is None:
                return False
            elif isinstance(item, str) and not item.strip():
                return False
        supplied_digest = value.get(required_digest_key) if required_digest_key is not None else None
        if (
            required_digest_key is None
            or not isinstance(supplied_digest, str)
            or supplied_digest.lower() != json_digest(
                {key: item for key, item in value.items() if key != required_digest_key}
            )
        ):
            return False
        return True
    if isinstance(value, (list, tuple)):
        if not value:
            return False
        return all(
            _typed_effect_observable(kind, item, strict=strict, _depth=_depth + 1)
            for item in value
        )
    return False


def _runner_durable_value(
    side_result: Mapping[str, Any],
    *,
    field: str,
    pointer: str,
    phase: str,
) -> Any:
    """Return a pointer from a runner-observed after/reopen snapshot.

    Response JSON is application output and can repeat an arbitrary digest.
    Mutation evidence is eligible only when the runner has a separately
    observed durable snapshot, authenticated by the daemon action receipt
    and by a digest of the exact snapshot value map.  This helper accepts the
    published ``values`` shape and a legacy direct-pointer shape solely for
    focused fixtures; production execution emits the published shape.
    """

    if not isinstance(side_result, Mapping):
        return _MISSING
    durable = side_result.get("runner_observed_durable")
    if not isinstance(durable, Mapping):
        return _MISSING
    if (
        durable.get("observed") is not True
        or durable.get("source") != "runner_owned_daemon_action_receipt"
        or durable.get("authority_correlated") is not True
    ):
        return _MISSING
    action_receipt = side_result.get("action_receipt")
    receipt_digest = action_receipt.get("receipt_sha256") if isinstance(action_receipt, Mapping) else None
    if (
        not isinstance(receipt_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", receipt_digest.lower()) is None
        or durable.get("action_receipt_sha256") != receipt_digest
    ):
        return _MISSING
    section = durable.get(phase)
    if not isinstance(section, Mapping) or section.get("observed") is not True:
        return _MISSING
    if section.get("source") == f"runner_observed_{phase}_checkpoint":
        section_receipt = section.get("action_receipt")
        section_response_digest = section.get("response_digest")
        section_receipt_sha = section.get("action_receipt_sha256")
        section_route = section.get("route")
        section_entrypoint = section.get("entrypoint")
        section_action_id = section.get("action_id")
        if (
            not isinstance(section_receipt, Mapping)
            or not isinstance(section_receipt_sha, str)
            or re.fullmatch(r"[0-9a-f]{64}", section_receipt_sha) is None
            or not isinstance(section_response_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", section_response_digest) is None
            or section.get("side") not in SIDE_NAMES
            or not isinstance(section_route, str)
            or not isinstance(section_entrypoint, str)
            or not isinstance(section_action_id, str)
            or section.get("authority_correlated") is not True
            or section.get("authority_digest") != section_receipt.get("authority_digest")
            or section_receipt.get("route") != section_route
            or section_receipt.get("entrypoint") != section_entrypoint
            or not _action_receipt_wrapper_valid(
                section_receipt,
                side=section["side"],
                route_id=section_route,
                entrypoint=section_entrypoint,
                action_id=section_action_id,
            )
            or section_receipt.get("receipt_sha256") != section_receipt_sha
            or not _verified_receipt_proof(section_receipt)
            or section_receipt.get("receipt", {}).get("result_digest") != section_response_digest
        ):
            return _MISSING
    values = section.get("values")
    if not isinstance(values, Mapping):
        return _MISSING
    declared_digest = section.get("sha256")
    if (
        not isinstance(declared_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", declared_digest.lower()) is None
        or declared_digest != json_digest(values)
    ):
        return _MISSING
    # Comparison declarations use ``*_json_pointers`` names while daemon
    # receipts publish the corresponding semantic section (effect/receipt/
    # state).  Bind the two spellings before looking up the exact pointer.
    lookup_field = field.removesuffix("_json_pointers") if isinstance(field, str) else field
    by_field = values.get(lookup_field)
    if isinstance(by_field, Mapping) and pointer in by_field:
        return copy.deepcopy(by_field[pointer])
    # A direct pointer map is accepted only as a convenience for the
    # runner's own serialized observation object; it remains covered by the
    # section digest above.
    if pointer in values:
        return copy.deepcopy(values[pointer])
    return _MISSING


def _compare_operation_effects(
    original: Mapping[str, Any],
    product: Mapping[str, Any],
    *,
    comparison: Mapping[str, Any] | None,
    route: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Enforce mutation receipts and explicit no-effect read evidence."""

    policy = _route_effect_policy(route)
    if policy not in ("required", "none", "retrieval"):
        return {
            "status": "effect_unknown",
            "reason": "route did not publish a recognized operation effect policy",
            "effect_policy": policy,
        }
    if not isinstance(comparison, Mapping) or comparison.get("effect_mode") != policy:
        return {
            "status": "effect_unknown",
            "reason": f"operation effect policy {policy!r} was not declared by the case",
            "effect_policy": policy,
        }
    pointers = _effect_pointers(comparison)
    if policy == "required":
        fields = ("effect_json_pointers", "receipt_json_pointers", "state_json_pointers")
    elif policy == "none":
        fields = ("no_effect_json_pointers",)
    else:
        fields = (
            ("effect_json_pointers",)
            if pointers["effect_json_pointers"]
            else ("no_effect_json_pointers",)
        )
    declared = [
        (field, pointer)
        for field in fields
        for pointer in pointers[field]
    ]
    if not declared:
        return {
            "status": "effect_unknown",
            "reason": f"{policy} operation has no declared effect/no-effect observables",
            "effect_policy": policy,
        }
    missing_original: list[str] = []
    missing_product: list[str] = []
    original_values: dict[str, Any] = {}
    product_values: dict[str, Any] = {}

    for field, pointer in declared:
        if policy == "required" or (
            policy == "retrieval" and field == "effect_json_pointers"
        ):
            durable_phase = "reopen" if field == "state_json_pointers" else "after"
            left = _runner_durable_value(
                original,
                field=field,
                pointer=pointer,
                phase=durable_phase,
            )
            right = _runner_durable_value(
                product,
                field=field,
                pointer=pointer,
                phase=durable_phase,
            )
        else:
            left = json_pointer(original.get("response"), pointer)
            right = json_pointer(product.get("response"), pointer)
        if not _typed_effect_observable(field, left, strict=policy == "required"):
            missing_original.append(pointer)
        else:
            original_values[pointer] = copy.deepcopy(left)
        if not _typed_effect_observable(field, right, strict=policy == "required"):
            missing_product.append(pointer)
        else:
            product_values[pointer] = copy.deepcopy(right)
    if missing_original or missing_product:
        return {
            "status": "effect_unknown" if bool(missing_original) == bool(missing_product) else "fail",
            "reason": "required operation effect/no-effect evidence was absent",
            "effect_policy": policy,
            "missing_original": missing_original,
            "missing_product": missing_product,
        }
    if not _json_semantic_equal(original_values, product_values):
        return {
            "status": "fail",
            "reason": "operation effect/no-effect evidence differs",
            "effect_policy": policy,
            "original": original_values,
            "product": product_values,
        }
    return {
        "status": "pass",
        "reason": f"{policy} operation effect policy was observed on both sides",
        "effect_policy": policy,
        "observed": original_values,
        "effect_digest": json_digest(original_values),
    }


def compare_results(
    original: Mapping[str, Any],
    product: Mapping[str, Any],
    comparison: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare semantic responses while retaining raw side evidence."""

    _require(isinstance(original, Mapping) and isinstance(product, Mapping), "comparison side result must be an object", RunnerError)
    _require(comparison is None or isinstance(comparison, Mapping), "comparison declaration must be an object", RunnerError)
    left_status, right_status = original.get("status"), product.get("status")
    for side_result, side_name in ((original, "left"), (product, "right")):
        parsed_status, _ = _parsed_terminal_status(side_result.get("response"))
        if parsed_status is not None and parsed_status != "completed":
            if side_name == "left":
                left_status = parsed_status
            else:
                right_status = parsed_status
    comparison = comparison or {}
    expected_terminal = comparison.get("expected_terminal")
    if expected_terminal is not None and (left_status != expected_terminal or right_status != expected_terminal):
        return {
            "status": "fail",
            "reason": f"observed terminal status does not meet expected_terminal={expected_terminal!r}",
            "expected_terminal": expected_terminal,
            "original_status": left_status,
            "product_status": right_status,
        }
    if left_status != right_status:
        return {
            "status": "fail",
            "reason": f"side outcome changed: original={left_status!r}, product={right_status!r}",
            "original_status": left_status,
            "product_status": right_status,
        }
    if left_status in (
        "unsupported",
        "unknown",
        "invalid",
        "censored",
        "cancelled",
        "partial",
        "effect_unknown",
        "blocked",
        "error",
    ):
        return {
            "status": left_status,
            "reason": original.get("reason") or product.get("reason") or f"both sides reported {left_status}",
            "original_status": left_status,
            "product_status": right_status,
        }
    if left_status not in ("completed", "error"):
        return {"status": "unknown", "reason": f"unrecognized side status: {left_status!r}"}
    left_response, right_response = original.get("response"), product.get("response")
    if left_response is None or right_response is None:
        return {"status": "unknown", "reason": "a side did not produce a parseable semantic response"}
    if not isinstance(left_response, (Mapping, list)) or not isinstance(right_response, (Mapping, list)):
        return {"status": "unknown", "reason": "a side produced a non-object/non-array semantic response"}
    if left_status == "error" and (
        not _recognized_production_error(left_response)
        or not _recognized_production_error(right_response)
    ):
        return {
            "status": "unknown",
            "reason": "nonzero process results require a recognized typed production error payload",
        }
    for side_result, side_name in ((original, "original"), (product, "product")):
        if side_result.get("binding_errors"):
            return {
                "status": "unknown",
                "reason": f"{side_name} output binding was missing: {side_result['binding_errors']}",
                "original_status": left_status,
                "product_status": right_status,
            }
    # A sparse projection is an assertion about the selected observables.  If
    # a case does not spell out a narrower required set, every selected path
    # is required; two responses that both omit the observable therefore do
    # not become a false pass.  Negative cases can explicitly provide an
    # empty/narrow required_json_pointers list together with expected_terminal.
    required = comparison["required_json_pointers"] if "required_json_pointers" in comparison else comparison.get("semantic_json_pointers", ())
    _require(isinstance(required, (list, tuple)), "required_json_pointers must be a list", RunnerError)
    for pointer in required:
        _require(isinstance(pointer, str), "required JSON pointers must be strings", RunnerError)
        _pointer_parts(pointer)
    missing_original = [pointer for pointer in required if json_pointer(left_response, pointer) is _MISSING]
    missing_product = [pointer for pointer in required if json_pointer(right_response, pointer) is _MISSING]
    if missing_original or missing_product:
        return {
            "status": "fail" if bool(missing_original) != bool(missing_product) else "unknown",
            "reason": "a required semantic observable was absent",
            "missing_original": missing_original,
            "missing_product": missing_product,
        }
    left_projection = semantic_projection(left_response, comparison, side="original")
    right_projection = semantic_projection(right_response, comparison, side="product")
    if not _json_semantic_equal(left_projection, right_projection):
        return {
            "status": "fail",
            "reason": "original and product semantic responses differ",
            "original_semantic": left_projection,
            "product_semantic": right_projection,
        }
    return {
        "status": "pass",
        "reason": "semantic responses agree at the selected production boundary",
        "semantic_sha256": json_digest(left_projection),
        "original_status": left_status,
        "product_status": right_status,
    }


def _checkpoint_specs(action: Mapping[str, Any], phase: str) -> list[dict[str, Any]]:
    _require(isinstance(action, Mapping), "action must be an object", RunnerError)
    checkpoints = action.get("checkpoints", {})
    if not isinstance(checkpoints, dict):
        return []
    value = checkpoints.get(phase, [])
    if isinstance(value, dict):
        return [copy.deepcopy(value)]
    _require(isinstance(value, (list, tuple)), f"checkpoint {phase} must be a list or object", RunnerError)
    _require(
        all(isinstance(item, Mapping) for item in value),
        f"checkpoint {phase} entries must be objects",
        RunnerError,
    )
    return [copy.deepcopy(dict(item)) for item in value]


def _status_priority(statuses: Iterable[str]) -> str:
    if isinstance(statuses, (str, bytes)) or statuses is None:
        raise RunnerError("status collection must be an iterable of status strings")
    try:
        observed = set(statuses)
    except (TypeError, ValueError) as error:
        raise RunnerError("status collection must be an iterable of status strings") from error
    order = (
        "fail",
        "invalid",
        "blocked",
        "effect_unknown",
        "partial",
        "cancelled",
        "unknown",
        "censored",
        "unsupported",
        "pass",
    )
    for candidate in order:
        if candidate in observed:
            return candidate
    return "unknown"


def _pair_comparison(
    original: Mapping[str, Any],
    product: Mapping[str, Any],
    *,
    comparison: Mapping[str, Any] | None,
    route: Mapping[str, Any] | None,
    case_classification: str,
) -> dict[str, Any]:
    """Compare a parity leg, keeping extension-only legs out of the denominator."""

    _require(isinstance(original, Mapping), "original comparison leg must be an object", RunnerError)
    _require(isinstance(product, Mapping), "product comparison leg must be an object", RunnerError)
    _require(comparison is None or isinstance(comparison, Mapping), "comparison declaration must be an object", RunnerError)
    _require(route is None or isinstance(route, Mapping), "comparison route must be an object", RunnerError)
    _require(isinstance(case_classification, str), "case classification must be a string", RunnerError)
    route_classification = route.get("classification") if route else None
    if route_classification == "host_extension" or case_classification == "host_extension":
        if original.get("status") in ("unknown", "unsupported"):
            return {
                "status": "unknown",
                "reason": "host-extension leg has no callable original counterpart; product evidence is retained separately",
                "original_status": original.get("status"),
                "product_status": product.get("status"),
                "product_leg": product.get("status"),
            }
    semantic = compare_results(original, product, comparison)
    if semantic.get("status") != "pass":
        return semantic
    reachability_missing = []
    for side, side_result in (("original", original), ("product", product)):
        reachability = side_result.get("operation_reachability")
        if (
            not isinstance(reachability, Mapping)
            or reachability.get("observed") is not True
            or not isinstance(reachability.get("authority_digest"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", reachability["authority_digest"])
            or not isinstance(reachability.get("authority_source"), str)
            or not reachability.get("authority_source")
            or not _authority_evidence_observed(side_result)
        ):
            reachability_missing.append(f"{side}.operation_reachability")
            if not _authority_evidence_observed(side_result):
                reachability_missing.append(f"{side}.authority_evidence")
        store_identity = side_result.get("store_identity")
        if not isinstance(store_identity, Mapping) or store_identity.get("observed") is not True:
            reachability_missing.append(f"{side}.store_identity")
        elif store_identity.get("authority_correlated") is not True:
            reachability_missing.append(f"{side}.store_identity.authority_correlated")
        elif (
            not isinstance(reachability, Mapping)
            or
            store_identity.get("authority_digest") != reachability.get("authority_digest")
            or store_identity.get("authority_source") != reachability.get("authority_source")
        ):
            reachability_missing.append(f"{side}.store_identity.authority_binding")
        route_identity = side_result.get("route_identity")
        if not isinstance(route_identity, Mapping) or route_identity.get("observed") is not True:
            reachability_missing.append(f"{side}.route_identity")
        elif route_identity.get("route") != reachability.get("route"):
            reachability_missing.append(f"{side}.route_identity.route")
        elif not isinstance(route_identity.get("entrypoint"), str) or not route_identity.get("entrypoint"):
            reachability_missing.append(f"{side}.route_identity.entrypoint")
        elif route_identity.get("runner_owned") is not True:
            reachability_missing.append(f"{side}.route_identity.runner_owned")
        elif route_identity.get("contract_digest") != json_digest(
            {
                "route": route_identity.get("route"),
                "entrypoint": route_identity.get("entrypoint"),
                "tool": route_identity.get("tool"),
                "operation_kind": route_identity.get("operation_kind"),
                "classification": route_identity.get("classification"),
                "availability": route_identity.get("availability"),
            }
        ):
            reachability_missing.append(f"{side}.route_identity.contract_digest")
        elif route_identity.get("entrypoint") == "mcp_stdio" and (
            not isinstance(route_identity.get("tool"), str)
            or route_identity.get("tool") != route_identity.get("published_tool")
        ):
            reachability_missing.append(f"{side}.route_identity.tool")
    original_store = original.get("store_identity")
    product_store = product.get("store_identity")
    if isinstance(original_store, Mapping) and isinstance(product_store, Mapping):
        original_values = original_store.get("value")
        product_values = product_store.get("value")
        if (
            _usable_store_identity(original_values)
            and _usable_store_identity(product_values)
            and _same_physical_store(original_values, product_values)
        ):
            reachability_missing.append("original/product.store_identity.same_physical_store")
    if isinstance(original_store, Mapping) and isinstance(product_store, Mapping):
        original_values = original_store.get("value")
        product_values = product_store.get("value")
        if not _usable_store_identity(original_values):
            reachability_missing.append("original.store_identity.value")
        if not _usable_store_identity(product_values):
            reachability_missing.append("product.store_identity.value")
    if reachability_missing:
        return {
            **semantic,
            "status": "unknown",
            "reason": "operation reachability/store identity was not observed on both sides",
            "missing_observations": reachability_missing,
        }
    effect = _compare_operation_effects(
        original,
        product,
        comparison=comparison,
        route=route,
    )
    if effect is None:
        return semantic
    if effect.get("status") != "pass":
        return {
            **semantic,
            "status": effect.get("status", "effect_unknown"),
            "reason": effect.get("reason"),
            "effect": effect,
        }
    return {**semantic, "effect": effect}


def _validate_reopen_pair(
    original_action: Mapping[str, Any] | None,
    product_action: Mapping[str, Any] | None,
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Reject lifecycle reopen specs that do not describe the same operation."""

    _require(
        original_action is None or isinstance(original_action, Mapping),
        "original reopen action must be an object",
        RunnerError,
    )
    _require(
        product_action is None or isinstance(product_action, Mapping),
        "product reopen action must be an object",
        RunnerError,
    )
    _require(contract is None or isinstance(contract, Mapping), "reopen contract must be an object", RunnerError)
    original_route = (
        original_action.get("route", original_action.get("operation"))
        if isinstance(original_action, Mapping)
        else None
    )
    product_route = (
        product_action.get("route", product_action.get("operation"))
        if isinstance(product_action, Mapping)
        else None
    )
    _require(
        original_route is None or isinstance(original_route, str),
        "original reopen route must be a string",
        RunnerError,
    )
    _require(
        product_route is None or isinstance(product_route, str),
        "product reopen route must be a string",
        RunnerError,
    )
    if original_route != product_route:
        return {
            "status": "invalid",
            "reason": "original and product reopen operations differ",
            "original_route": original_route,
            "product_route": product_route,
        }
    original_comparison = original_action.get("comparison") if isinstance(original_action, Mapping) else None
    product_comparison = product_action.get("comparison") if isinstance(product_action, Mapping) else None
    _require(
        original_comparison is None or isinstance(original_comparison, Mapping),
        "original reopen comparison must be an object",
        RunnerError,
    )
    _require(
        product_comparison is None or isinstance(product_comparison, Mapping),
        "product reopen comparison must be an object",
        RunnerError,
    )
    selected_comparison = original_comparison if original_comparison is not None else product_comparison
    if original_comparison is not None and not _json_semantic_equal(product_comparison, original_comparison):
        return {
            "status": "invalid",
            "reason": "original and product reopen comparison declarations differ",
        }
    if isinstance(original_action, Mapping) and isinstance(product_action, Mapping):
        def signature(action: Mapping[str, Any], side: str) -> dict[str, Any]:
            route_obj = _route_map(contract).get(action.get("route", action.get("operation"))) if isinstance(contract, Mapping) else None
            availability = _availability(route_obj, side) if isinstance(route_obj, Mapping) else None
            entrypoint = action.get("entrypoint")
            if entrypoint is None and availability not in ("unsupported", "unknown"):
                entrypoint = "command" if availability == "external_harness" else "cli_tool"
            entrypoints = route_obj.get("entrypoints", {}) if isinstance(route_obj, Mapping) else {}
            _require(
                isinstance(entrypoints, Mapping),
                "reopen route entrypoints must be an object",
                RunnerError,
            )
            endpoint = entrypoints.get(entrypoint, {})
            _require(isinstance(endpoint, Mapping), "reopen route entrypoint must be an object", RunnerError)
            if entrypoint == "mcp_stdio" and isinstance(route_obj, Mapping):
                _validate_mcp_tool(route_obj, action, "reopen")
            published_tool = endpoint.get("tool") if isinstance(endpoint, Mapping) else None
            return {
                "route": action.get("route", action.get("operation")),
                "entrypoint": entrypoint,
                "tool": action.get("tool", published_tool),
                "published_tool": published_tool,
                "argv": copy.deepcopy(action.get("argv", endpoint.get("argv_template") if isinstance(endpoint, Mapping) else None)),
                "request": copy.deepcopy(action.get("request", {})),
                "request_bytes_sha256": bytes_digest(_json_bytes(action.get("request", {})) + b"\n"),
                "output_bindings": copy.deepcopy(action.get("output_bindings", {})),
                "bindings": copy.deepcopy(action.get("bindings", {})),
                "environment": copy.deepcopy(action.get("environment", {})),
                "cwd": action.get("cwd"),
                "includes_binary": action.get("includes_binary"),
                "protocol_version": action.get("protocol_version"),
                "comparison": copy.deepcopy(action.get("comparison")),
                "side": action.get("side"),
            }
        original_signature = signature(original_action, "original")
        product_signature = signature(product_action, "product")
        differences = sorted(
            key
            for key in original_signature
            if not _json_semantic_equal(original_signature.get(key), product_signature.get(key))
        )
        if differences:
            return {
                "status": "invalid",
                "reason": "original and product reopen requests/entrypoints/bindings differ",
                "different_fields": differences,
                "original_signature": original_signature,
                "product_signature": product_signature,
            }
    if selected_comparison is None:
        return {
            "status": "effect_unknown",
            "reason": "reopen outputs require an explicit paired comparison declaration",
        }
    return None


def _run_pair_action(
    *,
    action: Mapping[str, Any],
    case: Mapping[str, Any],
    contract: Mapping[str, Any],
    original_binary: Path,
    product_binary: Path,
    original_dir: Path,
    product_dir: Path,
    artifact_dir: Path,
    bindings: dict[str, dict[str, Any]],
    timeouts: ProcessTimeouts,
    daemons: Mapping[str, _OwnedDaemon] | None = None,
    source_roots: Mapping[str, Path] | None = None,
) -> dict[str, Any]:
    action_id = action.get("id", action.get("action_id", "action"))
    side_results: dict[str, Any] = {}
    for side, binary, side_dir in (
        ("original", original_binary, original_dir),
        ("product", product_binary, product_dir),
    ):
        side_results[side] = _execute_side_action(
            side=side,
            binary=binary,
            contract=contract,
            case=case,
            action=action,
            side_dir=side_dir,
            artifact_dir=artifact_dir / side,
            timeouts=timeouts,
            bindings=bindings[side],
            owned_daemon=(daemons or {}).get(side),
            source_root=(source_roots or {}).get(side),
        )
        bindings[side].update(side_results[side].get("bindings", {}))
    route = _route_map(contract).get(action.get("route", action.get("operation")))
    comparison = _pair_comparison(
        side_results["original"],
        side_results["product"],
        comparison=action.get("comparison"),
        route=route,
        case_classification=case.get("classification", "original_native"),
    )
    return {
        "action_id": action_id,
        "route": action.get("route", action.get("operation")),
        "input": copy.deepcopy(dict(action)),
        "original": side_results["original"],
        "product": side_results["product"],
        "comparison": comparison,
        "status": comparison["status"],
    }


def _proof_result(result: Mapping[str, Any], proof: Mapping[str, Any], side: str) -> dict[str, Any]:
    _require(isinstance(result, Mapping), "composition proof result must be an object", RunnerError)
    _require(isinstance(proof, Mapping), "composition proof declaration must be an object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown composition proof side: {side!r}", RunnerError)
    response = result.get("response")
    if result.get("status") != "completed" or response is None:
        return {
            "status": "unknown",
            "composition_reached": False,
            "reason": f"{side} composition proof did not return a completed response",
            "side_status": result.get("status"),
        }
    required = proof.get("required_json_pointers", ())
    _require(isinstance(required, (list, tuple)), "composition proof required_json_pointers must be a list", RunnerError)
    _require(all(isinstance(pointer, str) for pointer in required), "composition proof pointers must be strings", RunnerError)
    equals = proof.get("equals", {})
    _require(isinstance(equals, Mapping), "composition proof equals must be an object", RunnerError)
    missing = [pointer for pointer in required if json_pointer(response, pointer) is _MISSING]
    if missing:
        return {
            "status": "unknown",
            "composition_reached": False,
            "reason": f"{side} composition proof omitted required observables",
            "missing": missing,
        }
    mismatches = []
    for pointer, expected in equals.items():
        _require(isinstance(pointer, str), "composition proof equals keys must be strings", RunnerError)
        actual = json_pointer(response, pointer)
        if actual is _MISSING or not _json_semantic_equal(actual, expected):
            mismatches.append({"pointer": pointer, "expected": expected, "actual": None if actual is _MISSING else actual})
    if mismatches:
        return {
            "status": "fail",
            "composition_reached": False,
            "reason": f"{side} composition proof did not match its declared evidence",
            "mismatches": mismatches,
        }
    # A matching response can still be fabricated by a child that never
    # contacted the runner-owned daemon.  Composition is reached only after
    # the same challenge-bound authority protocol used for operation
    # reachability has been observed on this side.
    if not _authority_evidence_observed(result):
        return {
            "status": "unknown",
            "composition_reached": False,
            "reason": f"{side} composition proof lacks runner-owned authority protocol evidence",
        }
    return {
        "status": "pass",
        "composition_reached": True,
        "reason": f"{side} composition proof exposed all declared observables",
        "required_json_pointers": list(required),
        "equals": copy.deepcopy(dict(proof.get("equals", {}))),
    }


def _lifecycle_close_observation(
    *,
    close_spec: Mapping[str, Any],
    last_result: Mapping[str, Any] | None,
    side: str,
    case: Mapping[str, Any],
    contract: Mapping[str, Any],
    binary: Path,
    side_dir: Path,
    artifact_dir: Path,
    bindings: dict[str, Any],
    timeouts: ProcessTimeouts,
    owned_daemon: _OwnedDaemon | None,
    source_root: Path | None = None,
) -> dict[str, Any]:
    mode = close_spec.get("mode")
    if mode == "process_exit":
        if owned_daemon is None:
            return {
                "mode": mode,
                "status": "unknown",
                "reason": "process_exit close has no runner-owned foreground daemon",
            }
        cleanup = owned_daemon.stop(timeouts, reason="lifecycle process_exit close")
        status = "pass" if cleanup.get("status") == "completed" else "unknown"
        return {
            "mode": mode,
            "status": status,
            "reason": "runner-owned foreground daemon exited and its group was empty"
            if status == "pass"
            else "runner-owned foreground daemon did not settle",
            "observed_cleanup": copy.deepcopy(cleanup),
            "daemon_identity": copy.deepcopy(owned_daemon.identity),
        }
    close_action = close_spec.get("action")
    if not isinstance(close_action, Mapping):
        return {"mode": mode, "status": "invalid", "reason": "command close hook omitted action"}
    close_result = _execute_side_action(
        side=side,
        binary=binary,
        contract=contract,
        case=case,
        action=close_action,
        side_dir=side_dir,
        artifact_dir=artifact_dir / "action",
        timeouts=timeouts,
        bindings=bindings,
        owned_daemon=owned_daemon,
        source_root=source_root,
    )
    bindings.update(close_result.get("bindings", {}))
    exited = owned_daemon is not None and owned_daemon.wait_for_exit(timeouts.terminate_seconds)
    cleanup = owned_daemon.stop(timeouts, reason="lifecycle command close") if owned_daemon is not None else None
    status = "pass" if close_result.get("status") == "completed" and exited and cleanup and cleanup.get("status") == "completed" else "unknown"
    return {
        "mode": mode,
        "status": status,
        "reason": (
            "close command completed and the runner-owned daemon exited"
            if status == "pass"
            else "close command output did not prove exit of the runner-owned daemon"
        ),
        "action": close_result,
        "daemon_identity": copy.deepcopy(owned_daemon.identity) if owned_daemon is not None else None,
        "daemon_exited": exited,
        "cleanup": copy.deepcopy(cleanup),
    }


def _runner_durable_checkpoint_section(
    rows: Any,
    *,
    side: str,
    phase: str,
    pointers: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    """Build one durable snapshot from a runner-executed checkpoint.

    A response pointer is eligible for mutation evidence only when its
    checkpoint completed under the runner-owned daemon and carries a receipt
    that was MAC-verified before this section was created.  The section keeps
    the exact pointer values and receipt/response digests so a later ledger
    consumer can recompute the evidence rather than trusting a marker.
    """

    if isinstance(rows, Mapping):
        rows = (rows,)
    if not isinstance(rows, (list, tuple)):
        rows = ()
    required_fields = (
        "effect_json_pointers",
        "receipt_json_pointers",
        "state_json_pointers",
    )
    requested = {
        field: list(pointers.get(field, ()))
        for field in required_fields
        if isinstance(pointers.get(field, ()), (list, tuple))
        and pointers.get(field, ())
    }
    if not requested:
        return {
            "observed": False,
            "phase": phase,
            "source": f"runner_observed_{phase}_checkpoint",
            "authority_correlated": False,
            "values": {},
            "sha256": json_digest({}),
            "reason": "no durable pointers were declared for this checkpoint phase",
        }
    reasons: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            reasons.append("checkpoint row is malformed")
            continue
        side_result = row.get(side)
        if not isinstance(side_result, Mapping):
            reasons.append(f"{side} checkpoint result is missing")
            continue
        if side_result.get("status") not in ("completed", "pass"):
            reasons.append(f"{side} checkpoint status is not completed")
            continue
        response = side_result.get("response")
        if response is None:
            reasons.append(f"{side} checkpoint response is missing")
            continue
        if not _authenticated_result_observation(side_result):
            reasons.append(f"{side} checkpoint receipt was not runner-authenticated")
            continue
        receipt_wrapper = side_result.get("action_receipt")
        receipt = receipt_wrapper.get("receipt") if isinstance(receipt_wrapper, Mapping) else None
        receipt_sha = receipt_wrapper.get("receipt_sha256") if isinstance(receipt_wrapper, Mapping) else None
        reachability = side_result.get("operation_reachability")
        if (
            not isinstance(receipt_wrapper, Mapping)
            or not isinstance(receipt, Mapping)
            or not isinstance(receipt_sha, str)
            or re.fullmatch(r"[0-9a-f]{64}", receipt_sha) is None
            or not isinstance(reachability, Mapping)
            or reachability.get("authority_correlated") is not True
            or not _verified_receipt_proof(receipt_wrapper, response=response)
        ):
            reasons.append(f"{side} checkpoint receipt/authority evidence is incomplete")
            continue
        values: dict[str, dict[str, Any]] = {}
        missing: list[str] = []
        for field, field_pointers in requested.items():
            field_values: dict[str, Any] = {}
            for pointer in field_pointers:
                value = json_pointer(response, pointer)
                if value is _MISSING:
                    missing.append(pointer)
                else:
                    field_values[pointer] = copy.deepcopy(value)
            if field_values:
                # Durable sections use the semantic names expected by the
                # ledger reader (effect/receipt/state), while the case
                # declaration uses the ``*_json_pointers`` suffix.
                values[field.removesuffix("_json_pointers")] = field_values
        if missing:
            reasons.append(f"{side} checkpoint omitted durable pointers: {', '.join(missing)}")
            continue
        response_digest = _daemon_result_digest(response)
        authority_digest = reachability.get("authority_digest")
        route_identity = side_result.get("route_identity")
        if (
            not isinstance(response_digest, str)
            or not isinstance(authority_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", authority_digest) is None
            or not isinstance(route_identity, Mapping)
        ):
            reasons.append(f"{side} checkpoint lacks canonical response/authority identity")
            continue
        section = {
            "observed": True,
            "phase": phase,
            "source": f"runner_observed_{phase}_checkpoint",
            "side": side,
            "action_id": row.get("id") or side_result.get("input", {}).get("id"),
            "route": side_result.get("route"),
            "entrypoint": side_result.get("entrypoint"),
            "authority_correlated": True,
            "authority_digest": authority_digest,
            "action_receipt_sha256": receipt_sha,
            "action_receipt": copy.deepcopy(dict(receipt_wrapper)),
            "response_digest": response_digest,
            "values": values,
        }
        section["sha256"] = json_digest(values)
        return section
    return {
        "observed": False,
        "phase": phase,
        "source": f"runner_observed_{phase}_checkpoint",
        "side": side,
        "authority_correlated": False,
        "values": {},
        "sha256": json_digest({}),
        "reason": "; ".join(reasons) or f"no {phase} checkpoint was executed",
    }


def _attach_runner_observed_durable(
    action_result: Mapping[str, Any],
    action: Mapping[str, Any],
    *,
    lifecycle: Mapping[str, Any] | None = None,
) -> None:
    """Attach runner-owned after/reopen snapshots to one action result."""

    if not isinstance(action_result, dict) or not isinstance(action, Mapping):
        return
    if lifecycle is not None and not isinstance(lifecycle, Mapping):
        lifecycle = None
    route_id = action_result.get("route", action.get("route", action.get("operation")))
    route = action_result.get("_route")
    if not isinstance(route, Mapping):
        # The route object is not normally retained on the pair row.  The
        # operation kind/effect policy is copied into each side result by the
        # executor, which is enough to determine whether a durable snapshot
        # is required here.
        route = {"effect_policy": action_result.get("effect_policy")}
    input_action = action_result.get("input", action)
    comparison = input_action.get("comparison", {}) if isinstance(input_action, Mapping) else {}
    if not isinstance(comparison, Mapping):
        comparison = {}
    policy = comparison.get("effect_mode")
    if policy is None:
        policy = route.get("effect_policy")
    pointers = _effect_pointers(comparison)
    durable_required = policy == "required" or (
        policy == "retrieval" and bool(pointers.get("effect_json_pointers"))
    )
    if not durable_required:
        return
    checkpoints = action_result.get("checkpoints", {})
    if not isinstance(checkpoints, Mapping):
        checkpoints = {}
    for side in SIDE_NAMES:
        side_result = action_result.get(side)
        if not isinstance(side_result, dict):
            continue
        after = _runner_durable_checkpoint_section(
            checkpoints.get("after", ()),
            side=side,
            phase="after",
            pointers=pointers,
        )
        reopen = _runner_durable_checkpoint_section(
            checkpoints.get("reopened", ())
            if checkpoints.get("reopened")
            else (
                [
                    {
                        "id": f"lifecycle-reopen-{side}",
                        side: (
                            lifecycle.get("reopen", {}).get(side, {}).get("result")
                            if isinstance(lifecycle.get("reopen"), Mapping)
                            and isinstance(lifecycle.get("reopen", {}).get(side), Mapping)
                            and lifecycle.get("reopen", {}).get(side, {}).get("status") == "pass"
                            else None
                        ),
                    }
                ]
                if isinstance(lifecycle, Mapping)
                else ()
            ),
            side=side,
            phase="reopen",
            pointers=pointers,
        )
        main_receipt = side_result.get("action_receipt")
        main_receipt_sha = (
            main_receipt.get("receipt_sha256")
            if isinstance(main_receipt, Mapping)
            else None
        )
        main_receipt_valid = (
            isinstance(main_receipt, Mapping)
            and isinstance(main_receipt_sha, str)
            and re.fullmatch(r"[0-9a-f]{64}", main_receipt_sha) is not None
            and _authenticated_result_observation(side_result)
        )
        observed = bool(
            main_receipt_valid
            and after.get("observed") is True
            and reopen.get("observed") is True
        )
        durable: dict[str, Any] = {
            "observed": observed,
            "source": "runner_owned_daemon_action_receipt",
            "authority_correlated": bool(
                observed
                and after.get("authority_correlated") is True
                and reopen.get("authority_correlated") is True
            ),
            "action_receipt_sha256": main_receipt_sha if main_receipt_valid else None,
            "after": after,
            "reopen": reopen,
        }
        if not observed:
            durable["reason"] = (
                "runner-owned after and reopened observations were not both "
                "authenticated; mutation evidence remains unknown"
            )
        side_result["runner_observed_durable"] = durable
        roots = side_result.get("roots")
        artifact_root = roots.get("artifact_root") if isinstance(roots, Mapping) else None
        if isinstance(artifact_root, str) and artifact_root:
            try:
                _rewrite_json_atomically(
                    Path(artifact_root) / "result.json",
                    side_result,
                    label="side-result-with-durable-observation",
                )
            except (OSError, RunnerError, TypeError, ValueError):
                # The in-memory result remains conservative: the durable
                # section is present, but its artifact is not trusted by
                # ledger reload until the atomic rewrite succeeds.
                durable["artifact_rewrite"] = {"status": "unknown"}


def _run_case_inner(
    case: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    original_binary: Path,
    product_binary: Path,
    case_artifact_dir: Path,
    timeouts: ProcessTimeouts = ProcessTimeouts(),
    candidate_product_revision: str | None = None,
    daemons: Mapping[str, _OwnedDaemon] | None = None,
    source_roots: Mapping[str, Path] | None = None,
) -> dict[str, Any]:
    validate_case(case, contract)
    case_id = case.get("id", case.get("case_id"))
    case_artifact_dir.mkdir(parents=True, exist_ok=True)
    original_dir = case_artifact_dir / "original"
    product_dir = case_artifact_dir / "product"
    original_dir.mkdir(exist_ok=True)
    product_dir.mkdir(exist_ok=True)
    _write_json(case_artifact_dir / "consumed-case.json", case)
    action_rows: list[dict[str, Any]] = []
    reopened_checkpoint_actions: list[tuple[dict[str, Any], dict[str, Any]]] = []
    comparison_statuses: list[str] = []
    bindings: dict[str, dict[str, Any]] = {"original": {}, "product": {}}
    setup_rows: dict[str, list[dict[str, Any]]] = {"original": [], "product": []}

    # Setup is side-specific by design.  It is where fixtures may seed or
    # configure each independent store through public routes; no state is
    # copied between the two roots.
    for side, binary, side_dir in (
        ("original", original_binary, original_dir),
        ("product", product_binary, product_dir),
    ):
        for setup_index, source_setup in enumerate(case.get("side_setup", {}).get(side, [])):
            setup = copy.deepcopy(source_setup)
            setup.setdefault("id", f"setup-{setup_index}")
            result = _execute_side_action(
                side=side,
                binary=binary,
                contract=contract,
                case=case,
                action=setup,
                side_dir=side_dir,
                artifact_dir=case_artifact_dir / "setup" / side / _slug(setup["id"]),
                timeouts=timeouts,
                bindings=bindings[side],
                owned_daemon=(daemons or {}).get(side),
                source_root=(source_roots or {}).get(side),
            )
            bindings[side].update(result.get("bindings", {}))
            setup_rows[side].append({"id": setup["id"], "result": result})
            if result.get("status") not in ("completed", "pass"):
                # Setup is causal evidence: an unavailable/cancelled/partial
                # seed cannot be hidden by a later equal response.
                comparison_statuses.append(
                    "unknown" if result["status"] == "error" else result["status"]
                )

    proof_rows: dict[str, Any] = {}
    proof_specs = case.get("composition_proof", {})
    if isinstance(proof_specs, Mapping):
        for side, binary, side_dir in (
            ("original", original_binary, original_dir),
            ("product", product_binary, product_dir),
        ):
            proof = proof_specs.get(side)
            if not isinstance(proof, Mapping):
                if case.get("classification") == "host_extension" and side == "original":
                    proof_rows[side] = {
                        "status": "unknown",
                        "composition_reached": False,
                        "reason": "no original counterpart is declared for host extension",
                    }
                    continue
                proof_rows[side] = {
                    "status": "invalid",
                    "composition_reached": False,
                    "reason": "side-specific composition proof is missing",
                }
                comparison_statuses.append("invalid")
                continue
            proof_checks = proof.get("checks")
            if proof_checks is None:
                proof_checks = [proof]
            check_rows = []
            for check_index, source_check in enumerate(proof_checks):
                check = copy.deepcopy(dict(source_check))
                check.setdefault("id", f"composition-proof-{side}-{check_index}")
                check_result = _execute_side_action(
                    side=side,
                    binary=binary,
                    contract=contract,
                    case=case,
                    action=check,
                    side_dir=side_dir,
                    artifact_dir=case_artifact_dir / "composition-proof" / side / _slug(check["id"]),
                    timeouts=timeouts,
                    bindings=bindings[side],
                    owned_daemon=(daemons or {}).get(side),
                    source_root=(source_roots or {}).get(side),
                )
                bindings[side].update(check_result.get("bindings", {}))
                assessment = _proof_result(check_result, source_check, side)
                check_rows.append({"id": check["id"], "result": check_result, "assessment": assessment})
            proof_status = _status_priority(row["assessment"]["status"] for row in check_rows)
            proof_rows[side] = {
                "status": proof_status,
                "composition_reached": bool(check_rows)
                and all(row["assessment"].get("composition_reached") is True for row in check_rows),
                "checks": check_rows,
            }
            comparison_statuses.append(proof_status)

    for action_index, source_action in enumerate(case["actions"]):
        action = copy.deepcopy(source_action)
        action.setdefault("id", action.get("action_id", f"action-{action_index}"))
        action_id = action["id"]
        checkpoint_rows: dict[str, list[dict[str, Any]]] = {}
        for phase in ("before",):
            checkpoint_rows[phase] = []
            for checkpoint_index, spec in enumerate(_checkpoint_specs(action, phase)):
                checkpoint = dict(spec)
                checkpoint.setdefault("id", f"{action_id}/checkpoint/{phase}/{checkpoint_index}")
                checkpoint_dir = case_artifact_dir / "checkpoints" / _slug(checkpoint["id"])
                checkpoint["phase"] = phase
                checkpoint_result = _run_pair_action(
                    action=checkpoint,
                    case=case,
                    contract=contract,
                    original_binary=original_binary,
                    product_binary=product_binary,
                    original_dir=original_dir,
                    product_dir=product_dir,
                    artifact_dir=checkpoint_dir,
                    bindings=bindings,
                    timeouts=timeouts,
                    daemons=daemons,
                    source_roots=source_roots,
                )
                checkpoint_rows[phase].append({"id": checkpoint["id"], **checkpoint_result})
                comparison_statuses.append(checkpoint_result["status"])

        action_result = _run_pair_action(
            action=action,
            case=case,
            contract=contract,
            original_binary=original_binary,
            product_binary=product_binary,
            original_dir=original_dir,
            product_dir=product_dir,
            artifact_dir=case_artifact_dir / "actions" / _slug(action_id),
            bindings=bindings,
            timeouts=timeouts,
            daemons=daemons,
            source_roots=source_roots,
        )
        action_result["checkpoints"] = checkpoint_rows
        checkpoint_rows["after"] = []
        for checkpoint_index, spec in enumerate(_checkpoint_specs(action, "after")):
            checkpoint = dict(spec)
            checkpoint.setdefault("id", f"{action_id}/checkpoint/after/{checkpoint_index}")
            checkpoint_dir = case_artifact_dir / "checkpoints" / _slug(checkpoint["id"])
            checkpoint["phase"] = "after"
            checkpoint_result = _run_pair_action(
                action=checkpoint,
                case=case,
                contract=contract,
                original_binary=original_binary,
                product_binary=product_binary,
                original_dir=original_dir,
                product_dir=product_dir,
                artifact_dir=checkpoint_dir,
                bindings=bindings,
                timeouts=timeouts,
                daemons=daemons,
                source_roots=source_roots,
            )
            checkpoint_rows["after"].append({"id": checkpoint["id"], **checkpoint_result})
            comparison_statuses.append(checkpoint_result["status"])
        checkpoint_rows["reopened"] = []
        # Reopened probes are deferred until lifecycle close and fresh daemon
        # identity have both been verified below.  A phase label alone cannot
        # establish that the probe crossed a restart boundary.
        reopened_checkpoint_actions.append((action_result, action))
        action_result["checkpoints"] = checkpoint_rows
        action_rows.append(action_result)
        comparison_statuses.append(action_result["status"])

    lifecycle_rows: dict[str, Any] = {}
    lifecycle = case.get("lifecycle")
    if isinstance(lifecycle, Mapping):
        close_rows: dict[str, Any] = {}
        last_by_side = {
            side: (action_rows[-1].get(side) if action_rows else None)
            for side in SIDE_NAMES
        }
        for side, binary, side_dir in (
            ("original", original_binary, original_dir),
            ("product", product_binary, product_dir),
        ):
            close_rows[side] = _lifecycle_close_observation(
                close_spec=lifecycle["close"][side],
                last_result=last_by_side[side],
                side=side,
                case=case,
                contract=contract,
                binary=binary,
                side_dir=side_dir,
                artifact_dir=case_artifact_dir / "lifecycle" / "close" / side,
                bindings=bindings[side],
                timeouts=timeouts,
                owned_daemon=(daemons or {}).get(side),
                source_root=(source_roots or {}).get(side),
            )
            comparison_statuses.append(close_rows[side]["status"])
        reopen_rows: dict[str, Any] = {}
        for side, binary, side_dir in (
            ("original", original_binary, original_dir),
            ("product", product_binary, product_dir),
        ):
            close_row = close_rows[side]
            reopen_spec = lifecycle["reopen"][side]
            reopen_action = reopen_spec.get("action")
            declared_side = reopen_action.get("side") if isinstance(reopen_action, Mapping) else None
            reopen_route = (
                reopen_action.get("route", reopen_action.get("operation"))
                if isinstance(reopen_action, Mapping)
                else None
            )
            if declared_side not in (None, side):
                reopen_rows[side] = {
                    "side": side,
                    "status": "invalid",
                    "reason": f"reopen action is assigned to {declared_side!r}",
                    "route": reopen_route,
                    "requested_action": copy.deepcopy(reopen_action),
                }
                comparison_statuses.append("invalid")
                continue
            if close_row.get("status") != "pass":
                reopen_rows[side] = {
                    "side": side,
                    "status": "unknown",
                    "reason": "fresh daemon was not started because owned close was not verified",
                    "close": close_row,
                    "route": reopen_route,
                    "requested_action": copy.deepcopy(reopen_action),
                }
                comparison_statuses.append("unknown")
                continue
            old_daemon = (daemons or {}).get(side)
            if old_daemon is None or not isinstance(daemons, dict):
                reopen_rows[side] = {
                    "side": side,
                    "status": "unknown",
                    "reason": "runner-owned daemon state is unavailable for reopen",
                    "route": reopen_route,
                    "requested_action": copy.deepcopy(reopen_action),
                }
                comparison_statuses.append("unknown")
                continue
            try:
                new_daemon = _OwnedDaemon.start(
                    side=side,
                    binary=binary,
                    side_dir=side_dir,
                    profile_root=old_daemon.profile_root,
                    artifact_dir=case_artifact_dir / "lifecycle" / "reopen-daemon" / side,
                    timeouts=timeouts,
                )
                daemons[side] = new_daemon
            except (OSError, RunnerError, ValueError) as error:
                reopen_rows[side] = {
                    "side": side,
                    "status": "unknown",
                    "reason": f"fresh daemon could not be started: {error}",
                    "route": reopen_route,
                    "requested_action": copy.deepcopy(reopen_action),
                    "previous_daemon": copy.deepcopy(old_daemon.identity),
                }
                comparison_statuses.append("unknown")
                continue
            if reopen_spec.get("mode", "action") != "action" or not isinstance(reopen_action, Mapping):
                reopen_rows[side] = {
                    "side": side,
                    "status": "invalid",
                    "reason": "fresh-process reopen action is required",
                    "route": reopen_route,
                    "requested_action": copy.deepcopy(reopen_action),
                    "previous_daemon": copy.deepcopy(old_daemon.identity),
                    "reopened_daemon": copy.deepcopy(new_daemon.identity),
                }
                comparison_statuses.append("invalid")
                continue
            reopen_action = copy.deepcopy(dict(reopen_action))
            reopen_action.setdefault("id", f"lifecycle-reopen-{side}")
            reopen_result = _execute_side_action(
                side=side,
                binary=binary,
                contract=contract,
                case=case,
                action=reopen_action,
                side_dir=side_dir,
                artifact_dir=case_artifact_dir / "lifecycle" / "reopen" / side,
                timeouts=timeouts,
                bindings=bindings[side],
                owned_daemon=new_daemon,
                source_root=(source_roots or {}).get(side),
            )
            bindings[side].update(reopen_result.get("bindings", {}))
            old_identity = old_daemon.identity or {}
            new_identity = new_daemon.current_identity()
            identity_ok = (
                new_identity is not None
                and old_identity.get("pid") is not None
                and new_identity.get("pid") != old_identity.get("pid")
                and old_identity.get("process_run_id") is not None
                and new_identity.get("process_run_id") != old_identity.get("process_run_id")
                and old_identity.get("epoch") is not None
                and isinstance(new_identity.get("epoch"), int)
                and new_identity.get("epoch") > old_identity.get("epoch", 0)
                and new_identity.get("profile_root") == old_identity.get("profile_root")
            )
            reopen_rows[side] = {
                "side": side,
                "status": "pass"
                if reopen_result.get("status") == "completed" and identity_ok
                else "unknown",
                "reason": (
                    "fresh action completed under a new runner-owned daemon identity"
                    if reopen_result.get("status") == "completed" and identity_ok
                    else "fresh action did not prove a new runner-owned daemon identity"
                ),
                "result": reopen_result,
                "route": reopen_route,
                "requested_action": copy.deepcopy(reopen_action),
                "fresh_process": True,
                "previous_daemon": copy.deepcopy(old_daemon.identity),
                "reopened_daemon": copy.deepcopy(new_identity),
            }
            comparison_statuses.append(reopen_rows[side]["status"])
        reopen_comparison: dict[str, Any]
        original_reopen = reopen_rows.get("original", {}).get("result")
        product_reopen = reopen_rows.get("product", {}).get("result")
        original_reopen_action = lifecycle["reopen"].get("original", {}).get("action")
        product_reopen_action = lifecycle["reopen"].get("product", {}).get("action")
        original_reopen_route = (
            original_reopen_action.get("route", original_reopen_action.get("operation"))
            if isinstance(original_reopen_action, Mapping)
            else None
        )
        reopen_pair_error = _validate_reopen_pair(original_reopen_action, product_reopen_action, contract=contract)
        reopen_comparison_spec = (
            original_reopen_action.get("comparison")
            if isinstance(original_reopen_action, Mapping)
            else None
        )
        if reopen_comparison_spec is None and isinstance(product_reopen_action, Mapping):
            reopen_comparison_spec = product_reopen_action.get("comparison")
        reopen_route_obj = _route_map(contract).get(original_reopen_route)
        if reopen_pair_error is not None and reopen_pair_error.get("status") == "invalid":
            reopen_comparison = reopen_pair_error
        elif not isinstance(original_reopen, Mapping) or not isinstance(product_reopen, Mapping):
            reopen_comparison = {
                "status": "unknown",
                "reason": "reopen outputs were not produced on both sides",
            }
        elif reopen_pair_error is not None:
            reopen_comparison = reopen_pair_error
        else:
            reopen_comparison = _pair_comparison(
                original_reopen,
                product_reopen,
                comparison=reopen_comparison_spec,
                route=reopen_route_obj,
                case_classification=case.get("classification", "original_native"),
            )
        lifecycle_rows = {
            "require_owned_close": bool(lifecycle.get("require_owned_close", True)),
            "close": close_rows,
            "reopen": reopen_rows,
            "reopen_comparison": reopen_comparison,
        }
        if lifecycle_rows["require_owned_close"] and any(row["status"] != "pass" for row in close_rows.values()):
            lifecycle_rows["status"] = "unknown"
            lifecycle_rows["reason"] = "a side did not prove owned close before reopening"
        elif any(row["status"] != "pass" for row in reopen_rows.values()):
            lifecycle_rows["status"] = "unknown"
            lifecycle_rows["reason"] = "a side did not complete its fresh-process reopen action"
        elif reopen_comparison["status"] != "pass":
            lifecycle_rows["status"] = _status_priority(("unknown", reopen_comparison["status"]))
            lifecycle_rows["reason"] = "paired reopen outputs did not provide a comparative result"
        else:
            lifecycle_rows["status"] = "pass"
        comparison_statuses.append(lifecycle_rows["status"])

    # A reopened checkpoint is meaningful only after both sides crossed the
    # verified close/restart boundary.  Keep an explicit unknown row when a
    # case omitted lifecycle hooks or when either side could not establish the
    # new daemon identity; do not invoke the probe against the old process.
    reopened_ready = isinstance(lifecycle, Mapping) and lifecycle_rows.get("status") == "pass"
    for action_result, action in reopened_checkpoint_actions:
        for checkpoint_index, spec in enumerate(_checkpoint_specs(action, "reopened")):
            checkpoint = dict(spec)
            checkpoint.setdefault(
                "id", f"{action.get('id', action.get('action_id', 'action'))}/checkpoint/reopened/{checkpoint_index}"
            )
            checkpoint["phase"] = "reopened"
            if reopened_ready:
                checkpoint_result = _run_pair_action(
                    action=checkpoint,
                    case=case,
                    contract=contract,
                    original_binary=original_binary,
                    product_binary=product_binary,
                    original_dir=original_dir,
                    product_dir=product_dir,
                    artifact_dir=case_artifact_dir / "checkpoints" / _slug(checkpoint["id"]),
                    bindings=bindings,
                    timeouts=timeouts,
                    daemons=daemons,
                    source_roots=source_roots,
                )
            else:
                reason = (
                    "reopened checkpoint was withheld because lifecycle close/reopen evidence did not pass"
                    if isinstance(lifecycle, Mapping)
                    else "reopened checkpoint requires an explicit lifecycle close/reopen hook"
                )
                unknown_side = {
                    "status": "unknown",
                    "reason": reason,
                    "route": checkpoint.get("route", checkpoint.get("operation")),
                    "response": None,
                }
                checkpoint_result = {
                    "action_id": checkpoint["id"],
                    "route": checkpoint.get("route", checkpoint.get("operation")),
                    "input": copy.deepcopy(checkpoint),
                    "original": copy.deepcopy(unknown_side),
                    "product": copy.deepcopy(unknown_side),
                    "comparison": {"status": "unknown", "reason": reason},
                    "status": "unknown",
                }
            action_result["checkpoints"]["reopened"].append(
                {"id": checkpoint["id"], **checkpoint_result}
            )
            comparison_statuses.append(checkpoint_result["status"])

    # Materialize mutation/retrieval durability only after both the after and
    # fresh-process reopened checkpoint phases have run.  This writes the
    # runner-observed section into each side artifact before the enclosing
    # case-result digest and attempt ledger are finalized.
    for action_result, action in zip(action_rows, case["actions"]):
        _attach_runner_observed_durable(
            action_result,
            action,
            lifecycle=lifecycle_rows if isinstance(lifecycle_rows, Mapping) else None,
        )

    composition_reached = {
        side: proof_rows.get(side, {}).get("composition_reached") is True
        for side in SIDE_NAMES
    }
    result = {
        "case_id": case_id,
        "classification": case.get("classification", "original_native"),
        "status": _status_priority(
            [*comparison_statuses]
            + (["blocked"] if not all(composition_reached.values()) else [])
        ),
        "reference_revision": REFERENCE_REVISION,
        "expected_product_revision": PRODUCT_REVISION,
        "product_source_revision": candidate_product_revision or "unknown",
        "historical_audit": {
            "reference_revision": HISTORICAL_REFERENCE_REVISION,
            "product_revision": HISTORICAL_PRODUCT_REVISION,
            "status": "historical-only; never used as an execution oracle",
        },
        "composition": COMPOSITIONS,
        "composition_reached": composition_reached,
        "composition_blocked": not all(composition_reached.values()),
        "composition_evidence": proof_rows,
        "setup": setup_rows,
        "lifecycle_spec": copy.deepcopy(dict(lifecycle)) if isinstance(lifecycle, Mapping) else None,
        "lifecycle": lifecycle_rows,
        "daemon_runtime": {
            side: copy.deepcopy((daemons or {}).get(side).identity)
            if (daemons or {}).get(side) is not None
            else None
            for side in SIDE_NAMES
        },
        "bindings": bindings,
        "actions": action_rows,
        "artifact_directory": str(case_artifact_dir),
    }
    return result


def _iter_side_observations(
    result: Mapping[str, Any],
    side: str,
) -> Iterable[tuple[str, Mapping[str, Any], Mapping[str, Any]]]:
    """Yield top-level, checkpoint and fresh-reopen observations for a side."""

    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    setup = result.get("setup", {})
    if isinstance(setup, Mapping):
        rows = setup.get(side, ())
        if isinstance(rows, Mapping):
            rows = (rows,)
        if isinstance(rows, (list, tuple)):
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                side_result = row.get("result")
                if isinstance(side_result, Mapping):
                    yield "setup", {"action_id": row.get("id"), "route": side_result.get("route"), "input": side_result.get("input", {})}, side_result

    proof = result.get("composition_evidence", {})
    if isinstance(proof, Mapping):
        side_proof = proof.get(side)
        checks = side_proof.get("checks", ()) if isinstance(side_proof, Mapping) else ()
        if isinstance(checks, Mapping):
            checks = (checks,)
        if isinstance(checks, (list, tuple)):
            for row in checks:
                if not isinstance(row, Mapping):
                    continue
                side_result = row.get("result")
                if isinstance(side_result, Mapping):
                    yield "composition", {"action_id": row.get("id"), "route": side_result.get("route"), "input": side_result.get("input", {})}, side_result

    actions = result.get("actions", ())
    _require(isinstance(actions, (list, tuple)), "case result actions must be a list", RunnerError)
    for action in actions:
        if not isinstance(action, Mapping):
            continue
        side_result = action.get(side)
        if isinstance(side_result, Mapping):
            yield "action", action, side_result
        checkpoints = action.get("checkpoints", {})
        if not isinstance(checkpoints, Mapping):
            continue
        for phase in ("before", "after", "reopened"):
            rows = checkpoints.get(phase, ())
            if isinstance(rows, Mapping):
                rows = (rows,)
            if not isinstance(rows, (list, tuple)):
                continue
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                side_result = row.get(side)
                if isinstance(side_result, Mapping):
                    yield f"checkpoint:{phase}", row, side_result
    lifecycle = result.get("lifecycle")
    if not isinstance(lifecycle, Mapping):
        return
    reopen_rows = lifecycle.get("reopen", {})
    reopen_specs = result.get("lifecycle_spec", {})
    if not isinstance(reopen_rows, Mapping):
        return
    row = reopen_rows.get(side)
    if not isinstance(row, Mapping):
        return
    side_result = row.get("result")
    if not isinstance(side_result, Mapping):
        return
    spec = reopen_specs.get(side, {}) if isinstance(reopen_specs, Mapping) else {}
    action = spec.get("action", {}) if isinstance(spec, Mapping) else {}
    if not isinstance(action, Mapping):
        action = {}
    synthetic = {
        "action_id": action.get("id", f"lifecycle-reopen-{side}"),
        "route": action.get("route", action.get("operation")),
        "input": dict(action),
    }
    yield "reopen", synthetic, side_result


def _side_no_effect_evidence(result: Mapping[str, Any], side: str) -> dict[str, Any]:
    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    observations: list[dict[str, Any]] = []
    for scope, action, side_result in _iter_side_observations(result, side):
        input_action = action.get("input", {})
        comparison = input_action.get("comparison", {}) if isinstance(input_action, Mapping) else {}
        if not isinstance(comparison, Mapping):
            continue
        raw_pointers = comparison.get("no_effect_json_pointers", ())
        _require(isinstance(raw_pointers, (list, tuple)), "no_effect_json_pointers must be a list", RunnerError)
        pointers: list[str] = []
        for pointer in raw_pointers:
            _require(isinstance(pointer, str), "no-effect JSON pointers must be strings", RunnerError)
            _pointer_parts(pointer)
            pointers.append(pointer)
        if not pointers:
            continue
        response = side_result.get("response")
        values: dict[str, Any] = {}
        missing: list[str] = []
        for pointer in pointers:
            value = json_pointer(response, pointer)
            if value is _MISSING:
                missing.append(pointer)
            else:
                values[pointer] = copy.deepcopy(value)
        observations.append(
            {
                "scope": scope,
                "action_id": action.get("action_id"),
                "route": action.get("route"),
                "pointers": pointers,
                "values": values,
                "missing": missing,
                "status": side_result.get("status"),
                "response_observed": response is not None,
                "authority_correlated": _authority_evidence_observed(side_result),
            }
        )
    complete = bool(observations) and all(
        not item["missing"]
        and item.get("status") in ("completed", "pass")
        and item.get("response_observed") is True
        and item.get("authority_correlated") is True
        for item in observations
    )
    return {
        "observed": complete,
        "digest": json_digest(observations) if observations else None,
        "observations": observations,
    }


def _side_checkpoint_state(result: Mapping[str, Any], side: str) -> dict[str, Any]:
    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    observations: list[dict[str, Any]] = []
    for scope, action, side_result in _iter_side_observations(result, side):
        if not scope.startswith("checkpoint:"):
            continue
        response = side_result.get("response")
        observations.append(
            {
                "scope": scope,
                "action_id": action.get("action_id"),
                "route": action.get("route"),
                "status": side_result.get("status"),
                "response_digest": json_digest(response) if response is not None else None,
                "response_observed": response is not None,
                "authority_correlated": _authority_evidence_observed(side_result),
            }
        )
    return {
        "observed": bool(observations)
        and all(
            item.get("status") in ("completed", "pass")
            and item.get("response_observed") is True
            and item.get("authority_correlated") is True
            for item in observations
        ),
        "digest": json_digest(observations) if observations else None,
        "observations": observations,
    }


def _side_operation_reachability(result: Mapping[str, Any], side: str) -> dict[str, Any]:
    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    observations: list[dict[str, Any]] = []
    for scope, action, side_result in _iter_side_observations(result, side):
        reachability = side_result.get("operation_reachability")
        if not isinstance(reachability, Mapping):
            observations.append(
                {
                    "scope": scope,
                    "action_id": action.get("action_id"),
                    "route": action.get("route"),
                    "observed": False,
                    "authority_correlated": False,
                    "route_identity": copy.deepcopy(side_result.get("route_identity")),
                    "store_identity": copy.deepcopy(side_result.get("store_identity")),
                    "runner_identity_binding": copy.deepcopy(side_result.get("runner_identity_binding")),
                    "response_observed": side_result.get("response") is not None,
                    "status": side_result.get("status"),
                    "reason": "operation reachability evidence was absent",
                }
            )
            continue
        observations.append(
            {
                "scope": scope,
                "action_id": action.get("action_id"),
                "route": action.get("route"),
                "observed": reachability.get("observed") is True and _authority_evidence_observed(side_result),
                "authority_correlated": _authority_evidence_observed(side_result),
                "route_identity": copy.deepcopy(side_result.get("route_identity")),
                "store_identity": copy.deepcopy(side_result.get("store_identity")),
                "runner_identity_binding": copy.deepcopy(side_result.get("runner_identity_binding")),
                "authority_digest": reachability.get("authority_digest"),
                "authority_source": reachability.get("authority_source"),
                "challenge_required": reachability.get("challenge_required"),
                "challenge": reachability.get("challenge"),
                "challenge_response": reachability.get("challenge_response"),
                "authority_binding": copy.deepcopy(reachability.get("authority_binding")),
                "independent_authority_observation": copy.deepcopy(
                    reachability.get("independent_authority_observation")
                ),
                "independent_authority_observed": reachability.get(
                    "independent_authority_observed"
                ),
                "authority_protocol_evidence": copy.deepcopy(
                    reachability.get("authority_protocol_evidence")
                ),
                "action_receipt": copy.deepcopy(reachability.get("action_receipt")),
                "response_observed": reachability.get("response_observed"),
                "status": side_result.get("status"),
            }
        )
    observed = bool(observations) and all(item["observed"] and item["authority_correlated"] for item in observations)
    return {
        "observed": observed,
        # Keep the aggregate flag explicit and derived from the complete
        # observation set.  Ledger consumers use this alongside the
        # per-attempt top-level authority_correlated map; omitting it makes a
        # fully observed operation look self-asserted during revalidation.
        "authority_correlated": observed,
        "digest": json_digest(observations) if observations else None,
        "observations": observations,
    }


def _side_reopen_state(result: Mapping[str, Any], side: str) -> dict[str, Any]:
    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    lifecycle = result.get("lifecycle")
    rows = lifecycle.get("reopen", {}) if isinstance(lifecycle, Mapping) else {}
    row = rows.get(side) if isinstance(rows, Mapping) else None
    if not isinstance(row, Mapping):
        return {"observed": False, "status": "unknown", "reason": "reopen state was not recorded"}
    side_result = row.get("result")
    response = side_result.get("response") if isinstance(side_result, Mapping) else None
    return {
        "observed": (
            isinstance(side_result, Mapping)
            and row.get("status") == "pass"
            and side_result.get("status") == "completed"
            and isinstance(row.get("reopened_daemon"), Mapping)
            and response is not None
            and _authority_evidence_observed(side_result)
        ),
        "status": row.get("status"),
        "result_status": side_result.get("status") if isinstance(side_result, Mapping) else None,
        "response_digest": json_digest(response) if response is not None else None,
        "previous_daemon": copy.deepcopy(row.get("previous_daemon")),
        "reopened_daemon": copy.deepcopy(row.get("reopened_daemon")),
    }


def _side_operation_digest(
    result: Mapping[str, Any],
    side: str,
    fields: Sequence[str],
) -> str | None:
    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(side in SIDE_NAMES, f"unknown comparison side: {side!r}", RunnerError)
    _require(isinstance(fields, (list, tuple)), "operation evidence fields must be a list", RunnerError)
    _require(
        all(isinstance(field, str) and bool(field) for field in fields),
        "operation evidence field names must be strings",
        RunnerError,
    )
    observations: list[dict[str, Any]] = []
    for scope, action, side_result in _iter_side_observations(result, side):
        input_action = action.get("input", {})
        comparison = input_action.get("comparison", {}) if isinstance(input_action, Mapping) else {}
        if not isinstance(comparison, Mapping):
            continue
        pointers: list[str] = []
        for field in fields:
            raw_pointers = comparison.get(field, ())
            _require(isinstance(raw_pointers, (list, tuple)), f"{field} must be a list", RunnerError)
            for pointer in raw_pointers:
                _require(isinstance(pointer, str), f"{field} entries must be strings", RunnerError)
                _pointer_parts(pointer)
                pointers.append(pointer)
        if not pointers:
            continue
        response = side_result.get("response")
        values: dict[str, Any] = {}
        for pointer in pointers:
            value = _MISSING
            for field in fields:
                raw_pointers = comparison.get(field, ())
                if isinstance(raw_pointers, (list, tuple)) and pointer in raw_pointers:
                    if field in ("effect_json_pointers", "receipt_json_pointers", "state_json_pointers"):
                        phase = "reopen" if field == "state_json_pointers" else "after"
                        value = _runner_durable_value(
                            side_result,
                            field=field,
                            pointer=pointer,
                            phase=phase,
                        )
                    else:
                        value = json_pointer(response, pointer)
                    break
            if value is _MISSING:
                value = json_pointer(response, pointer)
            if value is _MISSING:
                return None
            values[pointer] = copy.deepcopy(value)
        observations.append({"action_id": action.get("action_id"), "values": values})
        observations[-1]["scope"] = scope
    return json_digest(observations) if observations else None


def _case_track(case: Mapping[str, Any]) -> str:
    row = case.get("_matrix_row")
    if isinstance(row, Mapping) and isinstance(row.get("track"), str):
        return row["track"]
    binding = _case_matrix_binding(case)
    if isinstance(binding, Mapping) and isinstance(binding.get("track"), str):
        return binding["track"]
    return "comparison"


def _usable_identity_field_value(field: str, value: Any) -> bool:
    """Validate the primitive/container shape of one track identity field."""

    if value is None or value is _MISSING or isinstance(value, bool):
        return False
    if field == "store_identity":
        # This is the runner's aggregate, not one of the nested response
        # wrappers.  Validate the observed representative value separately;
        # routing the whole aggregate through _usable_store_identity would
        # reject the required observations/authority fields as an arbitrary
        # container.
        return (
            isinstance(value, Mapping)
            and value.get("observed") is True
            and value.get("authority_correlated") is True
            and _usable_store_identity(value.get("value"))
            and isinstance(value.get("observations"), list)
            and bool(value.get("observations"))
            and isinstance(value.get("digest"), str)
            and re.fullmatch(r"[0-9a-f]{64}", value.get("digest", "")) is not None
        )
    if field in {"state_generation", "source_generation", "vector_generation"}:
        return isinstance(value, int) and value >= 0
    if field == "exact_scope":
        return isinstance(value, Mapping) and bool(value)
    if field == "marker":
        return isinstance(value, Mapping) and bool(value)
    if field == "registration_revision":
        return (
            isinstance(value, int)
            and value > 0
        ) or (
            isinstance(value, str)
            and bool(value.strip())
        )
    if field == "request_digest":
        return isinstance(value, (list, tuple)) and bool(value)
    if field == "route_identity":
        if not isinstance(value, Mapping):
            return False
        required = {
            "route",
            "entrypoint",
            "operation_kind",
            "classification",
            "availability",
            "contract_digest",
            "observed",
            "runner_owned",
        }
        if not required.issubset(value) or value.get("observed") is not True or value.get("runner_owned") is not True:
            return False
        if not all(isinstance(value.get(key), str) and bool(value.get(key, "").strip()) for key in required - {"observed", "runner_owned"}):
            return False
        if re.fullmatch(r"[0-9a-f]{64}", value.get("contract_digest", "")) is None:
            return False
        if value.get("entrypoint") == "mcp_stdio" and (
            not isinstance(value.get("tool"), str)
            or value.get("tool") != value.get("published_tool")
        ):
            return False
        return True
    if field.endswith("sha256") or field.endswith("_digest"):
        return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value.lower()) is not None
    if field in {
        "provider_id",
        "provider_instance",
        "protocol_version",
        "state_schema_version",
        "projection_key",
        "search_index_key",
        "calibration_profile_id",
    }:
        return isinstance(value, str) and bool(value.strip())
    if field == "rerank_policy_pins":
        return isinstance(value, (Mapping, list, tuple, str)) and bool(value)
    return _usable_store_identity(value)


def _find_observed_field(value: Any, field: str, _depth: int = 0) -> Any:
    """Find a named identity field in retained production response data."""

    if _depth > 64:
        return None
    wanted = str(field).lower()
    if isinstance(value, Mapping):
        for key, nested in value.items():
            key_text = str(key).lower()
            if key_text == wanted or key_text.rstrip("_") == wanted.rstrip("_"):
                if _usable_identity_field_value(field, nested):
                    return copy.deepcopy(nested)
            found = _find_observed_field(nested, field, _depth + 1)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _find_observed_field(nested, field, _depth + 1)
            if found is not None:
                return found
    return None


def _native_executable_path(path: Path) -> bool:
    """Return whether ``path`` has a native executable image, not a script."""

    try:
        if stat.S_ISLNK(path.lstat().st_mode) or not stat.S_ISREG(path.lstat().st_mode):
            return False
        with path.open("rb") as stream:
            magic = stream.read(4)
    except (OSError, TypeError, ValueError):
        return False
    # ELF, Mach-O (32/64-bit and byte order variants), and PE/COFF are the
    # supported native image families.  A shebang, Python file, or shell
    # script therefore cannot masquerade as a pinned encoder worker.
    return magic in {
        b"\x7fELF",
        b"\xfe\xed\xfa\xce",
        b"\xce\xfa\xed\xfe",
        b"\xfe\xed\xfa\xcf",
        b"\xcf\xfa\xed\xfe",
        b"\xca\xfe\xba\xbe",
        b"MZ\x90\x00",
    }


def _forbidden_ncm_worker_path(path: str | os.PathLike[str]) -> bool:
    """Reject shell/interpreter/echo paths before worker attestation."""

    try:
        lexical = Path(path).expanduser()
        resolved = lexical.resolve(strict=False)
    except (OSError, RuntimeError, TypeError, ValueError):
        return True
    names = {lexical.name.lower(), resolved.name.lower()}
    forbidden_names = {
        "sh",
        "bash",
        "dash",
        "zsh",
        "fish",
        "echo",
        "python",
        "python3",
        "pypy",
        "perl",
        "ruby",
        "node",
    }
    if names & forbidden_names or any(
        name.startswith(("python", "pypy", "bash", "shell")) for name in names
    ):
        return True
    # A system utility can be a native ELF/Mach-O image and still have no
    # relationship to the attested encoder implementation.  Require the
    # trusted manifest to point at a separately provisioned worker image;
    # common system executable roots are therefore never accepted as NCM
    # workers (for example ``/bin/sleep`` or ``/usr/bin/true``).
    system_roots = tuple(
        Path(candidate)
        for candidate in ("/bin", "/sbin", "/usr/bin", "/usr/sbin", "/usr/local/bin")
    )
    if any(
        resolved == root or root in resolved.parents
        for root in system_roots
    ):
        return True
    return not _native_executable_path(lexical)


def _process_executable_path(pid: int) -> tuple[str, str] | None:
    """Obtain a process executable path from the host process table.

    Linux exposes this through ``/proc/<pid>/exe``.  macOS intentionally has
    no procfs in many hardened environments, so use the OS-owned libproc
    ``proc_pidpath`` observation there.  The caller still hashes the returned
    regular file before accepting it.
    """

    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return None
    if sys.platform == "darwin":
        try:
            libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
            proc_pidpath = libproc.proc_pidpath
            proc_pidpath.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
            proc_pidpath.restype = ctypes.c_int
            buffer = ctypes.create_string_buffer(4096)
            length = int(proc_pidpath(pid, buffer, ctypes.c_uint32(len(buffer))))
            if length <= 0:
                return None
            raw = bytes(buffer.raw[:length])
            target = raw.decode("utf-8", errors="strict")
            if target and not target.endswith(" (deleted)"):
                return target, "libproc.proc_pidpath"
            return None
        except (OSError, UnicodeDecodeError, AttributeError, TypeError, ValueError):
            return None
    proc_link = Path("/proc") / str(pid) / "exe"
    try:
        target = os.readlink(proc_link)
    except (OSError, TypeError, ValueError):
        return None
    if not target or target.endswith(" (deleted)"):
        return None
    return target, "/proc/<pid>/exe"


def _macos_process_bsd_info(pid: int) -> dict[str, Any] | None:
    """Read one macOS process incarnation through ``proc_pidinfo``.

    macOS does not provide a stable procfs contract on hardened hosts.  The
    136-byte ``PROC_PIDTBSDINFO`` record is the OS-owned source for pid,
    parent pid, uid, status and start time; parsing ``ps`` output would leave
    both command selection and text formatting outside that contract.
    """

    if sys.platform != "darwin":
        return None
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return None

    class _ProcBsdInfo(ctypes.Structure):
        _fields_ = [
            ("pbi_flags", ctypes.c_uint32),
            ("pbi_status", ctypes.c_uint32),
            ("pbi_xstatus", ctypes.c_uint32),
            ("pbi_pid", ctypes.c_uint32),
            ("pbi_ppid", ctypes.c_uint32),
            ("pbi_uid", ctypes.c_uint32),
            ("pbi_gid", ctypes.c_uint32),
            ("pbi_ruid", ctypes.c_uint32),
            ("pbi_rgid", ctypes.c_uint32),
            ("pbi_svuid", ctypes.c_uint32),
            ("pbi_svgid", ctypes.c_uint32),
            ("rfu_1", ctypes.c_uint32),
            ("pbi_comm", ctypes.c_char * 16),
            ("pbi_name", ctypes.c_char * 32),
            ("pbi_nfiles", ctypes.c_uint32),
            ("pbi_pgid", ctypes.c_uint32),
            ("pbi_pjobc", ctypes.c_uint32),
            ("e_tdev", ctypes.c_uint32),
            ("e_tpgid", ctypes.c_uint32),
            ("pbi_nice", ctypes.c_int32),
            ("pbi_start_tvsec", ctypes.c_uint64),
            ("pbi_start_tvusec", ctypes.c_uint64),
        ]

    expected_size = 136
    try:
        if ctypes.sizeof(_ProcBsdInfo) != expected_size:
            return None
        libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        proc_pidinfo = libproc.proc_pidinfo
        proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        proc_pidinfo.restype = ctypes.c_int
        info = _ProcBsdInfo()
        observed_size = int(
            proc_pidinfo(
                ctypes.c_int(pid),
                ctypes.c_int(3),  # PROC_PIDTBSDINFO
                ctypes.c_uint64(0),
                ctypes.byref(info),
                ctypes.c_int(expected_size),
            )
        )
        if observed_size != expected_size or int(info.pbi_pid) != pid:
            return None
        parent_pid = int(info.pbi_ppid)
        uid = int(info.pbi_uid)
        start_seconds = int(info.pbi_start_tvsec)
        start_microseconds = int(info.pbi_start_tvusec)
        if parent_pid <= 0 or uid < 0 or start_seconds < 0 or start_microseconds < 0:
            return None
        return {
            "pid": pid,
            "ppid": parent_pid,
            "uid": uid,
            "status": int(info.pbi_status),
            "start_time": {
                "seconds": start_seconds,
                "microseconds": start_microseconds,
            },
            "incarnation": f"{pid}:{start_seconds}:{start_microseconds}",
            "source": "libproc.proc_pidinfo(PROC_PIDTBSDINFO)",
            "record_bytes": expected_size,
        }
    except (OSError, AttributeError, TypeError, ValueError, OverflowError):
        return None


def _process_incarnation(pid: int) -> dict[str, Any] | None:
    """Return stable pid/start-time identity without trusting child claims."""

    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return None
    if sys.platform == "darwin":
        return _macos_process_bsd_info(pid)
    if not Path("/proc").is_dir():
        return None
    try:
        raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="ascii")
        fields = raw.rsplit(")", 1)[1].split()
        if len(fields) <= 19:
            return None
        state = fields[0]
        parent_pid = int(fields[1])
        start_ticks = int(fields[19])
        status_text = (Path("/proc") / str(pid) / "status").read_text(encoding="ascii")
        uid_match = re.search(r"^Uid:\s+(\d+)", status_text, re.MULTILINE)
        uid = int(uid_match.group(1)) if uid_match else None
        if parent_pid <= 0 or start_ticks < 0 or uid is None:
            return None
        return {
            "pid": pid,
            "ppid": parent_pid,
            "uid": uid,
            "status": state,
            "start_time_ticks": start_ticks,
            "incarnation": f"{pid}:{start_ticks}",
            "source": "/proc/<pid>/stat",
        }
    except (OSError, UnicodeDecodeError, IndexError, TypeError, ValueError):
        return None


def _same_process_incarnation(left: Any, right: Any) -> bool:
    """Compare only stable process incarnation fields; status may change."""

    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return False
    return all(
        left.get(field) == right.get(field)
        for field in ("pid", "ppid", "uid", "incarnation")
    )


def _macos_lsof_executable_observation(
    pid: int,
    target_path: Path,
) -> dict[str, Any] | None:
    """Cross-check a macOS executable with the root-owned ``lsof`` binary."""

    if sys.platform != "darwin":
        return None
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return None
    try:
        lsof = Path("/usr/sbin/lsof")
        tool_info = lsof.lstat()
        if (
            stat.S_ISLNK(tool_info.st_mode)
            or not stat.S_ISREG(tool_info.st_mode)
            or tool_info.st_uid != 0
            or stat.S_IMODE(tool_info.st_mode) & 0o022
        ):
            return None
        target_info = target_path.lstat()
        if stat.S_ISLNK(target_info.st_mode) or not stat.S_ISREG(target_info.st_mode):
            return None
        target = target_path.resolve(strict=True)
        target_info = target.stat()
        completed = subprocess.run(
            [
                str(lsof),
                "-nP",
                "-a",
                "-p",
                str(pid),
                "-d",
                "txt",
                "-F0pDinf",
            ],
            env={"PATH": os.defpath},
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=2.0,
        )
        if completed.returncode != 0:
            return None
        records: list[dict[str, str]] = []
        current: dict[str, str] = {}
        for token in completed.stdout.split(b"\0"):
            if not token:
                continue
            try:
                text_token = token.decode("utf-8", errors="strict")
            except UnicodeDecodeError:
                return None
            key, field_value = text_token[:1], text_token[1:]
            if key == "p":
                current["p"] = field_value
            elif key == "f":
                if "f" in current:
                    records.append(current)
                    current = {"p": current.get("p", "")}
                current["f"] = field_value
            elif key in {"D", "i", "n"}:
                current[key] = field_value
            else:
                # -F0pDinf asks lsof for exactly these fields.  Unknown data
                # means the parser and the OS tool no longer agree.
                return None
        if "f" in current:
            records.append(current)
        matching = [
            record
            for record in records
            if record.get("p") == str(pid) and record.get("f", "").lower() == "txt"
        ]
        if len(matching) != 1:
            return None
        record = matching[0]
        record_path = Path(record.get("n", ""))
        if not record_path.is_absolute() or record_path.resolve(strict=True) != target:
            return None
        try:
            device = int(record.get("D", ""), 0)
            inode = int(record.get("i", ""), 0)
        except (TypeError, ValueError):
            return None
        if device != int(target_info.st_dev) or inode != int(target_info.st_ino):
            return None
        return {
            "observed": True,
            "tool": str(lsof),
            "tool_uid": int(tool_info.st_uid),
            "tool_mode": stat.S_IMODE(tool_info.st_mode),
            "pid": pid,
            "fd": "txt",
            "path": str(target),
            "device": device,
            "inode": inode,
            "format": "-F0pDinf",
        }
    except (OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError):
        return None


def _process_parent_pid(pid: int) -> tuple[int, str] | None:
    """Read a process parent id independently of child-provided claims."""

    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return None
    if Path("/proc").is_dir():
        try:
            raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="ascii")
            # comm may contain spaces and ')' characters; the fields after
            # the final ')' begin with state (field 3), then ppid (field 4).
            fields = raw.rsplit(")", 1)[1].split()
            parent = int(fields[1])
            return (parent, "/proc/<pid>/stat") if parent > 0 else None
        except (OSError, UnicodeDecodeError, IndexError, TypeError, ValueError):
            return None
    if sys.platform == "darwin":
        observation = _macos_process_bsd_info(pid)
        if isinstance(observation, Mapping):
            parent = observation.get("ppid")
            if isinstance(parent, int) and not isinstance(parent, bool) and parent > 0:
                return (parent, "libproc.proc_pidinfo(PROC_PIDTBSDINFO)")
        return None
    return None


def _observed_worker_process(pid: Any) -> dict[str, Any] | None:
    """Read the measured executable for a live worker process independently."""

    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return None
    try:
        os.kill(pid, 0)
        executable = _process_executable_path(pid)
        parent = _process_parent_pid(pid)
        process_incarnation = _process_incarnation(pid)
        if executable is None or parent is None or process_incarnation is None:
            return None
        target, executable_source = executable
        target_path = Path(target)
        lexical_info = target_path.lstat()
        if stat.S_ISLNK(lexical_info.st_mode) or not stat.S_ISREG(lexical_info.st_mode):
            return None
        target_path = target_path.resolve(strict=True)
        info = verify_binary(target_path, f"ncm worker process {pid}")
        lsof_observation = _macos_lsof_executable_observation(pid, target_path)
        if sys.platform == "darwin" and lsof_observation is None:
            return None
    except (OSError, RuntimeError, TypeError, ValueError, BinaryError):
        return None
    parent_pid, parent_source = parent
    try:
        # Ensure the pid/executable observation did not race process exit or
        # pid reuse while the executable was being hashed.
        os.kill(pid, 0)
        post_executable = _process_executable_path(pid)
        post_incarnation = _process_incarnation(pid)
        if (
            post_executable is None
            or Path(post_executable[0]).resolve(strict=True) != target_path
            or not _same_process_incarnation(process_incarnation, post_incarnation)
        ):
            return None
    except (OSError, RuntimeError, TypeError, ValueError):
        return None
    parent_observation = _process_executable_path(parent_pid) if parent_pid > 1 else None
    parent_incarnation = _process_incarnation(parent_pid)
    parent_path = None
    parent_info = None
    if parent_observation is not None:
        try:
            parent_path = Path(parent_observation[0]).resolve(strict=True)
            parent_info = verify_binary(parent_path, f"ncm worker parent process {parent_pid}")
        except (OSError, RuntimeError, TypeError, ValueError, BinaryError):
            parent_path = None
            parent_info = None
    return {
        "observed": True,
        "pid": pid,
        "binary_path": str(target_path),
        "sha256": info["sha256"],
        "source": executable_source,
        "parent_pid": parent_pid,
        "parent_source": parent_source,
        "parent_binary_path": str(parent_path) if parent_path is not None else None,
        "parent_sha256": parent_info["sha256"] if isinstance(parent_info, Mapping) else None,
        "process_incarnation": process_incarnation,
        "parent_incarnation": parent_incarnation,
        "lsof": lsof_observation,
    }


def _seal_immutable_copy_root(root: Path, *, label: str) -> None:
    """Seal a worker copy root before any child can use it."""

    _require(isinstance(root, Path), "immutable copy root must be a Path", BinaryError)
    try:
        info = root.lstat()
        _require(stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode), f"{label} copy root is not a real directory", BinaryError)
        _require(not hasattr(os, "getuid") or info.st_uid == os.getuid(), f"{label} copy root is not runner-owned", BinaryError)
        _require(not (stat.S_IMODE(info.st_mode) & 0o077), f"{label} copy root is not owner-private", BinaryError)
        # Keep the seal mode identical to the mode used while copying.  A
        # previous implementation changed 0700 to 0555 and then checked for
        # zero group/other bits, which made every correctly sealed root fail
        # its own validation (0555 has group/other read/execute bits).
        root.chmod(IMMUTABLE_COPY_MODE)
        sealed = root.lstat()
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise BinaryError(f"{label} copy root could not be sealed: {error}") from error
    _require(
        stat.S_IMODE(sealed.st_mode) == IMMUTABLE_COPY_MODE,
        f"{label} copy root did not retain the owner-only immutable mode",
        BinaryError,
    )


def _ncm_worker_identity(
    *,
    result: Mapping[str, Any],
    side: str,
    side_results: Sequence[Mapping[str, Any]],
    expected_source: Mapping[str, Any] | None,
    trusted_attestation: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Measure a pinned NCM worker and relate its binary to its live process."""

    # Setup/composition rows can complete before the NCM operation itself.  Do
    # not accidentally treat the first completed row as the worker result:
    # select the completed row carrying worker evidence, with a deterministic
    # completed-row fallback so the missing-evidence error remains typed.
    completed_results = [
        side_result
        for side_result in side_results
        if isinstance(side_result, Mapping)
        and side_result.get("status") in ("completed", "pass", "error")
    ]
    candidate: Mapping[str, Any] | None = None
    for side_result in completed_results:
        response_value = side_result.get("response")
        has_worker_evidence = any(
            key in side_result
            for key in ("worker_sha256", "worker_process", "process")
        )
        if isinstance(response_value, Mapping):
            has_worker_evidence = has_worker_evidence or any(
                key in response_value
                for key in (
                    "worker_sha256",
                    "worker_process",
                    "process",
                    "worker_functionality",
                    "functionality_probe",
                    "worker_probe",
                )
            )
        if has_worker_evidence:
            candidate = side_result
            break
    if candidate is None and completed_results:
        candidate = completed_results[0]
    if candidate is None:
        return None, "NCM worker execution result is missing"
    if not isinstance(trusted_attestation, Mapping) or trusted_attestation.get("_runner_input") is not True:
        return None, "NCM worker requires an explicit runner-owned trusted attestation input"
    if not _authenticated_result_observation(candidate):
        return None, "NCM worker identity is not authenticated by a daemon action receipt and result digest"
    runtime_values = result.get("daemon_runtime")
    runtime_identity = runtime_values.get(side) if isinstance(runtime_values, Mapping) else None
    if not isinstance(runtime_identity, Mapping):
        return None, "NCM worker daemon runtime identity is unavailable"
    receipt_wrapper = candidate.get("action_receipt")
    receipt = receipt_wrapper.get("receipt") if isinstance(receipt_wrapper, Mapping) else None
    receipt_generation = receipt.get("daemon_generation") if isinstance(receipt, Mapping) else None
    if (
        not isinstance(receipt_wrapper, Mapping)
        or not isinstance(receipt, Mapping)
        or not isinstance(receipt_generation, Mapping)
        or receipt_wrapper.get("authority_digest") != _authority_identity_digest(runtime_identity)
        or receipt_generation.get("epoch") != runtime_identity.get("epoch")
        or receipt_generation.get("process_run_id") != runtime_identity.get("process_run_id")
    ):
        return None, "NCM worker receipt authority does not match the runner daemon runtime"
    input_path = trusted_attestation.get("_runner_input_path")
    input_sha = trusted_attestation.get("_runner_input_sha256")
    if not isinstance(input_path, str) or not isinstance(input_sha, str):
        return None, "NCM worker trusted attestation lacks its runner input identity"
    try:
        raw_attestation, _ = _read_stable_file(Path(input_path), label="NCM worker attestation")
        parsed_attestation = _strict_json_loads(raw_attestation.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError, RunnerError) as error:
        return None, f"NCM worker trusted attestation could not be re-read: {error}"
    if (
        not isinstance(parsed_attestation, Mapping)
        or bytes_digest(raw_attestation) != input_sha
        or set(parsed_attestation) != set(NCM_WORKER_ATTESTATION_FIELDS)
        or raw_attestation != _json_bytes(parsed_attestation) + b"\n"
    ):
        return None, "NCM worker trusted attestation changed or is not canonical"
    attestation = {field: copy.deepcopy(parsed_attestation[field]) for field in NCM_WORKER_ATTESTATION_FIELDS}
    if any(not isinstance(attestation.get(field), str) or not attestation.get(field, "").strip() for field in NCM_WORKER_ATTESTATION_FIELDS):
        return None, "NCM worker attestation lacks required identity fields"
    if (
        attestation.get("format") != NCM_WORKER_ATTESTATION_FORMAT
        or attestation.get("kind") != "ncm_encoder_worker"
        or attestation.get("worker_id") != NCM_WORKER_ID
    ):
        return None, "NCM worker attestation format/kind is not recognized"
    for field in (
        "binary_sha256",
        "implementation_sha256",
        "model_artifact_sha256",
        "tokenizer_sha256",
        "vector_fixture_digest",
        "pin_sha256",
    ):
        if re.fullmatch(r"[0-9a-f]{64}", attestation[field].lower()) is None:
            return None, f"NCM worker attestation {field} is not a SHA-256"
    revision = attestation.get("source_revision", "").lower()
    if re.fullmatch(r"[0-9a-f]{40}", revision) is None:
        return None, "NCM worker source revision is not a full Git SHA"
    try:
        worker_path = Path(attestation["binary_path"]).expanduser()
        if not worker_path.is_absolute() or _forbidden_ncm_worker_path(worker_path):
            return None, "NCM worker must be an attested native executable, not a shell/interpreter/script"
        measured_worker = verify_binary(worker_path, f"{side} NCM worker")
        source_root = Path(attestation["source_root"]).expanduser()
        if not source_root.is_absolute() or not source_root.is_dir():
            return None, "NCM worker source root is absent or not absolute"
        source_root = source_root.resolve(strict=True)
        top = _git(source_root, "rev-parse", "--show-toplevel")
        if top.returncode != 0 or Path(top.stdout.strip()).expanduser().resolve() != source_root:
            return None, "NCM worker source root is not the exact Git top-level"
        head = _git(source_root, "rev-parse", "--verify", "HEAD")
        if head.returncode != 0 or head.stdout.strip().lower() != revision:
            return None, "NCM worker source revision does not match its checkout"
    except (BinaryError, OSError, RuntimeError, TypeError, ValueError) as error:
        return None, f"NCM worker could not be measured: {error}"
    if measured_worker["sha256"] != attestation["binary_sha256"].lower():
        return None, "NCM worker binary hash does not match its attestation"
    if isinstance(expected_source, Mapping):
        if expected_source.get("path") != str(source_root) or expected_source.get("revision") != revision:
            return None, "NCM worker source is not bound to the observed side checkout"
    pin_payload = {
        key: attestation.get(key) for key in NCM_WORKER_PIN_FIELDS
    }
    if attestation.get("pin_sha256") != json_digest(pin_payload):
        return None, "NCM worker pin digest does not match its canonical identity"
    response_value = candidate.get("response")
    worker_claim = _find_observed_field(response_value, "worker_sha256")
    if worker_claim is None:
        worker_claim = candidate.get("worker_sha256")
    if not isinstance(worker_claim, str) or worker_claim.lower() != measured_worker["sha256"]:
        return None, "NCM worker hash is missing or does not match independent measurement"
    process_claim = candidate.get("worker_process")
    if not isinstance(process_claim, Mapping):
        process = candidate.get("process")
        process_claim = process.get("worker") if isinstance(process, Mapping) else None
    if not isinstance(process_claim, Mapping) and isinstance(response_value, Mapping):
        for key in ("worker_process", "process"):
            nested = response_value.get(key)
            if isinstance(nested, Mapping):
                process_claim = nested
                break
    if not isinstance(process_claim, Mapping):
        return None, "NCM worker process observation is missing"
    observed_process = _observed_worker_process(process_claim.get("pid"))
    if observed_process is None:
        return None, "NCM worker process executable could not be independently observed"
    if (
        process_claim.get("observed") is not True
        or process_claim.get("pid") != observed_process["pid"]
        or process_claim.get("binary_path") != observed_process["binary_path"]
        or process_claim.get("sha256", "").lower() != observed_process["sha256"]
        or observed_process["binary_path"] != measured_worker["path"]
        or observed_process["sha256"] != measured_worker["sha256"]
    ):
        return None, "NCM worker process is not the independently measured attested executable"
    # The child may repeat a daemon identity in its response, but the
    # authority for this relation is the runner's owned daemon runtime and
    # the signed receipt above.  Never select a child-supplied identity.
    daemon_identity = runtime_identity
    daemon_pid = daemon_identity.get("pid")
    daemon_sha = daemon_identity.get("binary_sha256")
    daemon_incarnation = daemon_identity.get("process_incarnation")
    worker_parent_incarnation = observed_process.get("parent_incarnation")
    if (
        isinstance(daemon_pid, bool)
        or not isinstance(daemon_pid, int)
        or daemon_pid <= 0
        or not isinstance(daemon_sha, str)
        or re.fullmatch(r"[0-9a-f]{64}", daemon_sha.lower()) is None
        or observed_process.get("parent_pid") != daemon_pid
        or observed_process.get("parent_sha256") != daemon_sha.lower()
        or not _same_process_incarnation(worker_parent_incarnation, daemon_incarnation)
    ):
        return None, "NCM worker is not a child of the independently observed owned daemon"
    functionality = None
    if isinstance(response_value, Mapping):
        for key in ("worker_functionality", "functionality_probe", "worker_probe"):
            nested = response_value.get(key)
            if isinstance(nested, Mapping):
                functionality = nested
                break
    if not isinstance(functionality, Mapping) or functionality.get("observed") is not True:
        return None, "NCM worker functionality probe is missing or unobserved"
    if (
        functionality.get("worker_sha256") != measured_worker["sha256"]
        or functionality.get("implementation_sha256") != attestation["implementation_sha256"]
    ):
        return None, "NCM worker functionality probe is not bound to the measured implementation"
    functionality_digest_key = next(
        (key for key in ("probe_sha256", "functionality_sha256", "digest") if key in functionality),
        None,
    )
    functionality_digest = functionality.get(functionality_digest_key) if functionality_digest_key else None
    if (
        not isinstance(functionality_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", functionality_digest.lower()) is None
    ):
        return None, "NCM worker functionality probe lacks a canonical digest"
    functionality_payload = dict(functionality)
    functionality_payload.pop(functionality_digest_key, None)
    if functionality_digest.lower() != json_digest(functionality_payload):
        return None, "NCM worker functionality probe digest is not canonical"
    # Provider/schema/generation values are accepted only from the exact
    # receipt-authenticated response bytes.  The worker manifest supplies the
    # implementation/model/tokenizer/vector pins; it cannot invent runtime
    # state values, and the response cannot replace those pins.
    response_identity: dict[str, Any] = {}
    for field in ("provider_id", "state_schema_version", "state_generation"):
        identity_value = _find_observed_field(response_value, field)
        if identity_value is None:
            return None, f"NCM daemon result lacks authenticated {field}"
        response_identity[field] = identity_value
    if response_identity.get("provider_id") != "ncm":
        return None, "NCM daemon result provider_id is not the pinned ncm identity"
    if (
        not isinstance(response_identity.get("state_schema_version"), str)
        or not response_identity["state_schema_version"].strip()
        or not isinstance(response_identity.get("state_generation"), int)
        or isinstance(response_identity.get("state_generation"), bool)
        or response_identity["state_generation"] < 0
    ):
        return None, "NCM daemon result state identity has an invalid typed shape"
    for field in (
        "implementation_sha256",
        "protocol_version",
        "model_artifact_sha256",
        "tokenizer_sha256",
        "vector_fixture_digest",
    ):
        response_pin = _find_observed_field(response_value, field)
        if response_pin is None:
            return None, f"NCM daemon result lacks authenticated {field} pin"
        if not _json_semantic_equal(response_pin, attestation[field]):
            return None, f"NCM daemon result {field} pin disagrees with trusted worker attestation"
    artifact_root = result.get("artifact_directory")
    if not isinstance(artifact_root, str) or not artifact_root.strip():
        return None, "NCM worker immutable copy root is unavailable"
    copy_root = Path(artifact_root).expanduser() / "ncm-worker-copy" / side
    try:
        copied = _copy_immutable_binary(
            {"path": measured_worker["path"], "sha256": measured_worker["sha256"]},
            root=copy_root,
            side="worker",
            expected_sha256=measured_worker["sha256"],
        )
        _seal_immutable_copy_root(copy_root, label=f"{side} NCM worker")
    except (BinaryError, OSError, RuntimeError, TypeError, ValueError) as error:
        return None, f"NCM worker immutable copy failed: {error}"
    return (
        {
            "observed": True,
            "worker_id": attestation["worker_id"],
            **response_identity,
            "protocol_version": attestation["protocol_version"],
            "implementation_sha256": attestation["implementation_sha256"],
            "model_artifact_sha256": attestation["model_artifact_sha256"],
            "tokenizer_sha256": attestation["tokenizer_sha256"],
            "vector_fixture_digest": attestation["vector_fixture_digest"],
            "worker_sha256": measured_worker["sha256"],
            "worker_binary_path": measured_worker["path"],
            "worker_attestation": copy.deepcopy(dict(attestation)),
            "worker_attestation_input": {
                "path": input_path,
                "sha256": input_sha,
                "bytes": trusted_attestation.get("_runner_input_bytes"),
            },
            "worker_process": observed_process,
            "worker_functionality": copy.deepcopy(dict(functionality)),
            "daemon_identity": copy.deepcopy(dict(daemon_identity)),
            "receipt_authority_digest": receipt_wrapper.get("authority_digest"),
            "receipt_result_digest": receipt.get("result_digest"),
            "immutable_copy": copied,
            "immutable_copy_root": str(copy_root),
        },
        None,
    )


def _track_identity_evidence(
    result: Mapping[str, Any],
    case: Mapping[str, Any],
    original_binary: Mapping[str, Any],
    product_binary: Mapping[str, Any],
    process_identity: Mapping[str, Any],
    trusted_ncm_attestation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Extract identity fields for the selected readiness track.

    Identity is observed from runner-bound process/route/store records and
    production responses.  A response saying only ``provider_id`` is not
    sufficient to establish the store or daemon that produced it.
    """

    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(isinstance(case, Mapping), "case must be an object", RunnerError)
    _require(isinstance(original_binary, Mapping), "original binary evidence must be an object", RunnerError)
    _require(isinstance(product_binary, Mapping), "product binary evidence must be an object", RunnerError)
    _require(isinstance(process_identity, Mapping), "process identity evidence must be an object", RunnerError)
    track = _case_track(case)
    matrix_row = case.get("_matrix_row")
    row_requirements = matrix_row.get("identity_evidence", {}) if isinstance(matrix_row, Mapping) else {}
    matrix_oracle_kind = (
        matrix_row.get("oracle_kind")
        if isinstance(matrix_row, Mapping)
        else (_case_matrix_binding(case) or {}).get("oracle_kind")
        if isinstance(_case_matrix_binding(case), Mapping)
        else None
    )
    required = set(str(key) for key in row_requirements) if isinstance(row_requirements, Mapping) else set()
    track_requirements = {
        "native": {"provider_id", "registration_revision", "provider_instance", "exact_scope", "request_digest", "contribution_digest", "marker"},
        "ncm": {"provider_id", "implementation_sha256", "protocol_version", "state_schema_version", "state_generation", "model_artifact_sha256", "tokenizer_sha256", "vector_fixture_digest", "worker_sha256"},
        "semantic": {"model_artifact_sha256", "tokenizer_sha256", "projection_key", "search_index_key", "source_generation", "vector_generation", "capability_manifest_digest", "calibration_profile_id", "calibration_digest", "rerank_policy_pins"},
        "release": set(),
    }
    required.update(track_requirements.get(track, set()))
    # NCM readiness rows use an independent real-worker conformance oracle.
    # The pinned 570 checkout has no shipped encoder worker, so the original
    # side is a declared unavailable worker boundary while the candidate must
    # still provide a separately measured worker.  Keep the asymmetry in the
    # required-field map instead of manufacturing a paired worker identity.
    independent_ncm = (
        track == "ncm"
        and matrix_oracle_kind == "independent_real_worker_conformance"
    )
    required_sides: dict[str, set[str]] = {
        field: set(SIDE_NAMES) for field in required
    }
    if independent_ncm and "worker_sha256" in required_sides:
        required_sides["worker_sha256"] = {"product"}
        # The reference half of this oracle is deliberately unavailable: the
        # 570 checkout has no encoder worker.  All worker-pinned NCM identity
        # fields therefore belong to the independently attested candidate
        # side; requiring them from the reference would manufacture a paired
        # worker identity.
        for field in track_requirements["ncm"] - {"worker_sha256"}:
            required_sides[field] = {"product"}
    observed: dict[str, dict[str, Any]] = {field: {} for field in sorted(required)}
    invalid: dict[str, dict[str, str]] = {}
    side_results: dict[str, list[Mapping[str, Any]]] = {side: [] for side in SIDE_NAMES}
    for side in SIDE_NAMES:
        side_results[side] = [side_result for _, _, side_result in _iter_side_observations(result, side)]
    request_evidence = _request_digest_evidence(result)
    for field in required:
        for side in sorted(required_sides.get(field, set(SIDE_NAMES))):
            value = None
            if field == "reference_binary_sha256":
                value = original_binary.get("sha256")
            elif field == "candidate_binary_sha256":
                value = product_binary.get("sha256")
            elif field == "process_identity":
                value = process_identity.get(side)
                if not _usable_process_identity(value):
                    value = None
                    for side_result in side_results[side]:
                        candidate = side_result.get("daemon_identity")
                        if _usable_process_identity(candidate):
                            value = candidate
                            break
            elif field == "store_identity":
                value = _observed_store_identity(result, side)
                if value.get("observed") is not True:
                    value = None
            elif field == "request_digest":
                side_requests = request_evidence.get(side)
                value = side_requests if isinstance(side_requests, list) and side_requests else None
            elif field == "route_identity":
                for side_result in side_results[side]:
                    candidate = side_result.get("route_identity")
                    if (
                        isinstance(candidate, Mapping)
                        and candidate.get("observed") is True
                        and isinstance(candidate.get("route"), str)
                        and isinstance(candidate.get("entrypoint"), str)
                        and _authenticated_result_observation(side_result)
                        and _runner_identity_binding_observed(
                            side_result,
                            side=side,
                            binary_digest=(
                                original_binary.get("sha256")
                                if side == "original"
                                else product_binary.get("sha256")
                            ),
                        )
                        and (
                            candidate.get("entrypoint") != "mcp_stdio"
                            or (
                                isinstance(candidate.get("tool"), str)
                                and candidate.get("tool") == candidate.get("published_tool")
                            )
                        )
                    ):
                        value = candidate
                        break
            else:
                for side_result in side_results[side]:
                    if not _authenticated_result_observation(side_result):
                        continue
                    if not _runner_identity_binding_observed(
                        side_result,
                        side=side,
                        binary_digest=(
                            original_binary.get("sha256")
                            if side == "original"
                            else product_binary.get("sha256")
                        ),
                    ):
                        continue
                    value = _find_observed_field(side_result.get("response"), field)
                    if value is not None:
                        break
            if value is not None and value != {} and value != []:
                observed[field][side] = value
                invalid_reason = None
                if not _usable_identity_field_value(field, value):
                    invalid_reason = "identity value has an invalid typed shape"
                elif field == "exact_scope":
                    required_scope = {
                        "profile_id",
                        "project_id",
                        "repository_identity",
                        "worktree_identity",
                        "branch_identity",
                        "agent_session_id",
                        "resolved_scope_digest",
                    }
                    if set(value) != required_scope or any(
                        not isinstance(value.get(key), str) or not value.get(key, "").strip()
                        for key in required_scope
                    ):
                        invalid_reason = "exact_scope must contain all runner scope dimensions"
                    elif re.fullmatch(r"[0-9a-f]{64}", value["resolved_scope_digest"].lower()) is None:
                        invalid_reason = "exact_scope.resolved_scope_digest is not a SHA-256"
                elif field == "marker":
                    required_marker = {
                        "provider_id",
                        "registration_revision",
                        "provider_instance",
                        "scope_digest",
                        "request_digest",
                        "contribution_digest",
                    }
                    if not required_marker.issubset(value):
                        invalid_reason = "Native marker lacks provider, revision, scope and digest fields"
                elif field == "registration_revision":
                    if (
                        isinstance(value, int)
                        and not isinstance(value, bool)
                        and value > 0
                    ):
                        pass
                    elif re.fullmatch(r"[0-9a-f]{40}", str(value).lower()) is None:
                        invalid_reason = "registration_revision must be a positive integer or full Git SHA"
                elif field.endswith("sha256") or field.endswith("_digest"):
                    if field == "request_digest":
                        digest_values = value if isinstance(value, list) else []
                        if not digest_values or any(
                            not isinstance(item, Mapping)
                            or re.fullmatch(r"[0-9a-f]{64}", str(item.get("sha256", ""))) is None
                            for item in digest_values
                        ):
                            invalid_reason = "request digest entries must carry 64-hex hashes"
                    elif not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value.lower()) is None:
                        invalid_reason = "identity digest must be a 64-character lowercase SHA-256"
                elif field == "state_generation" or field == "source_generation" or field == "vector_generation":
                    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        invalid_reason = "generation must be a non-negative integer"
                elif field in ("provider_id", "provider_instance", "protocol_version", "state_schema_version", "projection_key", "search_index_key", "calibration_profile_id"):
                    if not isinstance(value, str) or not value.strip():
                        invalid_reason = "identity value must be a non-empty string"
                elif field in ("exact_scope", "rerank_policy_pins") and not isinstance(value, (Mapping, list, tuple, str)):
                    invalid_reason = "identity scope/policy value has an invalid type"
                if invalid_reason is not None:
                    invalid.setdefault(field, {})[side] = invalid_reason

    # NCM workers are an independently attested production boundary.  A path
    # or hash echoed in a response is never enough: the runner measures the
    # native image, verifies its source pin, copies it into a sealed root, and
    # relates that hash to the live worker process executable.
    if track == "ncm":
        source_bindings = result.get("source_bindings", {})
        for side in SIDE_NAMES:
            if independent_ncm and side == "original":
                # Preserve the missing worker as explicit conformance
                # evidence.  It is intentionally not a worker_sha256 value:
                # a textual unavailable marker must never satisfy a digest
                # requirement or be mistaken for a paired process.
                observed.setdefault("worker_conformance", {})[side] = {
                    "status": "unavailable",
                    "reason": "pinned 570 reference checkout does not ship an NCM encoder worker",
                    "oracle_kind": "independent_real_worker_conformance",
                }
                continue
            expected_source = source_bindings.get(side) if isinstance(source_bindings, Mapping) else None
            worker_observation, worker_error = _ncm_worker_identity(
                result=result,
                side=side,
                side_results=side_results[side],
                expected_source=expected_source if isinstance(expected_source, Mapping) else None,
                trusted_attestation=trusted_ncm_attestation,
            )
            if worker_observation is None:
                invalid.setdefault("worker_sha256", {})[side] = worker_error or "NCM worker evidence is unavailable"
                continue
            runtime_identity = (
                result.get("daemon_runtime", {}).get(side)
                if isinstance(result.get("daemon_runtime"), Mapping)
                else None
            )
            worker_daemon = worker_observation.get("daemon_identity")
            worker_process = worker_observation.get("worker_process")
            if (
                not isinstance(runtime_identity, Mapping)
                or not isinstance(worker_daemon, Mapping)
                or not isinstance(worker_process, Mapping)
                or worker_observation.get("receipt_authority_digest") != _authority_identity_digest(runtime_identity)
                or worker_process.get("parent_pid") != runtime_identity.get("pid")
                or worker_process.get("parent_sha256") != runtime_identity.get("binary_sha256")
                or worker_daemon.get("pid") != runtime_identity.get("pid")
                or worker_daemon.get("binary_sha256") != runtime_identity.get("binary_sha256")
                or worker_daemon.get("process_run_id") != runtime_identity.get("process_run_id")
                or worker_daemon.get("epoch") != runtime_identity.get("epoch")
            ):
                invalid.setdefault("worker_sha256", {})[side] = (
                    "NCM worker parent/receipt authority is not bound to the runner daemon runtime"
                )
                continue
            observed.setdefault("worker_sha256", {})[side] = worker_observation["worker_sha256"]
            observed.setdefault("worker_binary_sha256", {})[side] = worker_observation["worker_sha256"]
            observed.setdefault("worker_attestation", {})[side] = copy.deepcopy(worker_observation["worker_attestation"])
            observed.setdefault("worker_attestation_input", {})[side] = copy.deepcopy(
                worker_observation.get("worker_attestation_input")
            )
            observed.setdefault("worker_process", {})[side] = copy.deepcopy(worker_observation["worker_process"])
            # These track pins come from the runner-owned manifest and the
            # independently measured worker observation.  A response field
            # with the same spelling cannot replace them.
            for field in (
                "provider_id",
                "implementation_sha256",
                "protocol_version",
                "state_schema_version",
                "state_generation",
                "model_artifact_sha256",
                "tokenizer_sha256",
                "vector_fixture_digest",
            ):
                if field in worker_observation:
                    observed.setdefault(field, {})[side] = worker_observation[field]
            observed.setdefault("worker_immutable_copy", {})[side] = copy.deepcopy(worker_observation["immutable_copy"])

    # Frozen row descriptors may pin an expected identity value.  Never let a
    # fixture relabel that value or weaken a required-per-attempt descriptor.
    if isinstance(row_requirements, Mapping):
        for field, descriptor in row_requirements.items():
            if not isinstance(descriptor, Mapping):
                invalid.setdefault(str(field), {})["both"] = "identity requirement descriptor is malformed"
                continue
            if descriptor.get("state") != "required_per_attempt":
                invalid.setdefault(str(field), {})["both"] = "identity requirement is not required per attempt"
            expected_value = descriptor.get("value")
            if expected_value is not None:
                for side in SIDE_NAMES:
                    actual = observed.get(str(field), {}).get(side)
                    if actual is None or not _json_semantic_equal(actual, expected_value):
                        invalid.setdefault(str(field), {})[side] = "observed identity does not match frozen expected value"
    identity_requirements = (
        matrix_row.get("track_identity_requirements")
        if isinstance(matrix_row, Mapping)
        else None
    )
    if isinstance(identity_requirements, Mapping):
        for field, descriptor in identity_requirements.items():
            if not isinstance(descriptor, Mapping):
                _require(
                    isinstance(descriptor, str) and bool(descriptor.strip())
                    or isinstance(descriptor, list)
                    and bool(descriptor)
                    and all(isinstance(item, str) and bool(item.strip()) for item in descriptor),
                    f"frozen identity requirement {field!r} is malformed",
                    RunnerError,
                )
                # Literal provider/protocol identities are authoritative. The
                # other strings/lists are reviewed requirement descriptions.
                if field in ("provider_id", "protocol_version"):
                    for side in SIDE_NAMES:
                        actual = observed.get(str(field), {}).get(side)
                        if actual is None or not _json_semantic_equal(actual, descriptor):
                            invalid.setdefault(str(field), {})[side] = "observed identity does not match frozen expected value"
                continue
            if not isinstance(descriptor.get("state"), str) or not descriptor.get("state", "").startswith("required_"):
                invalid.setdefault(str(field), {})["both"] = "identity requirement is not required per attempt"
            expected_value = descriptor.get("value")
            if expected_value is not None:
                for side in SIDE_NAMES:
                    actual = observed.get(str(field), {}).get(side)
                    if actual is None or not _json_semantic_equal(actual, expected_value):
                        invalid.setdefault(str(field), {})[side] = "observed identity does not match frozen expected value"
    missing = {
        field: [side for side in sorted(required_sides.get(field, set(SIDE_NAMES))) if side not in observed[field]]
        for field in sorted(required)
        if any(side not in observed[field] for side in required_sides.get(field, set(SIDE_NAMES)))
    }
    return {
        "track": track,
        "required_fields": sorted(required),
        "required_sides": {
            field: sorted(required_sides.get(field, set(SIDE_NAMES)))
            for field in sorted(required)
        },
        "oracle_kind": matrix_oracle_kind,
        "observed": observed,
        "missing_fields": missing,
        "invalid_fields": invalid,
        "complete": not missing and not invalid,
    }


def _request_digest_evidence(result: Mapping[str, Any]) -> dict[str, Any]:
    """Return hashes of the exact request artifacts sent to each side."""

    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    evidence: dict[str, list[dict[str, Any]]] = {side: [] for side in SIDE_NAMES}
    for side in SIDE_NAMES:
        for scope, action, side_result in _iter_side_observations(result, side):
            request_hash = side_result.get("request_sha256")
            if not isinstance(request_hash, str):
                continue
            artifact = side_result.get("request_artifact")
            request_value: Mapping[str, Any] | None = None
            if isinstance(artifact, Mapping):
                artifact_hash = artifact.get("sha256")
                artifact_path = artifact.get("path")
                if artifact_hash != request_hash or not isinstance(artifact_path, str) or not artifact_path:
                    continue
                try:
                    request_bytes = Path(artifact_path).read_bytes()
                    parsed_request = _strict_json_loads(request_bytes.decode("utf-8", errors="strict"))
                    if (
                        not isinstance(parsed_request, Mapping)
                        or bytes_digest(request_bytes) != request_hash
                        or request_bytes != _json_bytes(parsed_request) + b"\n"
                    ):
                        continue
                    request_value = parsed_request
                except (OSError, ValueError, TypeError, RecursionError, RunnerError):
                    continue
            elif "request" in side_result and isinstance(side_result.get("request"), Mapping):
                try:
                    request_value = side_result["request"]
                    expected_hash = bytes_digest(_json_bytes(request_value) + b"\n")
                except RunnerError:
                    continue
                if expected_hash != request_hash:
                    continue
            else:
                # A bare producer-supplied hash is not evidence of the bytes
                # sent.  Require either a retained request artifact or the
                # exact rendered request object from which its canonical bytes
                # can be reconstructed.
                continue
            mcp_exchange: dict[str, Any] | None = None
            if side_result.get("entrypoint") == "mcp_stdio":
                process = side_result.get("process")
                stdin_ref = process.get("stdin") if isinstance(process, Mapping) else None
                stdin_hash = process.get("stdin_exchange_sha256") if isinstance(process, Mapping) else None
                if (
                    not isinstance(stdin_ref, Mapping)
                    or not isinstance(stdin_ref.get("path"), str)
                    or not isinstance(stdin_ref.get("sha256"), str)
                    or stdin_ref.get("sha256") != stdin_hash
                    or (
                        side_result.get("request_exchange_sha256") is not None
                        and side_result.get("request_exchange_sha256") != stdin_hash
                    )
                    or re.fullmatch(r"[0-9a-f]{64}", str(stdin_hash)) is None
                ):
                    continue
                try:
                    stdin_bytes = Path(stdin_ref["path"]).read_bytes()
                except (OSError, TypeError, ValueError):
                    continue
                if bytes_digest(stdin_bytes) != stdin_hash or stdin_ref.get("bytes") != len(stdin_bytes):
                    continue
                route_identity = side_result.get("route_identity")
                published_tool = (
                    route_identity.get("published_tool")
                    if isinstance(route_identity, Mapping)
                    else None
                )
                if not isinstance(published_tool, str) or not isinstance(request_value, Mapping):
                    continue
                case_id = result.get("case_id")
                action_id = action.get("action_id")
                if not _validate_mcp_stdin_exchange(
                    stdin_bytes,
                    side=side,
                    case_id=case_id if isinstance(case_id, str) else "",
                    action_id=action_id if isinstance(action_id, str) else "",
                    request=request_value,
                    tool=published_tool,
                    action_prepare=(
                        side_result.get("action_prepare")
                        if isinstance(side_result.get("action_prepare"), Mapping)
                        else None
                    ),
                ):
                    continue
                mcp_exchange = {
                    "path": stdin_ref["path"],
                    "bytes": len(stdin_bytes),
                    "sha256": stdin_hash,
                    "exchange_sha256": bytes_digest(stdin_bytes),
                    "request_id": f"native-original/{side}/{case_id}/{action_id}",
                    "tool": published_tool,
                }
            evidence[side].append(
                {
                    "scope": scope,
                    "action_id": action.get("action_id"),
                    "route": action.get("route"),
                    "sha256": request_hash,
                    "artifact": copy.deepcopy(artifact) if isinstance(artifact, Mapping) else None,
                    "request_exchange_sha256": side_result.get("request_exchange_sha256"),
                    "mcp_stdin": mcp_exchange,
                }
            )
    for side in SIDE_NAMES:
        evidence[side].sort(key=lambda item: (str(item.get("scope")), str(item.get("action_id"))))
    return {
        **evidence,
        "combined_sha256": json_digest(evidence),
        "complete": bool(evidence["original"]) and bool(evidence["product"]),
    }


def _first_causal_stage(result: Mapping[str, Any]) -> str | None:
    """Choose the earliest observed failure stage for a ledger row."""

    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    terminal_failure_statuses = {
        "error",
        "unknown",
        "invalid",
        "unsupported",
        "cancelled",
        "partial",
        "censored",
        "effect_unknown",
        "blocked",
    }

    def side_stage(side_result: Any) -> str | None:
        if not isinstance(side_result, Mapping):
            return None
        process = side_result.get("process", {})
        process_record = process.get("process", {}) if isinstance(process, Mapping) else {}
        if isinstance(process_record, Mapping) and process_record.get("failure_class") in ("spawn_failure", "process_spawn_failure"):
            return "spawn"
        if isinstance(process_record, Mapping) and process_record.get("failure_class") in ("process_failure", "transport_failure"):
            return "transport"
        process_status = process.get("status") if isinstance(process, Mapping) else None
        cleanup = side_result.get("cleanup")
        if isinstance(cleanup, Mapping) and cleanup.get("status") not in (None, "completed", "not_started"):
            return "cleanup"
        # A process that actually reports an unknown/error transport outcome
        # is a transport failure.  A completed process with a missing
        # authority, effect, or composition assertion is diagnosed at the
        # later evidence stage instead of being mislabeled as transport.
        if process_status in ("unknown", "error") and side_result.get("status") in terminal_failure_statuses:
            return "transport"
        return None

    # Setup and spawn are observed before any action/composition symptom.  A
    # top-level runner exception must not hide a concrete setup failure.
    setup = result.get("setup", {})
    if isinstance(setup, Mapping):
        for rows in setup.values():
            if isinstance(rows, Mapping):
                rows = (rows,)
            if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes)):
                for row in rows:
                    setup_result = row.get("result") if isinstance(row, Mapping) else None
                    if isinstance(setup_result, Mapping) and setup_result.get("status") in terminal_failure_statuses:
                        setup_process = setup_result.get("process")
                        has_process_evidence = bool(
                            isinstance(setup_process, Mapping)
                            and (
                                setup_process.get("status") in ("completed", "error", "unknown", "cancelled", "partial")
                                or isinstance(setup_process.get("process"), Mapping)
                            )
                        )
                        if not has_process_evidence:
                            return "setup"
                    stage = side_stage(setup_result)
                    if stage == "spawn":
                        return "spawn"
                    if stage == "transport":
                        return "setup"
                    if stage == "cleanup":
                        return "cleanup"
    # Composition proofs execute before the ordinary action list.  Preserve
    # that causal ordering so a later transport/effect symptom cannot mask a
    # missing or failed production boundary selection.
    composition = result.get("composition_evidence", {})
    if isinstance(composition, Mapping):
        composition_failed = False
        for value in composition.values():
            if not isinstance(value, Mapping):
                continue
            checks = value.get("checks", ())
            if isinstance(checks, Mapping):
                checks = (checks,)
            if isinstance(checks, Sequence) and not isinstance(checks, (str, bytes)):
                for check in checks:
                    check_result = check.get("result") if isinstance(check, Mapping) else None
                    stage = side_stage(check_result)
                    if stage in ("spawn", "transport", "cleanup"):
                        return stage
            if (
                value.get("status") not in (None, "pass", "completed")
                or value.get("composition_reached") is False
            ):
                composition_failed = True
        if composition_failed:
            return "composition"
    actions = result.get("actions", ())
    _require(isinstance(actions, (list, tuple)), "case result actions must be a list", RunnerError)
    for action in actions:
        if not isinstance(action, Mapping):
            continue
        for side in SIDE_NAMES:
            side_result = action.get(side)
            stage = side_stage(side_result)
            if stage == "spawn":
                return "spawn"
            if stage == "transport":
                return "transport"
            if stage == "cleanup":
                return "cleanup"
    daemon_cleanup = result.get("daemon_cleanup", {})
    cleanup_values = daemon_cleanup.values() if isinstance(daemon_cleanup, Mapping) else ()
    if result.get("cleanup_failure") or any(
        isinstance(value, Mapping) and value.get("status") not in (None, "pass", "completed")
        for value in cleanup_values
    ):
        return "cleanup"
    if result.get("status") in ("effect_unknown", "blocked"):
        return "effect"
    failure_class = result.get("failure_class")
    if failure_class in ("spawn_failure", "process_spawn_failure"):
        return "spawn"
    if failure_class in ("setup_failure", "setup_error"):
        return "setup"
    if failure_class in ("transport_failure", "process_failure"):
        return "transport"
    if failure_class in ("composition_failure", "composition_error"):
        return "composition"
    if failure_class in ("cleanup_failure", "cleanup_error"):
        return "cleanup"
    if failure_class in ("runner_failure", "ledger_materialization_failure"):
        return "runner"
    reason = result.get("reason")
    if isinstance(reason, str):
        reason_lower = reason.lower()
        if "spawn" in reason_lower or "could not be spawned" in reason_lower:
            return "spawn"
        if "transport" in reason_lower or "process" in reason_lower:
            return "transport"
        if "cleanup" in reason_lower or "daemon did not settle" in reason_lower:
            return "cleanup"
        if "composition" in reason_lower:
            return "composition"
        if "runner error" in reason_lower or reason_lower.startswith("ledger materialization failed:"):
            return "runner"
    if result.get("status") not in (None, "pass"):
        return "comparison"
    return None


def _effect_evidence(
    result: Mapping[str, Any],
    case: Mapping[str, Any],
    contract: Mapping[str, Any],
    checkpoint_state: Mapping[str, Any],
    reopen_state: Mapping[str, Any],
    no_effect_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(isinstance(case, Mapping), "case must be an object", RunnerError)
    _require(isinstance(contract, Mapping), "case contract must be an object", RunnerError)
    _require(isinstance(checkpoint_state, Mapping), "checkpoint evidence must be an object", RunnerError)
    _require(isinstance(reopen_state, Mapping), "reopen evidence must be an object", RunnerError)
    _require(isinstance(no_effect_evidence, Mapping), "no-effect evidence must be an object", RunnerError)
    raw_actions = result.get("actions", ())
    _require(isinstance(raw_actions, (list, tuple)), "case result actions must be a list", RunnerError)
    _require(all(isinstance(item, Mapping) for item in raw_actions), "case result actions must contain only objects", RunnerError)
    # Setup mutations are part of the attempt's effect boundary.  Keep them
    # as separate synthetic actions so a later equal search response cannot
    # hide a seed whose receipt/state was never observed.  Pair the two sides
    # by their declared setup id and retain each side's exact input/result.
    effect_actions: list[Mapping[str, Any]] = list(raw_actions)
    routes = _route_map(contract)
    setup = result.get("setup", {})
    if isinstance(setup, Mapping):
        setup_by_id: dict[str, dict[str, Any]] = {}
        for side in SIDE_NAMES:
            rows = setup.get(side, ())
            if isinstance(rows, Mapping):
                rows = (rows,)
            if not isinstance(rows, (list, tuple)):
                continue
            for index, row in enumerate(rows):
                if not isinstance(row, Mapping):
                    continue
                side_result = row.get("result")
                if not isinstance(side_result, Mapping):
                    continue
                action_id = row.get("id", f"setup-{index}")
                if not isinstance(action_id, str) or not action_id.strip():
                    action_id = f"setup-{index}"
                synthetic = setup_by_id.setdefault(
                    action_id,
                    {
                        "action_id": f"setup/{action_id}",
                        "route": side_result.get("route"),
                        "input": copy.deepcopy(side_result.get("input", {})),
                    },
                )
                synthetic[side] = side_result
                # A side-specific declaration must be visible to the effect
                # evaluator.  If the inputs differ, retain both for the
                # binding checks below rather than selecting one silently.
                input_key = f"{side}_input"
                synthetic[input_key] = copy.deepcopy(side_result.get("input", {}))
                if synthetic.get("route") != side_result.get("route"):
                    synthetic["route_mismatch"] = True
        for synthetic in setup_by_id.values():
            route_obj = routes.get(synthetic.get("route")) if isinstance(synthetic.get("route"), str) else None
            policy = _route_effect_policy(route_obj)
            input_action = synthetic.get("input", {})
            comparison = input_action.get("comparison", {}) if isinstance(input_action, Mapping) else {}
            if policy in ("required", "retrieval") or (
                isinstance(comparison, Mapping)
                and any(comparison.get(field) for field in _EFFECT_POINTER_FIELDS)
            ):
                effect_actions.insert(0, synthetic)
    per_action: list[dict[str, Any]] = []
    for action_index, action in enumerate(effect_actions):
        action_id = action.get("action_id", action.get("id", f"action-{action_index}"))
        route_id = action.get("route", action.get("operation"))
        route_obj = routes.get(route_id) if isinstance(route_id, str) else None
        policy = _route_effect_policy(route_obj)
        if policy is None and isinstance(action.get("operation_kind"), str):
            kind = action.get("operation_kind")
            policy = "required" if kind in ("mutation", "host_extension") else "none" if kind in ("read", "semantic_read", "temporal_read") else "unknown"
        input_action = action.get("input", action)
        comparison = input_action.get("comparison", {}) if isinstance(input_action, Mapping) else {}
        if not isinstance(comparison, Mapping):
            comparison = {}
        pointers = _effect_pointers(comparison)
        side_values: dict[str, dict[str, Any]] = {side: {} for side in SIDE_NAMES}
        side_missing: dict[str, dict[str, list[str]]] = {
            side: {field: [] for field in ("effect_json_pointers", "receipt_json_pointers", "state_json_pointers", "no_effect_json_pointers")}
            for side in SIDE_NAMES
        }
        side_valid: dict[str, bool] = {side: True for side in SIDE_NAMES}
        side_lifecycle: dict[str, bool] = {side: False for side in SIDE_NAMES}
        side_no_effect: dict[str, bool] = {side: False for side in SIDE_NAMES}
        for side in SIDE_NAMES:
            side_result = action.get(side)
            if not isinstance(side_result, Mapping):
                side_valid[side] = False
                continue
            if action.get("route_mismatch") is True:
                side_valid[side] = False
            response = side_result.get("response")
            side_valid[side] = side_valid[side] and (
                side_result.get("status") in ("completed", "pass")
                and response is not None
                and _authority_evidence_observed(side_result)
            )
            checks = action.get("checkpoints", {})
            if isinstance(checks, Mapping):
                # A mutation's durability must be observed after the action
                # (or after a verified reopen).  A pre-action snapshot alone
                # cannot prove that the write survived.
                for phase in ("after", "reopened"):
                    entries = checks.get(phase, ())
                    if isinstance(entries, Mapping):
                        entries = (entries,)
                    if isinstance(entries, (list, tuple)):
                        for entry in entries:
                            candidate = entry.get(side) if isinstance(entry, Mapping) else None
                            if isinstance(candidate, Mapping):
                                side_lifecycle[side] = side_lifecycle[side] or (
                                    candidate.get("status") in ("completed", "pass")
                                    and candidate.get("response") is not None
                                    and _authority_evidence_observed(candidate)
                                )
            side_lifecycle[side] = side_lifecycle[side] or (
                isinstance(reopen_state.get(side), Mapping)
                and reopen_state[side].get("observed") is True
            )
            durable_required = policy == "required" or (
                policy == "retrieval" and bool(pointers["effect_json_pointers"])
            )
            durable = side_result.get("runner_observed_durable")
            if durable_required:
                # A child response can be copied into effect/receipt/state
                # pointers.  Require the runner-owned after and reopen
                # snapshots before reading any mutation value from it.
                side_lifecycle[side] = side_lifecycle[side] and (
                    isinstance(durable, Mapping)
                    and durable.get("observed") is True
                    and durable.get("source") == "runner_owned_daemon_action_receipt"
                    and isinstance(durable.get("after"), Mapping)
                    and durable["after"].get("observed") is True
                    and isinstance(durable.get("reopen"), Mapping)
                    and durable["reopen"].get("observed") is True
                )
            for field, field_pointers in pointers.items():
                for pointer in field_pointers:
                    if durable_required and field != "no_effect_json_pointers":
                        phase = "reopen" if field == "state_json_pointers" else "after"
                        value = _runner_durable_value(
                            side_result,
                            field=field,
                            pointer=pointer,
                            phase=phase,
                        )
                    else:
                        value = json_pointer(response, pointer)
                    if not _typed_effect_observable(
                        field,
                        value,
                        strict=policy == "required" and field != "no_effect_json_pointers",
                    ):
                        side_missing[side][field].append(pointer)
                    else:
                        side_values[side].setdefault(field, {})[pointer] = copy.deepcopy(value)
            side_no_effect[side] = (
                bool(pointers["no_effect_json_pointers"])
                and not side_missing[side]["no_effect_json_pointers"]
                and side_valid[side]
            )
        if policy == "required":
            complete = (
                all(side_valid.values())
                and all(side_lifecycle.values())
                and all(not side_missing[side][field] for side in SIDE_NAMES for field in ("effect_json_pointers", "receipt_json_pointers", "state_json_pointers"))
                and all(pointers[field] for field in ("effect_json_pointers", "receipt_json_pointers", "state_json_pointers"))
            )
            # A mutation must expose three independently typed durable
            # boundaries.  Repeating one digest/map at every pointer is a
            # self-asserted summary, not evidence that the effect, receipt,
            # and resulting state were separately observed.
            if complete:
                for side in SIDE_NAMES:
                    values_by_kind = [
                        side_values[side].get(field)
                        for field in ("effect_json_pointers", "receipt_json_pointers", "state_json_pointers")
                    ]
                    if len({json_digest(value) for value in values_by_kind}) != 3:
                        complete = False
                        side_missing[side]["state_json_pointers"].append("<distinct-durable-semantics>")
        elif policy == "retrieval":
            effect_complete = (
                all(side_valid.values())
                and all(side_lifecycle.values())
                and bool(pointers["effect_json_pointers"])
                and all(not side_missing[side]["effect_json_pointers"] for side in SIDE_NAMES)
            )
            no_effect_complete = all(side_no_effect.values())
            complete = effect_complete or no_effect_complete
        elif policy == "none":
            complete = all(side_no_effect.values())
        else:
            complete = False
        per_action.append(
            {
                "action_id": action_id,
                "route": route_id,
                "policy": policy or "unknown",
                "observed": {side: side_valid[side] for side in SIDE_NAMES},
                "lifecycle_observed": side_lifecycle,
                "no_effect_observed": side_no_effect,
                "values": side_values,
                "missing": side_missing,
                "complete": complete,
            }
        )
    if not per_action:
        return {
            "policy": "unknown",
            "observed": {side: False for side in SIDE_NAMES},
            "complete": False,
            "reason": "case has no executable actions with effect policy",
            "per_action": [],
            "effect_digest": None,
            "receipt_digest": None,
            "state_digest": None,
        }
    observed = {
        side: all(item["observed"].get(side) is True for item in per_action)
        for side in SIDE_NAMES
    }
    complete = all(item["complete"] is True for item in per_action)
    reason = (
        "every action has its declared effect/no-effect evidence"
        if complete
        else "one or more actions lack typed effect, receipt, state, checkpoint, or no-effect evidence"
    )
    effect_digests = {
        side: _side_operation_digest(result, side, ("effect_json_pointers",))
        for side in SIDE_NAMES
    }
    receipt_digests = {
        side: _side_operation_digest(result, side, ("receipt_json_pointers",))
        for side in SIDE_NAMES
    }
    state_digests = {
        side: _side_operation_digest(result, side, ("state_json_pointers",))
        for side in SIDE_NAMES
    }
    policies = {str(item["policy"]) for item in per_action}
    return {
        "policy": next(iter(policies)) if len(policies) == 1 else "per_action",
        "observed": observed,
        "complete": complete,
        "reason": reason,
        "per_action": per_action,
        "effect_digest": effect_digests,
        "receipt_digest": receipt_digests,
        "state_digest": state_digests,
    }


def _build_attempt_ledger(
    *,
    result: Mapping[str, Any],
    case: Mapping[str, Any],
    contract: Mapping[str, Any],
    original_binary: Path,
    product_binary: Path,
    source_roots: Mapping[str, Path] | None,
    attempt_id: str,
    attempt_index: int,
    trusted_ncm_attestation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Materialize one complete append-only readiness attempt row.

    Values that a process did not expose remain ``None`` or a typed evidence
    object.  The row is still complete in shape, so an absent receipt/state
    cannot be mistaken for a successful mutation.
    """

    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(isinstance(case, Mapping), "case must be an object", RunnerError)
    _require(isinstance(contract, Mapping), "case contract must be an object", RunnerError)
    original_binary_ref = verify_binary(original_binary, "original")
    product_binary_ref = verify_binary(product_binary, "product")
    roots = result.get("roots", {})
    roots = roots if isinstance(roots, Mapping) else {}
    profile_paths = {
        side: copy.deepcopy(roots.get(side, {}))
        for side in SIDE_NAMES
    }
    process_identity = result.get("daemon_runtime", {})
    process_identity = process_identity if isinstance(process_identity, Mapping) else {}
    raw_actions = result.get("actions", ())
    _require(
        isinstance(raw_actions, (list, tuple)),
        "case result actions must be a list",
        RunnerError,
    )
    _require(
        all(isinstance(action, Mapping) for action in raw_actions),
        "case result actions must contain only objects",
        RunnerError,
    )
    actions = list(raw_actions)
    commands: dict[str, list[Any]] = {side: [] for side in SIDE_NAMES}
    for action in actions:
        for side in SIDE_NAMES:
            side_result = action.get(side)
            process = side_result.get("process", {}) if isinstance(side_result, Mapping) else {}
            process_record = process.get("process", {}) if isinstance(process, Mapping) else {}
            argv = process_record.get("argv") if isinstance(process_record, Mapping) else None
            if isinstance(argv, list):
                commands[side].append(copy.deepcopy(argv))
    attestation_bindings = result.get("build_attestations")
    attestation_bindings = attestation_bindings if isinstance(attestation_bindings, Mapping) else {}
    binary_provenance = result.get("binary_provenance")
    binary_provenance = binary_provenance if isinstance(binary_provenance, Mapping) else {}
    source_bindings = result.get("source_bindings")
    source_bindings = source_bindings if isinstance(source_bindings, Mapping) else {}
    provenance_errors: list[str] = []
    for side, binary_ref in (("original", original_binary_ref), ("product", product_binary_ref)):
        attestation = attestation_bindings.get(side)
        provenance = binary_provenance.get(side)
        source_evidence = source_bindings.get(side)
        if not isinstance(attestation, Mapping):
            provenance_errors.append(f"{side} build attestation is missing")
            continue
        if attestation.get("binary_sha256") != binary_ref.get("sha256"):
            provenance_errors.append(f"{side} attestation binary hash does not match execution binary")
        if not isinstance(provenance, Mapping) or provenance.get("sha256") != binary_ref.get("sha256") or provenance.get("immutable") is not True:
            provenance_errors.append(f"{side} executable provenance is not an immutable runner copy")
        if not isinstance(source_evidence, Mapping):
            provenance_errors.append(f"{side} source binding is missing")
            continue
        source_revision = source_evidence.get("revision")
        if not isinstance(source_revision, str) or re.fullmatch(r"[0-9a-f]{40}", source_revision.lower()) is None:
            provenance_errors.append(f"{side} source binding revision is unknown")
        if source_revision != attestation.get("source_revision"):
            provenance_errors.append(f"{side} source binding revision does not match attestation")
        source_path = source_evidence.get("path")
        if not isinstance(source_path, str) or not source_path.strip() or source_path != attestation.get("source_root"):
            provenance_errors.append(f"{side} source binding path does not match attestation")
    source_sha = {
        "reference": (
            source_bindings.get("original", {}).get("revision")
            if isinstance(source_bindings.get("original"), Mapping)
            else REFERENCE_REVISION
        ) or "unknown",
        "candidate": (
            source_bindings.get("product", {}).get("revision")
            if isinstance(source_bindings.get("product"), Mapping)
            else result.get("product_source_revision")
        ) or "unknown",
    }
    scope_values = {
        side: {
            key: profile_paths[side].get(key)
            for key in (
                "source_root",
                "binary_root",
                "project_root",
                "profile_root",
                "state_root",
                "store_root",
                "process_root",
                "socket_root",
                "artifact_root",
            )
        }
        for side in SIDE_NAMES
    }
    mutation_fields = ("effect_json_pointers", "receipt_json_pointers", "state_json_pointers")
    reopen = result.get("lifecycle", {})
    reopen_value = reopen.get("reopen_comparison") if isinstance(reopen, Mapping) else None
    checkpoint_state = {
        side: _side_checkpoint_state(result, side)
        for side in SIDE_NAMES
    }
    operation_reachability = {
        side: _side_operation_reachability(result, side)
        for side in SIDE_NAMES
    }
    reopen_state = {
        side: _side_reopen_state(result, side)
        for side in SIDE_NAMES
    }
    no_effect_evidence = {
        side: _side_no_effect_evidence(result, side)
        for side in SIDE_NAMES
    }
    track_identity = _track_identity_evidence(
        result,
        case,
        original_binary_ref,
        product_binary_ref,
        process_identity,
        trusted_ncm_attestation,
    )
    request_evidence = _request_digest_evidence(result)
    effect_evidence = _effect_evidence(
        result,
        case,
        contract,
        checkpoint_state,
        reopen_state,
        no_effect_evidence,
    )
    authority_correlated = {
        side: bool(
            operation_reachability[side].get("observed") is True
            and operation_reachability[side].get("authority_correlated") is True
            and _observed_store_identity(result, side).get("observed") is True
            and _observed_store_identity(result, side).get("authority_correlated") is True
        )
        for side in SIDE_NAMES
    }
    outcome = result.get("status", "unknown")
    if not isinstance(outcome, str) or outcome not in OUTCOME_STATUSES:
        outcome = "unknown"
    gate_reasons: list[str] = []
    gate_reasons.extend(provenance_errors)
    if not re.fullmatch(r"[0-9a-f]{40}", str(source_sha["candidate"]).strip().lower()):
        gate_reasons.append("candidate source revision is unknown or not a full Git SHA")
    if not re.fullmatch(r"[0-9a-f]{40}", str(source_sha["reference"]).strip().lower()) or source_sha["reference"] != REFERENCE_REVISION:
        gate_reasons.append("reference source revision is not the pinned 570 checkout")
    expected_features = {
        "reference": (
            contract.get("baseline", {}).get("reference_features")
            if isinstance(contract.get("baseline", {}), Mapping)
            else None
        ),
        "candidate": (
            contract.get("baseline", {}).get("candidate_features")
            if isinstance(contract.get("baseline", {}), Mapping)
            else None
        ),
    }
    for side in SIDE_NAMES:
        attestation = attestation_bindings.get(side)
        if not isinstance(attestation, Mapping) or not isinstance(attestation.get("features"), str):
            gate_reasons.append(f"{side} build feature attestation is missing")
        elif expected_features["reference" if side == "original" else "candidate"] is not None and attestation.get("features") != expected_features["reference" if side == "original" else "candidate"]:
            gate_reasons.append(f"{side} build features do not match the contract")
    composition_reached = result.get("composition_reached", {})
    if not isinstance(composition_reached, Mapping) or not all(
        composition_reached.get(side) is True for side in SIDE_NAMES
    ):
        gate_reasons.append("composition_reached=false")
    if not request_evidence.get("complete"):
        gate_reasons.append("request digest evidence is missing or not bound to exact request bytes")
    if _case_track(case) in READINESS_TRACKS and not track_identity["complete"]:
        gate_reasons.append(
            "invalid or missing track identity evidence: "
            + ", ".join(
                sorted(
                    set(track_identity.get("missing_fields", {}))
                    | set(track_identity.get("invalid_fields", {}))
                )
            )
        )
    if not effect_evidence["complete"]:
        gate_reasons.append(effect_evidence["reason"])
    if gate_reasons and outcome == "pass":
        outcome = "blocked"
    if outcome != result.get("status") and isinstance(result, dict):
        # The result is retained by the caller after this row is materialized;
        # mutating it here keeps case-result.json and attempts.jsonl aligned.
        result["status"] = outcome
        result["reason"] = "; ".join(gate_reasons)
    route_binding = _case_matrix_binding(case)
    matrix_row = case.get("_matrix_row") if isinstance(case.get("_matrix_row"), Mapping) else {}
    oracle_kind = (
        matrix_row.get("oracle_kind")
        if isinstance(matrix_row, Mapping)
        else route_binding.get("oracle_kind")
        if isinstance(route_binding, Mapping)
        else result.get("oracle_kind")
    ) or "unresolved"
    observed_identity = track_identity.get("observed", {})
    observed_identity = observed_identity if isinstance(observed_identity, Mapping) else {}
    provider_values = observed_identity.get("provider_id", {})
    provider_values = provider_values if isinstance(provider_values, Mapping) else {}
    provider_id = provider_values.get("product") or provider_values.get("original")
    implementation_values = observed_identity.get("implementation_sha256", {})
    implementation_values = implementation_values if isinstance(implementation_values, Mapping) else {}
    model_values = observed_identity.get("model_artifact_sha256", {})
    model_values = model_values if isinstance(model_values, Mapping) else {}
    tokenizer_values = observed_identity.get("tokenizer_sha256", {})
    tokenizer_values = tokenizer_values if isinstance(tokenizer_values, Mapping) else {}
    state_generation_values = observed_identity.get("state_generation", {})
    state_generation_values = state_generation_values if isinstance(state_generation_values, Mapping) else {}
    causal_stage = _first_causal_stage(result)
    if gate_reasons and causal_stage is None:
        if any("source revision" in reason or "binary" in reason or "attestation" in reason for reason in gate_reasons):
            causal_stage = "setup"
        elif any("effect" in reason or "identity" in reason or "request digest" in reason for reason in gate_reasons):
            causal_stage = "effect"
        else:
            causal_stage = "composition"
    actual = {
        "status": result.get("status", "unknown"),
        "action_statuses": {
            str(action.get("action_id")): action.get("status") for action in actions
        },
    }
    expected = copy.deepcopy(case.get("expected", {"outcome": "pass"}))
    if not isinstance(expected, Mapping):
        expected = {"value": expected}
    row: dict[str, Any] = {
        "actual": actual,
        "attempt_id": attempt_id,
        "attempt_index": attempt_index,
        "authority_correlated": authority_correlated,
        "candidate_binary_sha256": product_binary_ref["sha256"],
        "candidate_process_identity": copy.deepcopy(process_identity.get("product")),
        "candidate_store_identity": copy.deepcopy(
            _observed_store_identity(result, "product")
        ),
        "command": commands,
        "composition_reached": copy.deepcopy(result.get("composition_reached", {side: False for side in SIDE_NAMES})),
        "effect_digest": {
            side: _side_operation_digest(result, side, mutation_fields)
            for side in SIDE_NAMES
        },
        "evidence_paths": [str(result.get("artifact_directory", ""))],
        "expected": expected,
        "failure_class": None if outcome == "pass" else result.get("reason", outcome),
        "feature_set": {
            "reference": attestation_bindings.get("original", {}).get("features") if isinstance(attestation_bindings.get("original"), Mapping) else expected_features["reference"],
            "candidate": attestation_bindings.get("product", {}).get("features") if isinstance(attestation_bindings.get("product"), Mapping) else expected_features["candidate"],
        },
        "first_causal_stage": causal_stage,
        "fixture_seed": json_digest(case.get("fixtures", {})),
        "model_sha256": None,
        "oracle_kind": oracle_kind,
        "outcome": outcome,
        "profile_paths": profile_paths,
        "protocol_version": "native-original.v1",
        "provider_id": provider_id,
        "provider_implementation_sha256": implementation_values.get("product") or implementation_values.get("original"),
        "receipt_digest": {
            side: _side_operation_digest(result, side, ("receipt_json_pointers",))
            for side in SIDE_NAMES
        },
        "reference_binary_sha256": original_binary_ref["sha256"],
        "reference_process_identity": copy.deepcopy(process_identity.get("original")),
        "reference_store_identity": copy.deepcopy(
            _observed_store_identity(result, "original")
        ),
        "reopen_or_no_effect": {
            "reopen_comparison": copy.deepcopy(reopen_value),
            "checkpoint_state": checkpoint_state,
            "operation_reachability": operation_reachability,
            "reopen_state": reopen_state,
            "no_effect_evidence": no_effect_evidence,
            "no_effect_digest": {
                side: no_effect_evidence[side]["digest"]
                for side in SIDE_NAMES
            },
        },
        "request_digest": request_evidence,
        # The enclosing finalized case-result carries the authenticated
        # digest.  A ledger row cannot carry that same value because its
        # append receipt is itself part of the case-result payload.
        "result_digest": None,
        "case_id": str(result.get("case_id", case.get("id", case.get("case_id")))),
        "row_id": str(result.get("case_id", case.get("id", case.get("case_id")))),
        "scope_digest": json_digest(scope_values),
        "source_sha": source_sha,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "state_digest": {
            side: _side_operation_digest(result, side, ("state_json_pointers",))
            for side in SIDE_NAMES
        },
        "state_generation": state_generation_values.get("product"),
        "state_schema_version": None,
        "tokenizer_sha256": tokenizer_values.get("product") or tokenizer_values.get("original"),
    }
    row["track"] = track_identity["track"]
    row["track_identity"] = track_identity
    row["effect_evidence"] = effect_evidence
    row["request_evidence"] = request_evidence
    row["roots"] = profile_paths
    binding = result.get("runner_attempt_binding")
    if not isinstance(binding, Mapping):
        binding = {
            "run_id": attempt_id.rsplit("/", 1)[0] if "/" in attempt_id else attempt_id,
            "row_id": row["row_id"],
            "case_id": row["case_id"],
            "attempt_id": attempt_id,
            "attempt_index": attempt_index,
        }
    row["runner_attempt_binding"] = copy.deepcopy(dict(binding))
    row["build_attestations"] = copy.deepcopy(dict(attestation_bindings))
    row["binary_provenance"] = copy.deepcopy(dict(binary_provenance))
    row["source_bindings"] = copy.deepcopy(dict(source_bindings))
    row["model_artifact_sha256"] = model_values.get("product") or model_values.get("original")
    row["checkpoint_state"] = checkpoint_state
    row["operation_reachability"] = operation_reachability
    row["reopen_state"] = reopen_state
    row["no_effect_evidence"] = no_effect_evidence
    try:
        _validate_ledger_bindings(row)
        row["binding_validation"] = {"status": "pass"}
    except RunnerError as error:
        row["binding_validation"] = {"status": "blocked", "reason": str(error)}
        if row.get("outcome") == "pass":
            row["outcome"] = "blocked"
            row["actual"] = {"status": "blocked", "reason": str(error)}
            row["failure_class"] = str(error)
            row["first_causal_stage"] = "setup"
            if isinstance(result, dict):
                result["status"] = "blocked"
                result["reason"] = str(error)
    missing = sorted(set(LEDGER_REQUIRED_FIELDS) - row.keys())
    _require(not missing, "attempt ledger row is missing required fields: " + ", ".join(missing), RunnerError)
    return row


def _minimal_attempt_ledger(
    *,
    result: Mapping[str, Any],
    case: Mapping[str, Any],
    attempt_id: str,
    attempt_index: int,
    reason: str,
) -> dict[str, Any]:
    """Return a total, typed row when even normal fallback extraction failed."""

    outcome = result.get("status") if isinstance(result, Mapping) else None
    if not isinstance(outcome, str) or outcome not in OUTCOME_STATUSES or outcome == "pass":
        outcome = "unknown" if outcome != "pass" else "blocked"
    case_id = case.get("id", case.get("case_id")) if isinstance(case, Mapping) else None
    if not isinstance(case_id, str) or not case_id.strip():
        case_id = "unknown"
    artifact_directory = result.get("artifact_directory") if isinstance(result, Mapping) else None
    evidence_paths = [artifact_directory] if isinstance(artifact_directory, str) and artifact_directory else []
    candidate_revision = result.get("product_source_revision") if isinstance(result, Mapping) else None
    if not isinstance(candidate_revision, str) or re.fullmatch(r"[0-9a-f]{40}", candidate_revision.lower()) is None:
        candidate_revision = "unknown"
    try:
        fixture_seed = json_digest(case.get("fixtures", {})) if isinstance(case, Mapping) else None
    except RunnerError:
        fixture_seed = None
    request_evidence = {
        "original": [],
        "product": [],
        "combined_sha256": None,
        "complete": False,
    }
    empty_side = {side: {"observed": False, "observations": []} for side in SIDE_NAMES}
    row: dict[str, Any] = {field: None for field in LEDGER_REQUIRED_FIELDS}
    row.update(
        {
            "actual": {"status": outcome, "reason": reason},
            "attempt_id": attempt_id,
            "attempt_index": attempt_index,
            "authority_correlated": {side: False for side in SIDE_NAMES},
            "composition_reached": {side: False for side in SIDE_NAMES},
            "command": {},
            "evidence_paths": evidence_paths,
            "expected": {"outcome": "pass"},
            "failure_class": reason,
            "first_causal_stage": "runner",
            "fixture_seed": fixture_seed,
            "oracle_kind": "unresolved",
            "outcome": outcome,
            "profile_paths": {side: {} for side in SIDE_NAMES},
            "protocol_version": "native-original.v1",
            "reopen_or_no_effect": {
                "status": outcome,
                "reason": reason,
                "checkpoint_state": copy.deepcopy(empty_side),
                "operation_reachability": copy.deepcopy(empty_side),
                "reopen_state": copy.deepcopy(empty_side),
                "no_effect_evidence": copy.deepcopy(empty_side),
            },
            "request_digest": request_evidence,
            "result_digest": None,
            "case_id": case_id,
            "row_id": case_id,
            "scope_digest": None,
            "source_sha": {"reference": REFERENCE_REVISION, "candidate": candidate_revision},
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state_digest": {side: None for side in SIDE_NAMES},
        }
    )
    row["checkpoint_state"] = copy.deepcopy(empty_side)
    row["operation_reachability"] = copy.deepcopy(empty_side)
    row["reopen_state"] = copy.deepcopy(empty_side)
    row["no_effect_evidence"] = copy.deepcopy(empty_side)
    row["track"] = _case_track(case) if isinstance(case, Mapping) else "comparison"
    row["track_identity"] = {
        "track": row["track"],
        "required_fields": [],
        "observed": {},
        "missing_fields": {},
        "complete": False,
    }
    row["effect_evidence"] = {
        "policy": "unknown",
        "observed": {side: False for side in SIDE_NAMES},
        "complete": False,
        "reason": reason,
        "effect_digest": None,
        "receipt_digest": None,
        "state_digest": None,
    }
    row["request_evidence"] = request_evidence
    row["roots"] = {side: {} for side in SIDE_NAMES}
    binding = result.get("runner_attempt_binding") if isinstance(result, Mapping) else None
    if not isinstance(binding, Mapping):
        binding = {
            "run_id": attempt_id.rsplit("/", 1)[0] if "/" in attempt_id else attempt_id,
            "row_id": row["row_id"],
            "case_id": row["case_id"],
            "attempt_id": attempt_id,
            "attempt_index": attempt_index,
        }
    row["runner_attempt_binding"] = copy.deepcopy(dict(binding))
    return row


def _fallback_attempt_ledger(
    *,
    result: Mapping[str, Any],
    case: Mapping[str, Any],
    contract: Mapping[str, Any],
    attempt_id: str,
    attempt_index: int,
    source_roots: Mapping[str, Path] | None,
) -> dict[str, Any]:
    """Return a typed ledger row when binary/evidence materialization fails."""

    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(isinstance(case, Mapping), "case must be an object", RunnerError)
    _require(isinstance(contract, Mapping), "case contract must be an object", RunnerError)
    status = result.get("status")
    outcome = status if isinstance(status, str) and status in OUTCOME_STATUSES else "unknown"
    composition = result.get("composition_reached", {})
    if outcome == "pass" and (
        not isinstance(composition, Mapping)
        or not all(composition.get(side) is True for side in SIDE_NAMES)
    ):
        outcome = "blocked"
    artifact_directory = result.get("artifact_directory")
    evidence_paths = [str(artifact_directory)] if artifact_directory else []
    roots = result.get("roots", {})
    roots = roots if isinstance(roots, Mapping) else {}
    profile_paths = {
        side: copy.deepcopy(roots.get(side, {}))
        for side in SIDE_NAMES
    }
    source_sha = {
        "reference": REFERENCE_REVISION,
        "candidate": result.get("product_source_revision") or "unknown",
    }
    if outcome == "pass" and not re.fullmatch(
        r"[0-9a-f]{40}", str(source_sha["candidate"]).strip().lower()
    ):
        outcome = "blocked"
        if isinstance(result, dict):
            result["status"] = outcome
            result["reason"] = "candidate source revision is unknown or not a full Git SHA"
    request_evidence = _request_digest_evidence(result)
    if outcome == "pass" and not request_evidence.get("complete"):
        outcome = "blocked"
        if isinstance(result, dict):
            result["status"] = outcome
            result["reason"] = "request digest evidence is missing or not bound to exact request bytes"
    if outcome == "pass" and _case_track(case) in READINESS_TRACKS:
        outcome = "blocked"
        if isinstance(result, dict):
            result["status"] = outcome
            result["reason"] = "track identity evidence is missing or incomplete"
    if outcome == "pass":
        # This function is reached only after the full evidence builder failed;
        # it must never preserve a successful status without mutation/read
        # effect evidence and checkpoint/reopen/no-effect proof.
        outcome = "blocked"
        if isinstance(result, dict):
            result["status"] = outcome
            result["reason"] = "effect/checkpoint/reopen evidence could not be materialized"
    row: dict[str, Any] = {field: None for field in LEDGER_REQUIRED_FIELDS}
    fallback_reason = result.get("reason")
    if outcome == "blocked" and isinstance(fallback_reason, str) and (
        "source revision" in fallback_reason
        or "attestation" in fallback_reason
        or "binary" in fallback_reason
    ):
        fallback_stage = "setup"
    elif outcome == "blocked" and isinstance(fallback_reason, str) and (
        "request digest" in fallback_reason
        or "effect evidence" in fallback_reason
    ):
        fallback_stage = "effect"
    else:
        fallback_stage = _first_causal_stage(result) if outcome != "pass" else None
    row.update(
        {
            "actual": {"status": outcome, "reason": result.get("reason")},
            "attempt_id": attempt_id,
            "attempt_index": attempt_index,
            "authority_correlated": {
                side: bool(
                    isinstance(result.get("operation_reachability"), Mapping)
                    and isinstance(result.get("operation_reachability", {}).get(side), Mapping)
                    and result.get("operation_reachability", {}).get(side, {}).get("authority_correlated") is True
                )
                for side in SIDE_NAMES
            },
            "composition_reached": copy.deepcopy(
                result.get("composition_reached", {side: False for side in SIDE_NAMES})
            ),
            "command": {},
            "evidence_paths": evidence_paths,
            "expected": copy.deepcopy(case.get("expected", {"outcome": "pass"})),
            "failure_class": result.get("reason") or outcome,
            "feature_set": {
                "reference": (
                    contract.get("baseline", {}).get("reference_features")
                    if isinstance(contract.get("baseline", {}), Mapping)
                    else None
                ),
                "candidate": (
                    contract.get("baseline", {}).get("candidate_features")
                    if isinstance(contract.get("baseline", {}), Mapping)
                    else None
                ),
            },
            "first_causal_stage": fallback_stage,
            "fixture_seed": json_digest(case.get("fixtures", {})),
            "oracle_kind": (
                case.get("_matrix_row", {}).get("oracle_kind")
                if isinstance(case.get("_matrix_row"), Mapping)
                else (_case_matrix_binding(case) or {}).get("oracle_kind")
                if isinstance(_case_matrix_binding(case), Mapping)
                else "unresolved"
            ),
            "outcome": outcome,
            "profile_paths": profile_paths,
            "protocol_version": "native-original.v1",
            "provider_id": None,
            "reopen_or_no_effect": {
                "status": outcome,
                "reason": "attempt failed before complete operation evidence was materialized",
                "checkpoint_state": {},
                "reopen_state": {},
                "no_effect_evidence": {},
            },
            "request_digest": request_evidence,
            "result_digest": None,
            "case_id": str(result.get("case_id", case.get("id", case.get("case_id")))),
            "row_id": str(result.get("case_id", case.get("id", case.get("case_id")))),
            "scope_digest": json_digest(
                {
                    side: {
                        "source_root": str((source_roots or {}).get(side))
                        if (source_roots or {}).get(side) is not None
                        else None,
                        **{
                            key: profile_paths[side].get(key)
                            for key in (
                                "binary_root",
                                "project_root",
                                "profile_root",
                                "state_root",
                                "store_root",
                                "process_root",
                                "socket_root",
                                "artifact_root",
                            )
                        },
                    }
                    for side in SIDE_NAMES
                }
            ),
            "source_sha": source_sha,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    )
    row["checkpoint_state"] = {side: {"observed": False, "observations": []} for side in SIDE_NAMES}
    row["operation_reachability"] = {side: {"observed": False, "observations": []} for side in SIDE_NAMES}
    row["reopen_state"] = {side: {"observed": False, "status": "unknown"} for side in SIDE_NAMES}
    row["no_effect_evidence"] = {side: {"observed": False, "observations": []} for side in SIDE_NAMES}
    row["track"] = _case_track(case)
    row["track_identity"] = {"track": _case_track(case), "complete": False, "missing_fields": {}}
    row["effect_evidence"] = {"complete": False, "reason": "attempt failed before effect evidence"}
    row["request_evidence"] = request_evidence
    row["roots"] = profile_paths
    binding = result.get("runner_attempt_binding")
    if not isinstance(binding, Mapping):
        binding = {
            "run_id": attempt_id.rsplit("/", 1)[0] if "/" in attempt_id else attempt_id,
            "row_id": row["row_id"],
            "case_id": row["case_id"],
            "attempt_id": attempt_id,
            "attempt_index": attempt_index,
        }
    row["runner_attempt_binding"] = copy.deepcopy(dict(binding))
    return row


def _validate_ledger_bindings(row: Mapping[str, Any]) -> None:
    """Recompute provenance, feature, and scope bindings in an attempt row."""

    _require(isinstance(row, Mapping), "attempt ledger row must be an object", RunnerError)
    missing_fields = sorted(set(LEDGER_REQUIRED_FIELDS) - row.keys())
    _require(
        not missing_fields,
        "attempt ledger row is missing required fields: " + ", ".join(missing_fields),
        RunnerError,
    )
    row_id = row.get("row_id")
    case_id = row.get("case_id")
    _require(
        isinstance(row_id, str)
        and bool(row_id.strip())
        and isinstance(case_id, str)
        and case_id == row_id
        and ("id" not in row or row.get("id") == row_id),
        "attempt ledger case_id/row_id identity is missing or conflicting",
        RunnerError,
    )
    attempt_binding = row.get("runner_attempt_binding")
    _require(
        isinstance(attempt_binding, Mapping)
        and isinstance(attempt_binding.get("run_id"), str)
        and bool(attempt_binding.get("run_id", "").strip())
        and attempt_binding.get("row_id") == row_id
        and attempt_binding.get("case_id") == case_id
        and attempt_binding.get("attempt_id") == row.get("attempt_id")
        and attempt_binding.get("attempt_index") == row.get("attempt_index"),
        "attempt ledger runner-owned attempt binding is missing or inconsistent",
        RunnerError,
    )
    attestations = row.get("build_attestations")
    provenance = row.get("binary_provenance")
    sources = row.get("source_bindings")
    _require(isinstance(attestations, Mapping), "attempt ledger build attestations are missing", RunnerError)
    _require(isinstance(provenance, Mapping), "attempt ledger binary provenance is missing", RunnerError)
    _require(isinstance(sources, Mapping), "attempt ledger source bindings are missing", RunnerError)
    authority_flags = row.get("authority_correlated")
    _require(
        isinstance(authority_flags, Mapping)
        and all(side in authority_flags and isinstance(authority_flags.get(side), bool) for side in SIDE_NAMES),
        "attempt ledger authority_correlated flags are missing",
        RunnerError,
    )
    matrix_row = row.get("matrix_row")
    if matrix_row is not None:
        _require(isinstance(matrix_row, Mapping), "attempt ledger matrix_row is malformed", RunnerError)
        matrix_binding = row.get("matrix_binding")
        _require(isinstance(matrix_binding, Mapping), "attempt ledger matrix_binding is missing", RunnerError)
        expected_binding = {
            "matrix_row_id": matrix_row.get("id"),
            "track": matrix_row.get("track"),
            "profile": matrix_row.get("profile"),
            "operation": matrix_row.get("operation"),
            "oracle_kind": matrix_row.get("oracle_kind"),
            "oracle_case": matrix_row.get("oracle_case"),
            "identity": matrix_row.get("identity"),
            "identity_evidence": matrix_row.get("identity_evidence"),
            "effect_receipt_state": matrix_row.get("effect_receipt_state"),
            "required_identity_fields": sorted(
                str(key) for key in matrix_row.get("identity_evidence", {})
            ) if isinstance(matrix_row.get("identity_evidence"), Mapping) else None,
            "required_evidence_fields": sorted(
                str(key) for key in matrix_row.get("effect_receipt_state", {})
            ) if isinstance(matrix_row.get("effect_receipt_state"), Mapping) else None,
            "identity_requirements": matrix_row.get("identity_requirements", {}),
            "track_identity_requirements": matrix_row.get("track_identity_requirements", {}),
        }
        _require(
            all(value is not None for value in expected_binding.values()),
            "attempt ledger matrix_row lacks complete binding metadata",
            RunnerError,
        )
        _require(
            set(matrix_binding) == set(expected_binding)
            and all(
                _json_semantic_equal(matrix_binding.get(field), expected)
                for field, expected in expected_binding.items()
            ),
            "attempt ledger matrix binding does not match its frozen row",
            RunnerError,
        )
        _require(
            row.get("row_id") == matrix_row.get("id")
            and row.get("track") == matrix_row.get("track")
            and row.get("oracle_kind") == matrix_row.get("oracle_kind"),
            "attempt ledger row identity does not match its matrix row",
            RunnerError,
        )
    for side, binary_field, source_field in (
        ("original", "reference_binary_sha256", "reference"),
        ("product", "candidate_binary_sha256", "candidate"),
    ):
        attestation = attestations.get(side)
        binary = provenance.get(side)
        source = sources.get(side)
        _require(isinstance(attestation, Mapping), f"{side} ledger attestation is missing", RunnerError)
        _require(isinstance(binary, Mapping), f"{side} ledger binary provenance is missing", RunnerError)
        _require(isinstance(source, Mapping), f"{side} ledger source binding is missing", RunnerError)
        digest = row.get(binary_field)
        _require(
            isinstance(digest, str)
            and re.fullmatch(r"[0-9a-f]{64}", digest) is not None
            and digest == attestation.get("binary_sha256")
            and digest == binary.get("sha256")
            and binary.get("immutable") is True,
            f"{side} ledger binary/source binding is inconsistent",
            RunnerError,
        )
        revision = row.get("source_sha", {}).get(source_field) if isinstance(row.get("source_sha"), Mapping) else None
        _require(
            isinstance(revision, str)
            and re.fullmatch(r"[0-9a-f]{40}", revision) is not None
            and revision == attestation.get("source_revision")
            and revision == source.get("revision"),
            f"{side} ledger source revision binding is inconsistent",
            RunnerError,
        )
        _require(
            source.get("path") == attestation.get("source_root"),
            f"{side} ledger source root binding is inconsistent",
            RunnerError,
        )
        _require(
            source.get("git_root") == source.get("path"),
            f"{side} ledger source binding is not the exact Git top-level",
            RunnerError,
        )
        expected_scope = None
        profile_paths = row.get("profile_paths")
        if isinstance(profile_paths, Mapping) and isinstance(profile_paths.get(side), Mapping):
            expected_scope = profile_paths[side].get("source_root")
        _require(
            isinstance(expected_scope, str)
            and isinstance(source.get("path"), str)
            and Path(source["path"]).expanduser().resolve() == Path(expected_scope).expanduser().resolve(),
            f"{side} ledger source root is not the observed execution root",
            RunnerError,
        )
        _require(
            side != "original" or revision == REFERENCE_REVISION,
            "original ledger source is not pinned to the verified 570 checkout",
            RunnerError,
        )
        try:
            source_root = Path(source["path"]).expanduser().resolve()
            source_head = _git(source_root, "rev-parse", "--verify", "HEAD")
            source_top = _git(source_root, "rev-parse", "--show-toplevel")
            observed_head = source_head.stdout.strip().lower() if source_head.returncode == 0 else ""
            observed_top = (
                str(Path(source_top.stdout.strip()).expanduser().resolve())
                if source_top.returncode == 0 and source_top.stdout.strip()
                else ""
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            observed_head = ""
            observed_top = ""
        _require(
            observed_head == revision,
            f"{side} ledger source HEAD does not match its attested revision",
            RunnerError,
        )
        _require(
            observed_top == str(source_root),
            f"{side} ledger source is not the exact Git top-level",
            RunnerError,
        )
        attestation_path = attestation.get("path")
        _require(
            isinstance(attestation_path, str)
            and bool(attestation_path.strip())
            and isinstance(attestation.get("attestation_sha256"), str)
            and re.fullmatch(r"[0-9a-f]{64}", attestation["attestation_sha256"]) is not None,
            f"{side} ledger attestation digest is missing",
            RunnerError,
        )
        try:
            attestation_bytes, _ = _read_stable_file(
                Path(attestation_path).expanduser(),
                label=f"{side} ledger build attestation",
            )
            attestation_value = _strict_json_loads(
                attestation_bytes.decode("utf-8", errors="strict")
            )
        except (OSError, TypeError, ValueError, RunnerError) as error:
            raise RunnerError(f"{side} ledger attestation cannot be re-read: {error}") from error
        _require(
            isinstance(attestation_value, Mapping)
            and all(
                attestation_value.get(field) == attestation.get(field)
                for field in (
                    "format",
                    "side",
                    "features",
                    "source_root",
                    "source_revision",
                    "binary_path",
                    "binary_sha256",
                )
            ),
            f"{side} ledger build attestation content does not match its bound fields",
            RunnerError,
        )
        _require(
            bytes_digest(attestation_bytes) == attestation.get("attestation_sha256"),
            f"{side} ledger attestation digest does not match its file",
            RunnerError,
        )
        _require(
            isinstance(attestation.get("features"), str)
            and bool(attestation.get("features", "").strip())
            and isinstance(row.get("feature_set"), Mapping)
            and row["feature_set"].get("reference" if side == "original" else "candidate")
            == attestation.get("features"),
            f"{side} ledger build feature attestation is not bound to the feature_set",
            RunnerError,
        )
        binary_path = binary.get("path")
        _require(
            isinstance(binary_path, str) and bool(binary_path.strip()),
            f"{side} ledger binary provenance lacks an execution path",
            RunnerError,
        )
        profile = profile_paths.get(side) if isinstance(profile_paths, Mapping) else None
        binary_root = profile.get("binary_root") if isinstance(profile, Mapping) else None
        _require(
            isinstance(binary_root, str) and bool(binary_root.strip()),
            f"{side} ledger binary root is missing",
            RunnerError,
        )
        try:
            execution_path = Path(binary_path).expanduser().resolve()
            observed_binary = verify_binary(execution_path, side)
            binary_root_path = Path(binary_root).expanduser().resolve()
        except (BinaryError, OSError, RuntimeError, TypeError, ValueError) as error:
            raise RunnerError(f"{side} ledger execution binary cannot be re-measured: {error}") from error
        _require(
            execution_path.is_relative_to(binary_root_path)
            and observed_binary.get("sha256") == digest,
            f"{side} ledger execution binary is outside the observed immutable root",
            RunnerError,
        )
        _require(
            binary.get("input_path") == attestation.get("binary_path")
            and binary.get("input_sha256") == digest
            and binary.get("source_pre_sha256") == digest
            and binary.get("source_post_sha256") == digest,
            f"{side} ledger immutable binary provenance is incomplete",
            RunnerError,
        )
    feature_set = row.get("feature_set")
    _require(isinstance(feature_set, Mapping), "attempt ledger feature_set is missing", RunnerError)
    _require(
        feature_set.get("reference") == attestations.get("original", {}).get("features")
        and feature_set.get("candidate") == attestations.get("product", {}).get("features"),
        "attempt ledger feature_set does not match build attestations",
        RunnerError,
    )
    profile_paths = row.get("profile_paths")
    _require(isinstance(profile_paths, Mapping), "attempt ledger profile paths are missing", RunnerError)
    scope_values = {
        side: {
            key: profile_paths.get(side, {}).get(key)
            for key in (
                "source_root",
                "binary_root",
                "project_root",
                "profile_root",
                "state_root",
                "store_root",
                "process_root",
                "socket_root",
                "artifact_root",
            )
        }
        for side in SIDE_NAMES
    }
    _require(
        row.get("scope_digest") == json_digest(scope_values),
        "attempt ledger scope_digest does not match observed roots",
        RunnerError,
    )
    original_scope = scope_values["original"]
    product_scope = scope_values["product"]
    for field in ("source_root", "binary_root", "project_root", "profile_root", "state_root", "store_root", "process_root", "socket_root"):
        left = original_scope.get(field)
        right = product_scope.get(field)
        if isinstance(left, str) and isinstance(right, str) and left and right:
            _require(
                not _paths_overlap(Path(left), Path(right)),
                f"original and product {field} roots overlap in the attempt ledger",
                RunnerError,
            )
    # Store identity is an aggregate of runner-observed operation responses.
    # Recompute the relationship here so a ledger consumer cannot accept a
    # store path or authority digest copied from a child response.
    operation_reachability = row.get("operation_reachability")
    _require(
        isinstance(operation_reachability, Mapping),
        "attempt ledger operation reachability is missing",
        RunnerError,
    )
    for side in SIDE_NAMES:
        reachability = operation_reachability.get(side)
        store = row.get(
            "reference_store_identity" if side == "original" else "candidate_store_identity"
        )
        _require(
            isinstance(reachability, Mapping),
            f"{side} ledger operation reachability is malformed",
            RunnerError,
        )
        observations = reachability.get("observations")
        _require(
            isinstance(observations, list) and observations,
            f"{side} ledger operation reachability has no observations",
            RunnerError,
        )
        _require(
            reachability.get("digest") == json_digest(observations),
            f"{side} ledger operation reachability digest is inconsistent",
            RunnerError,
        )
        recomputed_observed = bool(observations) and all(
            isinstance(observation, Mapping)
            and observation.get("observed") is True
            and observation.get("authority_correlated") is True
            for observation in observations
        )
        _require(
            reachability.get("observed") is recomputed_observed
            and reachability.get("authority_correlated") is recomputed_observed,
            f"{side} ledger operation reachability flags are not derived from observations",
            RunnerError,
        )
        _require(
            isinstance(store, Mapping)
            and store.get("observed") is True
            and store.get("authority_correlated") is True
            and _usable_store_identity(store.get("value")),
            f"{side} ledger store identity is not observed",
            RunnerError,
        )
        protocol = store.get("authority_evidence")
        _require(
            isinstance(protocol, Mapping) and protocol.get("protocol_observed") is True,
            f"{side} ledger store identity lacks protocol authority evidence",
            RunnerError,
        )
        if protocol.get("source") == "runner_owned_daemon_action_receipt":
            _require(
                _action_receipt_wrapper_valid(
                    protocol,
                    side=side,
                    route_id=protocol.get("route"),
                    entrypoint=protocol.get("entrypoint"),
                    action_id=protocol.get("action_id"),
                ),
                f"{side} ledger store identity receipt is not bound to a runner-held MAC proof",
                RunnerError,
            )
            _require(
                _verified_receipt_proof(protocol),
                f"{side} ledger store identity lacks a valid daemon action receipt",
                RunnerError,
            )
        else:
            _require(
                isinstance(protocol.get("challenge"), str)
                and re.fullmatch(r"[0-9a-f]{64}", protocol.get("challenge")) is not None
                and isinstance(protocol.get("challenge_response"), str)
                and re.fullmatch(r"[0-9a-f]{64}", protocol.get("challenge_response")) is not None,
                f"{side} ledger store identity lacks runner challenge evidence",
                RunnerError,
            )
        required_authority_fields = (
            "authority_digest",
            "endpoint",
            "profile_root",
            "process_root",
            "binary_root",
            "store_root",
        )
        _require(
            all(field in protocol and protocol.get(field) not in (None, "") for field in required_authority_fields)
            and re.fullmatch(r"[0-9a-f]{64}", str(protocol.get("authority_digest"))) is not None,
            f"{side} ledger store authority evidence is incomplete",
            RunnerError,
        )
        _require(
            isinstance(store.get("digest"), str)
            and store.get("digest") == json_digest(store.get("observations", [])),
            f"{side} ledger store identity digest is inconsistent",
            RunnerError,
        )
        store_observations = store.get("observations")
        _require(
            isinstance(store_observations, list) and store_observations,
            f"{side} ledger store identity has no operation observations",
            RunnerError,
        )
        _require(
            all(isinstance(observation, Mapping) for observation in store_observations),
            f"{side} ledger store identity observations are malformed",
            RunnerError,
        )
        _require(
            _json_semantic_equal(store.get("value"), store_observations[0].get("value"))
            and isinstance(store.get("values"), list)
            and len(store.get("values")) == len(store_observations)
            and all(
                _json_semantic_equal(value, observation.get("value"))
                for value, observation in zip(store.get("values"), store_observations)
            ),
            f"{side} ledger store identity aggregate is not derived from observed values",
            RunnerError,
        )
        observation_protocols = [
            observation.get("authority_protocol_evidence")
            for observation in observations
            if isinstance(observation, Mapping)
            and isinstance(observation.get("authority_protocol_evidence"), Mapping)
        ]
        _require(
            any(
                _json_semantic_equal(store.get("authority_evidence"), protocol_value)
                for protocol_value in observation_protocols
            )
            or (
                isinstance(store_observations[0], Mapping)
                and isinstance(store_observations[0].get("protocol_evidence"), Mapping)
                and _json_semantic_equal(
                    store.get("authority_evidence"),
                    store_observations[0].get("protocol_evidence"),
                )
            ),
            f"{side} ledger store authority evidence is not derived from an operation response",
            RunnerError,
        )
        recomputed_authority = bool(
            reachability.get("observed") is True
            and reachability.get("authority_correlated") is True
            and store.get("observed") is True
            and store.get("authority_correlated") is True
        )
        _require(
            authority_flags.get(side) is recomputed_authority,
            f"{side} ledger authority_correlated flag is not derived from observed evidence",
            RunnerError,
        )
        operation_store_observations = [
            observation.get("store_identity")
            for observation in observations
            if isinstance(observation, Mapping)
        ]
        _require(
            all(isinstance(item, Mapping) for item in operation_store_observations)
            and sorted(json_digest(item) for item in operation_store_observations)
            == sorted(json_digest(item) for item in store_observations),
            f"{side} ledger store identity is not derived from operation observations",
            RunnerError,
        )
        for observation in observations:
            _require(
                isinstance(observation, Mapping)
                and observation.get("observed") is True
                and observation.get("authority_correlated") is True,
                f"{side} ledger operation reachability contains uncorrelated evidence",
                RunnerError,
            )
            independent_authority = observation.get("independent_authority_observation")
            _require(
                observation.get("independent_authority_observed") is True
                and isinstance(independent_authority, Mapping)
                and independent_authority.get("observed") is True
                and independent_authority.get("authority_digest")
                == observation.get("authority_digest"),
                f"{side} ledger operation reachability lacks independent authority observation",
                RunnerError,
            )
            observation_protocol = observation.get("authority_protocol_evidence")
            if isinstance(observation_protocol, Mapping) and observation_protocol.get("source") == "runner_owned_daemon_action_receipt":
                _require(
                    _action_receipt_wrapper_valid(
                        observation_protocol,
                        side=side,
                        route_id=observation.get("route"),
                        entrypoint=observation_protocol.get("entrypoint"),
                        action_id=observation_protocol.get("action_id"),
                    ),
                    f"{side} ledger operation reachability receipt is not bound to a runner-held MAC proof",
                    RunnerError,
                )
                _require(
                    _verified_receipt_proof(observation_protocol),
                    f"{side} ledger operation reachability lacks a valid daemon action receipt",
                    RunnerError,
                )
            else:
                _require(
                    observation.get("challenge_required") is True
                    and isinstance(observation.get("challenge"), str)
                    and re.fullmatch(r"[0-9a-f]{64}", observation.get("challenge")) is not None
                    and isinstance(observation.get("challenge_response"), str)
                    and re.fullmatch(r"[0-9a-f]{64}", observation.get("challenge_response")) is not None
                    and isinstance(observation_protocol, Mapping)
                    and observation_protocol.get("challenge") == observation.get("challenge")
                    and observation_protocol.get("challenge_response") == observation.get("challenge_response"),
                    f"{side} ledger operation reachability lacks runner challenge evidence",
                    RunnerError,
                )
            route_identity = observation.get("route_identity")
            _require(
                isinstance(route_identity, Mapping)
                and route_identity.get("observed") is True
                and route_identity.get("runner_owned") is True
                and route_identity.get("route") == observation.get("route")
                and route_identity.get("contract_digest") == json_digest(
                    {
                        "route": route_identity.get("route"),
                        "entrypoint": route_identity.get("entrypoint"),
                        "tool": route_identity.get("tool"),
                        "operation_kind": route_identity.get("operation_kind"),
                        "classification": route_identity.get("classification"),
                        "availability": route_identity.get("availability"),
                    }
                ),
                f"{side} ledger route identity is not runner-owned and contract-bound",
                RunnerError,
            )
            identity_binding = observation.get("runner_identity_binding")
            expected_binary_digest = row.get(
                "reference_binary_sha256" if side == "original" else "candidate_binary_sha256"
            )
            _require(
                isinstance(identity_binding, Mapping)
                and identity_binding.get("observed") is True
                and identity_binding.get("source") == "runner_identity_binding"
                and identity_binding.get("side") == side
                and identity_binding.get("binary_sha256") == expected_binary_digest
                and identity_binding.get("route_digest") == route_identity.get("contract_digest")
                and identity_binding.get("authority_digest") == observation.get("authority_digest")
                and isinstance(identity_binding.get("authority_observation_digest"), str)
                and isinstance(observation.get("independent_authority_observation"), Mapping)
                and identity_binding.get("authority_observation_digest")
                == json_digest(observation.get("independent_authority_observation"))
                and isinstance(identity_binding.get("protocol_evidence_digest"), str)
                and isinstance(observation.get("authority_protocol_evidence"), Mapping)
                and identity_binding.get("protocol_evidence_digest")
                == json_digest(observation.get("authority_protocol_evidence")),
                f"{side} ledger identity binding is not independently runner-observed",
                RunnerError,
            )
            if route_identity.get("entrypoint") == "mcp_stdio":
                _require(
                    isinstance(route_identity.get("tool"), str)
                    and route_identity.get("tool") == route_identity.get("published_tool"),
                    f"{side} ledger MCP route identity does not match its published tool",
                    RunnerError,
                )
            binding = observation.get("authority_binding")
            _require(
                isinstance(binding, Mapping)
                and _json_semantic_equal(
                    {key: protocol.get(key) for key in required_authority_fields},
                    {key: binding.get(key) for key in required_authority_fields},
                ),
                f"{side} ledger authority binding does not match store protocol evidence",
                RunnerError,
            )


def _materialize_attempt_ledger(
    *,
    result: Mapping[str, Any],
    case: Mapping[str, Any],
    contract: Mapping[str, Any],
    original_binary: Path,
    product_binary: Path,
    source_roots: Mapping[str, Path] | None,
    attempt_id: str,
    attempt_index: int,
    trusted_ncm_attestation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _require(isinstance(result, Mapping), "case result must be an object", RunnerError)
    _require(isinstance(case, Mapping), "case must be an object", RunnerError)
    _require(isinstance(contract, Mapping), "case contract must be an object", RunnerError)
    existing = result.get("attempt_ledger")
    if isinstance(existing, list) and existing and isinstance(existing[0], Mapping):
        row = dict(existing[0])
        missing = sorted(set(LEDGER_REQUIRED_FIELDS) - row.keys())
        if not missing:
            try:
                _validate_attempt_ledger_identity(
                    row,
                    attempt_id=attempt_id,
                    attempt_index=attempt_index,
                )
                _require(
                    isinstance(row.get("outcome"), str) and row["outcome"] in OUTCOME_STATUSES,
                    "attempt ledger outcome is invalid",
                    RunnerError,
                )
                _require(isinstance(row.get("track_identity"), Mapping), "attempt ledger track identity is malformed", RunnerError)
                _require(isinstance(row.get("effect_evidence"), Mapping), "attempt ledger effect evidence is malformed", RunnerError)
                _require(isinstance(row.get("request_evidence"), Mapping), "attempt ledger request evidence is malformed", RunnerError)
                if row["outcome"] == "pass":
                    _require(result.get("status") == "pass", "attempt ledger pass disagrees with case result status", RunnerError)
                    composition = row.get("composition_reached")
                    _require(
                        isinstance(composition, Mapping)
                        and all(composition.get(side) is True for side in SIDE_NAMES),
                        "attempt ledger pass lacks complete composition evidence",
                        RunnerError,
                    )
                    for side in SIDE_NAMES:
                        reachability = row.get("operation_reachability", {}).get(side) if isinstance(row.get("operation_reachability"), Mapping) else None
                        store = row.get(
                            "reference_store_identity" if side == "original" else "candidate_store_identity"
                        )
                        _require(
                            isinstance(reachability, Mapping)
                            and reachability.get("observed") is True
                            and reachability.get("authority_correlated") is True,
                            f"attempt ledger pass lacks {side} operation reachability",
                            RunnerError,
                        )
                        _require(
                            isinstance(store, Mapping)
                            and store.get("observed") is True
                            and store.get("authority_correlated") is True,
                            f"attempt ledger pass lacks {side} store identity",
                            RunnerError,
                        )
                    _require(row.get("track_identity", {}).get("complete") is True, "attempt ledger pass lacks complete track identity", RunnerError)
                    _require(row.get("effect_evidence", {}).get("complete") is True, "attempt ledger pass lacks complete effect evidence", RunnerError)
                    _require(row.get("request_evidence", {}).get("complete") is True, "attempt ledger pass lacks complete request evidence", RunnerError)
                    candidate_sha = row.get("source_sha", {}).get("candidate") if isinstance(row.get("source_sha"), Mapping) else None
                    _require(
                        isinstance(candidate_sha, str) and re.fullmatch(r"[0-9a-f]{40}", candidate_sha.lower()) is not None,
                        "attempt ledger pass lacks a full candidate source revision",
                        RunnerError,
                    )
                    _validate_ledger_bindings(row)
                return row
            except RunnerError:
                pass
    try:
        return _build_attempt_ledger(
            result=result,
            case=case,
            contract=contract,
            original_binary=original_binary,
            product_binary=product_binary,
            source_roots=source_roots,
            attempt_id=attempt_id,
            attempt_index=attempt_index,
            trusted_ncm_attestation=trusted_ncm_attestation,
        )
    except (RunnerError, OSError, TypeError, ValueError) as error:
        try:
            return _fallback_attempt_ledger(
                result=result,
                case=case,
                contract=contract,
                attempt_id=attempt_id,
                attempt_index=attempt_index,
                source_roots=source_roots,
            )
        except BaseException as fallback_error:
            return _minimal_attempt_ledger(
                result=result,
                case=case,
                attempt_id=attempt_id,
                attempt_index=attempt_index,
                reason=(
                    f"ledger materialization failed: {type(error).__name__}: {error}; "
                    f"fallback failed: {type(fallback_error).__name__}: {fallback_error}"
                ),
            )


def _validate_attempt_ledger_identity(
    row: Mapping[str, Any],
    *,
    attempt_id: str,
    attempt_index: int,
    seen_ids: set[str] | None = None,
) -> None:
    """Reject relabeled, duplicate, or malformed attempt identities."""

    _require(isinstance(row, Mapping), "attempt ledger row must be an object", RunnerError)
    _require(
        isinstance(attempt_id, str) and bool(attempt_id.strip()),
        "attempt id must be a non-empty string",
        RunnerError,
    )
    _require(
        isinstance(attempt_index, int) and not isinstance(attempt_index, bool) and attempt_index >= 0,
        "attempt index must be a non-negative integer",
        RunnerError,
    )
    _require(row.get("attempt_id") == attempt_id, "attempt ledger identity does not match the executing case", RunnerError)
    _require(row.get("attempt_index") == attempt_index, "attempt ledger index does not match the executing case", RunnerError)
    row_id = row.get("row_id")
    _require(isinstance(row_id, str) and bool(row_id.strip()), "attempt ledger row_id is missing", RunnerError)
    _require(
        isinstance(row.get("case_id"), str) and row.get("case_id") == row_id
        and ("id" not in row or row.get("id") == row_id),
        "attempt ledger case_id must be present and equal row_id",
        RunnerError,
    )
    binding = row.get("runner_attempt_binding")
    _require(
        isinstance(binding, Mapping)
        and binding.get("row_id") == row_id
        and binding.get("case_id") == row.get("case_id")
        and binding.get("attempt_id") == attempt_id
        and binding.get("attempt_index") == attempt_index,
        "attempt ledger runner attempt binding does not match the executing case",
        RunnerError,
    )
    if seen_ids is not None:
        _require(attempt_id not in seen_ids, f"duplicate attempt ledger identity: {attempt_id}", RunnerError)


def run_case(
    case: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    original_binary: Path,
    product_binary: Path,
    case_artifact_dir: Path,
    timeouts: ProcessTimeouts = ProcessTimeouts(),
    candidate_product_revision: str | None = None,
    source_roots: Mapping[str, Path] | None = None,
    attempt_id: str | None = None,
    attempt_index: int = 0,
    trusted_ncm_attestation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run one case with a runner-owned foreground daemon per side."""

    validate_case(case, contract)
    case_artifact_dir.mkdir(parents=True, exist_ok=False)
    original_dir = case_artifact_dir / "original"
    product_dir = case_artifact_dir / "product"
    original_dir.mkdir()
    product_dir.mkdir()
    daemons: dict[str, _OwnedDaemon] = {}
    daemon_cleanup: dict[str, Any] = {}
    profile_roots: dict[str, Path] = {}
    result: dict[str, Any] | None = None
    failure: BaseException | None = None
    try:
        for side, binary, side_dir in (
            ("original", original_binary, original_dir),
            ("product", product_binary, product_dir),
        ):
            profile_root = _owned_relative_path(
                case.get("profile_root"),
                side_dir=side_dir,
                field="profile_root",
                default="profile",
            )
            profile_roots[side] = profile_root
            daemons[side] = _OwnedDaemon.start(
                side=side,
                binary=binary,
                side_dir=side_dir,
                profile_root=profile_root,
                artifact_dir=case_artifact_dir / "daemon" / side,
                timeouts=timeouts,
            )
        result = _run_case_inner(
            case,
            contract=contract,
            original_binary=original_binary,
            product_binary=product_binary,
            case_artifact_dir=case_artifact_dir,
            timeouts=timeouts,
            candidate_product_revision=candidate_product_revision,
            daemons=daemons,
            source_roots=source_roots,
        )
    except BaseException as error:
        # Preserve the primary failure while still allowing the finally block
        # to capture every daemon that was started before the failure.
        failure = error
        raise
    finally:
        # This finally covers setup failures, request rendering errors, parser
        # failures, and comparison exceptions alike.  Every daemon started by
        # this case is stopped by identity and process group, never by an
        # ambient socket or a guessed PID.
        for side, daemon in daemons.items():
            try:
                daemon_cleanup[side] = daemon.stop(
                    timeouts, reason="case final cleanup"
                )
            except BaseException as error:
                daemon_cleanup[side] = {
                    "status": "unknown",
                    "reason": f"daemon final cleanup failed: {error}",
                    "identity": copy.deepcopy(daemon.identity),
                }
        if daemons:
            try:
                _write_json(case_artifact_dir / "daemon-cleanup.json", daemon_cleanup)
            except (OSError, TypeError, ValueError):
                # Preserve the case failure or result even if the aggregate
                # cleanup receipt cannot be written.
                pass
        if result is None:
            # An exception raised during setup, action rendering, or parsing
            # skips the normal return path.  Keep a machine-readable failure
            # receipt beside the per-daemon artifacts without replacing the
            # original exception that the caller needs to see.
            reason = (
                f"{type(failure).__name__}: {failure}"
                if failure is not None
                else "case did not produce a result"
            )
            reason_lower = reason.lower()
            failure_class = (
                "spawn_failure"
                if "spawn" in reason_lower or "could not be spawned" in reason_lower
                else "cleanup_failure"
                if "cleanup" in reason_lower or "did not settle" in reason_lower
                else "runner_failure"
            )
            failed_result = {
                "case_id": case.get("id", case.get("case_id")),
                "classification": case.get("classification", "original_native"),
                "status": "unknown",
                "reason": reason,
                "failure_class": failure_class,
                "daemon_cleanup": copy.deepcopy(daemon_cleanup),
                "artifact_directory": str(case_artifact_dir),
                "roots": {
                    side: {
                        "source_root": str((source_roots or {}).get(side).resolve())
                        if (source_roots or {}).get(side) is not None
                        else None,
                        "binary_root": str(Path(binary).resolve().parent),
                        "process_root": str((side_dir / "process").resolve()),
                        "project_root": str((side_dir / "project").resolve()),
                        "profile_root": str(profile_roots.get(side, side_dir / "profile").resolve()),
                        "state_root": str((side_dir / "state").resolve()),
                        "store_root": str(profile_roots.get(side, side_dir / "profile").resolve()),
                        "socket_root": str(_daemon_socket_path(side_dir).parent.resolve()),
                        "artifact_root": str((case_artifact_dir / side).resolve()),
                    }
                    for side, binary, side_dir in (
                        ("original", original_binary, original_dir),
                        ("product", product_binary, product_dir),
                    )
                },
            }
            if attempt_id is not None:
                try:
                    failed_result["attempt_ledger"] = [
                        _build_attempt_ledger(
                            result=failed_result,
                            case=case,
                            contract=contract,
                            original_binary=original_binary,
                            product_binary=product_binary,
                            source_roots=source_roots,
                            attempt_id=attempt_id,
                            attempt_index=attempt_index,
                            trusted_ncm_attestation=trusted_ncm_attestation,
                        )
                    ]
                except (RunnerError, OSError, TypeError, ValueError):
                    failed_result["attempt_ledger"] = []
            try:
                _finalize_case_result_digest(failed_result)
                _write_json(case_artifact_dir / "case-result.json", failed_result)
            except BaseException:
                # Evidence writing must not mask the setup/action exception.
                pass
    if result is None:
        # This branch is only reachable if a custom BaseException was somehow
        # swallowed by a caller.  Keep the public return contract total.
        result = {
            "case_id": case.get("id", case.get("case_id")),
            "classification": case.get("classification", "original_native"),
            "status": "unknown",
            "reason": "case did not produce a result",
            "daemon_cleanup": copy.deepcopy(daemon_cleanup),
            "artifact_directory": str(case_artifact_dir),
        }
    else:
        result["daemon_cleanup"] = daemon_cleanup
        result["roots"] = {
            side: {
                "source_root": str((source_roots or {}).get(side).resolve())
                if (source_roots or {}).get(side) is not None
                else None,
                "binary_root": str(Path(binary).resolve().parent),
                "process_root": str((side_dir / "process").resolve()),
                "store_root": str(profile_roots[side]),
                "project_root": str(_owned_relative_path(
                    case.get("project_root"),
                    side_dir=side_dir,
                    field="project_root",
                    default="project",
                )),
                "profile_root": str(profile_roots[side]),
                "state_root": str(_owned_relative_path(
                    case.get("state_root"),
                    side_dir=side_dir,
                    field="state_root",
                    default="state",
                )),
                "socket_root": str(_daemon_socket_path(side_dir).parent.resolve()),
                "artifact_root": str((case_artifact_dir / side).resolve()),
            }
            for side, binary, side_dir in (
                ("original", original_binary, original_dir),
                ("product", product_binary, product_dir),
            )
        }
        cleanup_failures = {
            side: copy.deepcopy(receipt)
            for side, receipt in daemon_cleanup.items()
            if receipt.get("status") != "completed"
        }
        if cleanup_failures:
            result["status"] = _status_priority((result.get("status", "unknown"), "unknown"))
            result["cleanup_failure"] = cleanup_failures
            result["cleanup_failure_reason"] = (
                "runner-owned process cleanup did not complete for one or more sides"
            )
        if attempt_id is not None:
            result["attempt_ledger"] = [
                _build_attempt_ledger(
                    result=result,
                    case=case,
                    contract=contract,
                    original_binary=original_binary,
                    product_binary=product_binary,
                    source_roots=source_roots,
                    attempt_id=attempt_id,
                    attempt_index=attempt_index,
                    trusted_ncm_attestation=trusted_ncm_attestation,
                )
            ]
        artifact_result = result
        if attempt_id is not None and result.get("status") == "pass":
            # A pass is not durable until the enclosing suite appends the
            # authoritative ledger receipt.  Keep the returned in-memory
            # result intact for the append, but stage an unknown artifact so
            # readers cannot observe a premature pass.
            artifact_result = copy.deepcopy(result)
            artifact_result["status"] = "unknown"
            artifact_result["reason"] = "pass is staged until the authoritative attempt ledger receipt is appended"
            staged_rows = artifact_result.get("attempt_ledger")
            if isinstance(staged_rows, list):
                for staged_row in staged_rows:
                    if isinstance(staged_row, dict) and staged_row.get("outcome") == "pass":
                        staged_row["outcome"] = "unknown"
                        staged_row["actual"] = {"status": "unknown", "reason": artifact_result["reason"]}
                        staged_row["failure_class"] = artifact_result["reason"]
        _finalize_case_result_digest(artifact_result)
        _write_json(case_artifact_dir / "case-result.json", artifact_result)
    return result


def _rewrite_json_atomically(path: Path, value: Any, *, label: str = "artifact") -> dict[str, Any]:
    """Replace a finalized JSON artifact without exposing a partial file."""

    temporary = path.with_name(f".{path.name}.final-{time.time_ns()}-{os.getpid()}")
    try:
        receipt = _write_json(temporary, value)
        os.replace(temporary, path)
        return {**receipt, "path": str(path), "atomic": True, "label": label}
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass
        raise


def _finalize_run_manifest(
    manifest: Mapping[str, Any],
    *,
    run_dir: Path,
    case_results: Sequence[Mapping[str, Any]],
    readiness_population: Mapping[str, Any] | None,
    status: str,
    error: BaseException | None = None,
) -> dict[str, Any]:
    """Write the authoritative post-execution manifest, including partial runs."""

    _require(isinstance(manifest, Mapping), "run manifest must be an object", RunnerError)
    _require(isinstance(run_dir, Path), "run directory must be a Path", RunnerError)
    _require(isinstance(case_results, (list, tuple)), "case results must be a list", RunnerError)
    _require(isinstance(status, str) and status in OUTCOME_STATUSES, "run status is invalid", RunnerError)
    try:
        final = copy.deepcopy(dict(manifest))
    except (RecursionError, TypeError, ValueError) as error:
        raise RunnerError(f"run manifest contains malformed or too deeply nested data: {error}") from error
    final["status"] = status
    final["case_count"] = len(case_results)
    # Count only rows whose append receipt survived into the finalized case
    # result.  A ledger-shaped in-memory row is not population evidence when
    # append or case-result finalization failed.
    final["observed_attempts"] = sum(
        len(result.get("attempt_ledger", ()))
        for result in case_results
        if (
            isinstance(result, Mapping)
            and _valid_ledger_receipt(
                result.get("attempt_ledger_receipt"),
                verify_file=True,
                expected_path=run_dir / CANONICAL_ATTEMPT_LEDGER_NAME,
                expected_case_id=result.get("case_id") if isinstance(result.get("case_id"), str) else None,
            )
            and isinstance(result.get("attempt_ledger", ()), Sequence)
            and not isinstance(result.get("attempt_ledger", ()), (str, bytes))
        )
    )
    final["attempt_ledger_path"] = str(run_dir / "attempts.jsonl")
    final["finalized_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    final["roots"] = {
        side: sorted(
            {
                str(value)
                for result in case_results
                if isinstance(result, Mapping)
                for roots in (result.get("roots", {}),)
                if isinstance(roots, Mapping)
                for side_roots in (roots.get(side, {}),)
                if isinstance(side_roots, Mapping)
                for value in side_roots.values()
                if isinstance(value, str) and value
            }
        )
        for side in SIDE_NAMES
    }
    if readiness_population is not None:
        _require(isinstance(readiness_population, Mapping), "readiness population must be an object", RunnerError)
        try:
            readiness = final.setdefault("readiness", {})
            _require(isinstance(readiness, Mapping), "run manifest readiness must be an object", RunnerError)
            readiness["population"] = copy.deepcopy(dict(readiness_population))
        except (RecursionError, TypeError, ValueError) as error:
            raise RunnerError(f"readiness population is malformed or too deeply nested: {error}") from error
    if error is not None:
        final["error"] = {
            "type": type(error).__name__,
            "message": str(error),
            "causal_stage": "runner",
        }
    _rewrite_json_atomically(run_dir / "run-manifest.json", final, label="run-manifest")
    return final

def run_suite(
    *,
    contract: Mapping[str, Any],
    cases: Sequence[Mapping[str, Any]],
    reference_checkout: str | os.PathLike[str],
    original_binary: str | os.PathLike[str],
    product_binary: str | os.PathLike[str],
    artifact_root: str | os.PathLike[str],
    required_operations: Iterable[str] = (),
    timeouts: ProcessTimeouts = ProcessTimeouts(),
    run_id: str | None = None,
    product_source_revision: str | None = None,
    product_source_root: str | os.PathLike[str] | None = None,
    original_build_attestation: str | os.PathLike[str] | None = None,
    product_build_attestation: str | os.PathLike[str] | None = None,
    ncm_worker_attestation: str | os.PathLike[str] | None = None,
    readiness_mode: str = "comparison",
    readiness_matrix_path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    _require(isinstance(contract, Mapping), "case contract must be an object", RunnerError)
    _require(isinstance(cases, (list, tuple)), "cases must be a list", RunnerError)
    _require(
        not isinstance(required_operations, (str, bytes)) and required_operations is not None,
        "required operations must be an iterable of route names",
        RunnerError,
    )
    _require(isinstance(timeouts, ProcessTimeouts), "process timeout configuration is invalid", RunnerError)
    validate_contract(contract)
    try:
        required = tuple(required_operations)
    except (TypeError, ValueError) as error:
        raise RunnerError("required operations must be an iterable of route names") from error
    _require(
        all(isinstance(operation, str) and bool(operation.strip()) for operation in required),
        "required operations must contain non-empty route names",
        RunnerError,
    )
    readiness_matrix = (
        load_readiness_matrix(readiness_matrix_path)
        if readiness_mode == "readiness"
        else None
    )
    validated_readiness = (
        validate_frozen_readiness_matrix(readiness_matrix)
        if readiness_matrix is not None
        else None
    )
    readiness_rows = {
        str(row["id"]): row
        for row in (validated_readiness.get("rows", ()) if validated_readiness else ())
    }
    selected = validate_readiness_selection(
        cases,
        required,
        readiness_mode=readiness_mode,
        readiness_matrix=readiness_matrix,
    )
    for case in selected:
        validate_case(case, contract)
    trusted_ncm_attestation = (
        _load_ncm_worker_attestation(ncm_worker_attestation)
        if ncm_worker_attestation is not None
        else None
    )
    reference = verify_reference_checkout(reference_checkout)
    reference_source_root = Path(reference["path"]).resolve()
    product_source = verify_source_root(
        product_source_root or Path(__file__).resolve().parents[3],
        side="product",
        reference_root=reference_source_root,
    )
    product_source_revision = resolve_product_revision(
        Path(product_source["path"]), product_source_revision
    )
    original, product = verify_distinct_binaries(original_binary, product_binary)
    _require(
        original_build_attestation is not None,
        "reference build attestation is required to bind the original binary to the verified 570 checkout",
        BinaryError,
    )
    _require(
        product_build_attestation is not None,
        "candidate build attestation is required to bind the binary to its source revision",
        BinaryError,
    )
    original_attestation = verify_build_attestation(
        original_build_attestation,
        binary=original,
        source_root=reference,
        expected_revision=REFERENCE_REVISION,
        expected_features=(
            contract.get("baseline", {}).get("reference_features")
            if isinstance(contract.get("baseline", {}), Mapping)
            else None
        ),
        side="original",
    )
    product_attestation = verify_build_attestation(
        product_build_attestation,
        binary=product,
        source_root=product_source,
        expected_revision=product_source_revision,
        expected_features=(
            contract.get("baseline", {}).get("candidate_features")
            if isinstance(contract.get("baseline", {}), Mapping)
            else None
        ),
        side="product",
    )
    try:
        root = Path(artifact_root).expanduser()
    except (TypeError, ValueError) as error:
        raise RunnerError(f"artifact root is invalid: {artifact_root!r}") from error
    if not root.is_absolute():
        root = root.resolve()
    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise RunnerError(f"cannot create artifact root {root}: {error}") from error
    if run_id is not None:
        _require(isinstance(run_id, str) and bool(run_id.strip()), "run id must be a non-empty string", RunnerError)
    token = run_id or f"run-{time.time_ns()}-{os.getpid()}"
    run_dir = root / _slug(token)
    try:
        run_dir.mkdir()
    except FileExistsError as error:
        raise RunnerError(f"run-id already exists; refusing to overwrite evidence: {run_dir}") from error
    except OSError as error:
        raise RunnerError(f"cannot create run directory {run_dir}: {error}") from error
    input_original = copy.deepcopy(original)
    input_product = copy.deepcopy(product)
    try:
        # Execute immutable copies so a producer cannot replace or mutate the
        # attested path after setup.  The pre/post/source hashes remain in the
        # manifest and every ledger row uses the same copied executable hash.
        original = _copy_immutable_binary(
            input_original,
            root=run_dir / "immutable-binaries",
            side="original",
            expected_sha256=original_attestation.get("binary_sha256"),
        )
        product = _copy_immutable_binary(
            input_product,
            root=run_dir / "immutable-binaries",
            side="product",
            expected_sha256=product_attestation.get("binary_sha256"),
        )
    except BaseException as error:
        # The run directory is already owned by this invocation.  Preserve a
        # typed finalized manifest for copy failures before surfacing the
        # setup error to the caller.
        failed_manifest = {
            "format": CONTRACT_FORMAT,
            "contract_id": contract.get("contract_id"),
            "run_id": token,
            "reference": reference,
            "source_roots": {"original": reference, "product": product_source},
            "input_binaries": {"original": input_original, "product": input_product},
            "build_attestations": {"original": original_attestation, "product": product_attestation},
            "readiness": {"mode": readiness_mode, "matrix_id": READINESS_MATRIX_ID},
        }
        _finalize_run_manifest(
            failed_manifest,
            run_dir=run_dir,
            case_results=[],
            readiness_population=None,
            status="blocked",
            error=error,
        )
        raise
    manifest = {
        "format": CONTRACT_FORMAT,
        "contract_id": contract["contract_id"],
        "run_id": token,
        "reference": reference,
        "source_roots": {
            "original": reference,
            "product": product_source,
        },
        "input_binaries": {"original": input_original, "product": input_product},
        "binaries": {"original": original, "product": product},
        "binary_roots": {
            "original": str(Path(original["path"]).resolve().parent),
            "product": str(Path(product["path"]).resolve().parent),
        },
        "expected_product_revision": PRODUCT_REVISION,
        "product_source_revision": product_source_revision,
        "build_attestations": {"original": original_attestation, "product": product_attestation},
        "immutable_binary_copies": {
            "original": copy.deepcopy(original),
            "product": copy.deepcopy(product),
        },
        "historical_audit": {
            "reference_revision": HISTORICAL_REFERENCE_REVISION,
            "product_revision": HISTORICAL_PRODUCT_REVISION,
            "status": "historical-only; never used as an execution oracle",
        },
        "selected_cases": [case.get("id", case.get("case_id")) for case in selected],
        "required_operations": sorted(set(required)),
        "readiness": {
            "mode": readiness_mode,
            "matrix_id": READINESS_MATRIX_ID,
            "matrix_path": readiness_matrix.get("_path") if readiness_matrix else None,
            "matrix_sha256": readiness_matrix.get("_sha256") if readiness_matrix else None,
            "row_count": len(readiness_matrix.get("rows", ())) if readiness_matrix else None,
            "accepted_row_ids": sorted(
                row["id"] for row in readiness_matrix.get("rows", ())
            ) if readiness_matrix else [],
            "rows": [
                {
                    "id": row["id"],
                    "oracle_kind": row["oracle_kind"],
                    "planned_runs": row["runs"]["planned"],
                    "profile": row["runs"].get("profile"),
                }
                for row in readiness_matrix.get("rows", ())
            ] if readiness_matrix else [],
            "planned_attempts": (
                sum(int(row["runs"]["planned"]) for row in readiness_matrix.get("rows", ()))
                if readiness_matrix
                else None
            ),
            "filtered_suite_rejected": readiness_mode == "readiness" and bool(required),
        },
        "timeouts": asdict(timeouts),
        "composition": COMPOSITIONS,
    }
    _write_json(run_dir / "run-manifest.json", manifest)
    case_results: list[dict[str, Any]] = []
    case_result_paths: list[tuple[dict[str, Any], Path]] = []
    attempt_ids_seen: set[str] = set()
    row_attempt_counts: dict[str, int] = {}
    suite_error: BaseException | None = None
    suite_traceback = None
    for index, source_case in enumerate(selected):
        # Attach the immutable row only for execution/ledger derivation.  The
        # consumed fixture remains otherwise unchanged and the binding was
        # validated against the raw frozen matrix above.
        case = copy.deepcopy(dict(source_case))
        case_id = case.get("id", case.get("case_id"))
        case_dir = run_dir / f"{index:03d}-{_slug(str(case_id))}"
        attempt_id = f"{token}/{index}"
        side_roots = {
            "original": reference_source_root,
            "product": Path(product_source["path"]),
        }
        case_matrix_row = readiness_rows.get(str(case_id))
        if case_matrix_row is None:
            for matrix_row_id in _case_matrix_row_ids(case):
                case_matrix_row = readiness_rows.get(matrix_row_id)
                if case_matrix_row is not None:
                    break
        attempt_index = index
        if readiness_mode == "readiness" and case_matrix_row is not None:
            matrix_row_id = str(case_matrix_row["id"])
            attempt_index = row_attempt_counts.get(matrix_row_id, 0)
            row_attempt_counts[matrix_row_id] = attempt_index + 1
        if case_matrix_row is not None:
            case["_matrix_row"] = copy.deepcopy(case_matrix_row)
        case_error: BaseException | None = None
        case_traceback = None
        try:
            case_result = run_case(
                case,
                contract=contract,
                original_binary=Path(original["path"]),
                product_binary=Path(product["path"]),
                case_artifact_dir=case_dir,
                timeouts=timeouts,
                candidate_product_revision=product_source_revision,
                source_roots=side_roots,
                attempt_id=attempt_id,
                attempt_index=attempt_index,
                trusted_ncm_attestation=trusted_ncm_attestation,
            )
        except BaseException as error:
            case_error = error
            case_traceback = error.__traceback__
            case_result: dict[str, Any] = {}
            try:
                recovered = _load_json(case_dir / "case-result.json")
                if isinstance(recovered, Mapping):
                    case_result = dict(recovered)
            except (ContractError, OSError, TypeError, ValueError):
                pass
            case_result.setdefault("case_id", case_id)
            case_result.setdefault("classification", case.get("classification", "original_native"))
            case_result["status"] = "unknown"
            case_result.setdefault("reason", f"{type(error).__name__}: {error}")
            case_result.setdefault("artifact_directory", str(case_dir))
            try:
                case_dir.mkdir(parents=True, exist_ok=True)
                _write_json(case_dir / "case-result.json", case_result)
            except (OSError, TypeError, ValueError, RecursionError):
                pass
        # These fields are allocated by the runner after the callable
        # returns.  A child supplied attempt id/index or case label cannot
        # relabel the append position or satisfy readiness population.
        bound_row_id = (
            str(case_matrix_row["id"])
            if isinstance(case_matrix_row, Mapping) and isinstance(case_matrix_row.get("id"), str)
            else str(case_id)
        )
        runner_attempt_binding = {
            "run_id": token,
            "row_id": bound_row_id,
            "case_id": str(case_id),
            "attempt_id": attempt_id,
            "attempt_index": attempt_index,
        }
        reported_case_id = case_result.get("case_id")
        reported_id = case_result.get("id")
        if (
            reported_case_id is not None
            and reported_case_id != case_id
        ) or (
            reported_id is not None
            and reported_id != case_id
        ):
            case_result["status"] = "blocked"
            case_result["reason"] = "child case identity conflicts with the runner-owned case id"
            case_result["failure_class"] = "case_identity_mismatch"
            case_result["reported_case_id"] = copy.deepcopy(reported_case_id)
            case_result["reported_id"] = copy.deepcopy(reported_id)
        case_result["case_id"] = str(case_id)
        if "id" in case_result:
            case_result["id"] = str(case_id)
        case_result["runner_attempt_binding"] = runner_attempt_binding
        # These bindings are runner-owned inputs, attached before ledger
        # derivation so a producer response cannot choose its own provenance.
        case_result.setdefault(
            "build_attestations",
            {"original": copy.deepcopy(original_attestation), "product": copy.deepcopy(product_attestation)},
        )
        case_result.setdefault(
            "binary_provenance",
            {"original": copy.deepcopy(original), "product": copy.deepcopy(product)},
        )
        case_result.setdefault(
            "source_bindings",
            {"original": copy.deepcopy(reference), "product": copy.deepcopy(product_source)},
        )
        try:
            ledger_row = _materialize_attempt_ledger(
                result=case_result,
                case=case,
                contract=contract,
                original_binary=Path(original["path"]),
                product_binary=Path(product["path"]),
                source_roots=side_roots,
                attempt_id=attempt_id,
                attempt_index=attempt_index,
                trusted_ncm_attestation=trusted_ncm_attestation,
            )
        except BaseException as error:
            # A ledger row is mandatory even when a case raised or an
            # evidence helper failed.  Preserve the failure as typed runner
            # evidence instead of dropping the attempt.
            fallback_result = {
                **case_result,
                "status": "unknown",
                "reason": f"ledger materialization failed: {type(error).__name__}: {error}",
                "failure_class": "ledger_materialization_failure",
            }
            try:
                ledger_row = _fallback_attempt_ledger(
                    result=fallback_result,
                    case=case,
                    contract=contract,
                    attempt_id=attempt_id,
                    attempt_index=attempt_index,
                    source_roots=side_roots,
                )
            except BaseException as fallback_error:
                ledger_row = _minimal_attempt_ledger(
                    result=fallback_result,
                    case=case,
                    attempt_id=attempt_id,
                    attempt_index=attempt_index,
                    reason=(
                        f"ledger materialization failed: {type(error).__name__}: {error}; "
                        f"fallback failed: {type(fallback_error).__name__}: {fallback_error}"
                    ),
                )
        if case_matrix_row is not None:
            case_result["matrix_row"] = {
                "id": case_matrix_row["id"],
                "track": case_matrix_row.get("track"),
                "operation": case_matrix_row.get("operation"),
                "oracle_kind": case_matrix_row.get("oracle_kind"),
                "oracle_case": case_matrix_row.get("oracle", {}).get("case") if isinstance(case_matrix_row.get("oracle"), Mapping) else None,
                "profile": case_matrix_row.get("runs", {}).get("profile") if isinstance(case_matrix_row.get("runs"), Mapping) else None,
                "identity_requirements": copy.deepcopy(case_matrix_row.get("identity_requirements")),
                "track_identity_requirements": copy.deepcopy(case_matrix_row.get("track_identity_requirements")),
            }
            ledger_row["oracle_kind"] = case_matrix_row["oracle_kind"]
            ledger_row["expected"] = {
                **(
                    dict(ledger_row["expected"])
                    if isinstance(ledger_row.get("expected"), Mapping)
                    else {"value": ledger_row.get("expected")}
                ),
                "matrix_row_id": case_matrix_row["id"],
                "planned_runs": case_matrix_row["runs"]["planned"],
                "run_profile": case_matrix_row["runs"].get("profile"),
            }
            ledger_row["matrix_row"] = {
                "id": case_matrix_row["id"],
                "track": case_matrix_row.get("track"),
                "operation": case_matrix_row.get("operation"),
                "oracle_kind": case_matrix_row["oracle_kind"],
                "oracle_case": case_matrix_row.get("oracle", {}).get("case") if isinstance(case_matrix_row.get("oracle"), Mapping) else None,
                "identity": case_matrix_row.get("identity"),
                "identity_evidence": copy.deepcopy(case_matrix_row.get("identity_evidence")),
                "effect_receipt_state": copy.deepcopy(case_matrix_row.get("effect_receipt_state")),
                "identity_requirements": copy.deepcopy(case_matrix_row.get("identity_requirements")),
                "track_identity_requirements": copy.deepcopy(case_matrix_row.get("track_identity_requirements")),
                "planned_runs": case_matrix_row["runs"]["planned"],
                "profile": case_matrix_row["runs"].get("profile"),
            }
            ledger_row["matrix_binding"] = copy.deepcopy(case.get("matrix_binding", case.get("readiness_binding", {})))
            # Matrix metadata is attached after the generic ledger builder;
            # re-run the binding verifier over the final row so a caller
            # cannot forge operation/oracle/track/identity fields between
            # materialization and the append-only write.
            try:
                _validate_ledger_bindings(ledger_row)
                ledger_row["binding_validation"] = {"status": "pass"}
            except RunnerError as error:
                ledger_row["binding_validation"] = {"status": "blocked", "reason": str(error)}
                ledger_row["outcome"] = "blocked"
                ledger_row["actual"] = {"status": "blocked", "reason": str(error)}
                ledger_row["failure_class"] = str(error)
                ledger_row["first_causal_stage"] = "runner"
                case_result["status"] = "blocked"
                case_result["reason"] = str(error)
                ledger_row["result_digest"] = None
        try:
            _validate_attempt_ledger_identity(
                ledger_row,
                attempt_id=attempt_id,
                attempt_index=attempt_index,
                seen_ids=attempt_ids_seen,
            )
        except RunnerError as error:
            ledger_row["outcome"] = "blocked"
            ledger_row["actual"] = {"status": "blocked", "reason": str(error)}
            ledger_row["failure_class"] = str(error)
            ledger_row["first_causal_stage"] = "runner"
            ledger_row["reopen_or_no_effect"] = {
                **(
                    dict(ledger_row.get("reopen_or_no_effect"))
                    if isinstance(ledger_row.get("reopen_or_no_effect"), Mapping)
                    else {}
                ),
                "status": "blocked",
                "reason": str(error),
            }
            case_result["status"] = "blocked"
            case_result["reason"] = str(error)
            ledger_row["result_digest"] = None
        if ledger_row.get("outcome") != case_result.get("status"):
            case_result["status"] = ledger_row.get("outcome", "unknown")
            case_result["reason"] = ledger_row.get("failure_class") or ledger_row.get("outcome")
            ledger_row["actual"] = {
                "status": case_result["status"],
                "reason": case_result.get("reason"),
            }
            ledger_row["result_digest"] = None
        attempt_ids_seen.add(attempt_id)
        case_result["attempt_ledger"] = [ledger_row]
        case_results.append(case_result)
        case_result_paths.append((case_result, case_dir))
        try:
            ledger_ref = _append_jsonl(run_dir / "attempts.jsonl", ledger_row)
            _require(
                _valid_ledger_receipt(
                    ledger_ref,
                    expected_path=run_dir / CANONICAL_ATTEMPT_LEDGER_NAME,
                    expected_case_id=ledger_row.get("case_id"),
                    expected_attempt_id=ledger_row.get("attempt_id"),
                    expected_attempt_index=ledger_row.get("attempt_index"),
                ),
                "authoritative attempt ledger append returned an invalid receipt",
                RunnerError,
            )
        except BaseException as error:
            # The case-result may have been a comparison pass in memory, but
            # without a durable append receipt it must remain unknown in every
            # finalized artifact.  Do this before the final digest pass so an
            # append failure cannot leave a pass-looking case-result behind.
            case_result["status"] = "unknown"
            case_result["reason"] = (
                "authoritative attempt ledger append failed: "
                f"{type(error).__name__}: {error}"
            )
            case_result["attempt_ledger_receipt"] = None
            if isinstance(ledger_row, dict):
                ledger_row["outcome"] = "unknown"
                ledger_row["actual"] = {
                    "status": "unknown",
                    "reason": case_result["reason"],
                }
                ledger_row["failure_class"] = "ledger_append_failure"
                ledger_row["first_causal_stage"] = "runner"
                ledger_row["result_digest"] = None
            suite_error = error
            suite_traceback = error.__traceback__
            break
        case_result["attempt_ledger_receipt"] = ledger_ref
        try:
            # ``attempt.json`` is a convenience copy.  The append-only JSONL
            # row above is the authority for pass durability; failure to write
            # this duplicate must not turn a successfully appended pass into a
            # contradictory unknown row.
            _write_json(
                case_dir / "attempt.json",
                {"row": ledger_row, "receipt": ledger_ref},
            )
        except BaseException as error:
            case_result["attempt_artifact_error"] = {
                "type": type(error).__name__,
                "message": str(error),
            }
        if case_error is not None:
            suite_error = case_error
            suite_traceback = case_traceback
            break
    readiness_population: dict[str, Any] | None = None
    if readiness_matrix is not None:
        try:
            readiness_population = _readiness_attempt_population(
                readiness_matrix,
                case_results,
                ledger_path=run_dir / CANONICAL_ATTEMPT_LEDGER_NAME,
                strict=True,
                run_id=token,
            )
        except BaseException as error:
            if suite_error is None:
                suite_error = error
                suite_traceback = error.__traceback__
            readiness_population = {
                "status": "blocked",
                "complete": False,
                "required_rows": None,
                "required_attempts": None,
                "observed_attempts": sum(
                    len(item.get("attempt_ledger", ()))
                    for item in case_results
                    if isinstance(item, Mapping)
                    and _valid_ledger_receipt(
                        item.get("attempt_ledger_receipt"),
                        verify_file=True,
                        expected_path=run_dir / CANONICAL_ATTEMPT_LEDGER_NAME,
                    )
                    and isinstance(item.get("attempt_ledger", ()), Sequence)
                    and not isinstance(item.get("attempt_ledger", ()), (str, bytes))
                ),
                "missing_attempts": None,
                "error": {"type": type(error).__name__, "message": str(error)},
            }
    # Finalize case-result artifacts only after the append-only ledger and the
    # readiness population are known.  This keeps each case artifact and the
    # run manifest in agreement when a short/partial campaign is blocked.
    for case_result, case_dir in case_result_paths:
        if readiness_population is not None:
            case_result["readiness_population"] = copy.deepcopy(readiness_population)
        try:
            _finalize_case_result_digest(case_result)
            _rewrite_json_atomically(case_dir / "case-result.json", case_result, label="case-result")
        except BaseException as error:
            if suite_error is None:
                suite_error = error
                suite_traceback = error.__traceback__
    report_status = _status_priority(result.get("status", "unknown") for result in case_results)
    if suite_error is not None:
        report_status = _status_priority((report_status, "unknown"))
    if readiness_population is not None and not readiness_population["complete"]:
        # A planned count is an obligation, not evidence that the attempts ran.
        # Keep the report typed as blocked until every frozen row has its full
        # population; a one-attempt-per-row invocation cannot claim readiness.
        report_status = "blocked"
    final_manifest = _finalize_run_manifest(
        manifest,
        run_dir=run_dir,
        case_results=case_results,
        readiness_population=readiness_population,
        status=report_status,
        error=suite_error,
    )
    report = {
        **final_manifest,
        "status": report_status,
        "cases": case_results,
        "case_count": len(case_results),
        "artifact_directory": str(run_dir),
        "attempt_ledger": [
            row
            for result in case_results
            for row in result.get("attempt_ledger", ())
        ],
        "attempt_ledger_path": str(run_dir / "attempts.jsonl"),
    }
    if readiness_population is not None:
        report["readiness_population"] = readiness_population
    _write_json(run_dir / "report.json", report)
    if suite_error is not None:
        raise suite_error.with_traceback(suite_traceback)
    return report


def _run_matrix_compatibility(
    *,
    row_id: str,
    ledger_path: Path,
    matrix_path: Path | None,
    reference_binary: Path | None,
    candidate_binary: Path | None,
) -> int:
    """Accept the frozen matrix's historical ``--case/--ledger`` shape.

    Matrix rows describe production campaigns rather than case fixture files.
    Until a campaign-specific adapter supplies the full run inputs, this
    compatibility entry point records a typed blocked attempt instead of
    rejecting the command at argument parsing or claiming a vacuous pass.
    """

    _require(isinstance(row_id, str) and bool(row_id.strip()), "matrix case row id must be a non-empty string", RunnerError)
    _require(isinstance(ledger_path, Path), "matrix ledger path must be a Path", RunnerError)
    _validate_existing_attempt_ledger(ledger_path)
    matrix = load_readiness_matrix(matrix_path)
    validated_matrix = validate_frozen_readiness_matrix(matrix)
    row = next(
        (candidate for candidate in validated_matrix["rows"] if candidate.get("id") == row_id),
        None,
    )
    _require(row is not None, f"frozen readiness matrix has no row {row_id!r}", RunnerError)
    planned = int(row["runs"]["planned"])
    command = [sys.executable, "-S", str(Path(__file__).resolve()), "--case", row_id, "--ledger", str(ledger_path)]
    if reference_binary is not None:
        command.extend(["--reference", str(reference_binary)])
    if candidate_binary is not None:
        command.extend(["--candidate", str(candidate_binary)])
    reason = (
        "matrix compatibility invocation recorded a blocked attempt; "
        "use the full run command with isolated reference/candidate inputs"
    )
    binding = _matrix_row_binding(row)
    matrix_row = {
        "id": row["id"],
        "track": row["track"],
        "operation": row["operation"],
        "oracle_kind": row["oracle_kind"],
        "oracle_case": row["oracle"]["case"],
        "identity": row["identity"],
        "identity_evidence": copy.deepcopy(row["identity_evidence"]),
        "effect_receipt_state": copy.deepcopy(row["effect_receipt_state"]),
        "identity_requirements": copy.deepcopy(
            matrix.get("identity_requirements", {})
        ),
        "track_identity_requirements": copy.deepcopy(
            matrix.get("identity_requirements", {}).get(row.get("track"), {})
            if isinstance(matrix.get("identity_requirements"), Mapping)
            else {}
        ),
        "planned_runs": planned,
        "profile": row["runs"]["profile"],
    }
    row_value: dict[str, Any] = {field: None for field in LEDGER_REQUIRED_FIELDS}
    row_value.update(
        {
            "actual": {"status": "blocked", "reason": reason},
            "attempt_id": f"matrix/{row_id}/{time.time_ns()}",
            "attempt_index": 0,
            "command": {"argv": command},
            "composition_reached": {side: False for side in SIDE_NAMES},
            "evidence_paths": [],
            "expected": {"outcome": "pass", "planned_runs": planned},
            "failure_class": "matrix_compatibility_requires_full_run_inputs",
            "feature_set": None,
            "first_causal_stage": "runner",
            "fixture_seed": None,
            "oracle_kind": row["oracle_kind"],
            "outcome": "blocked",
            "profile_paths": {},
            "protocol_version": "native-original.v1",
            "provider_id": None,
            "reopen_or_no_effect": {
                "status": "blocked",
                "reason": reason,
                "checkpoint_state": {},
                "reopen_state": {},
                "no_effect_evidence": {},
            },
            "case_id": row_id,
            "row_id": row_id,
            "source_sha": {"reference": REFERENCE_REVISION, "candidate": PRODUCT_REVISION},
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "track": row["track"],
            "track_identity": {
                "track": row["track"],
                "required_fields": copy.deepcopy(binding["required_identity_fields"]),
                "observed": {},
                "missing_fields": {
                    field: list(SIDE_NAMES) for field in binding["required_identity_fields"]
                },
                "complete": False,
            },
            "effect_evidence": {
                "policy": "unknown",
                "observed": {side: False for side in SIDE_NAMES},
                "complete": False,
                "reason": reason,
                "effect_digest": None,
                "receipt_digest": None,
                "state_digest": None,
            },
            "request_evidence": {
                "original": [],
                "product": [],
                "combined_sha256": None,
                "complete": False,
            },
            "roots": {side: {} for side in SIDE_NAMES},
        }
    )
    row_value["checkpoint_state"] = {side: {"observed": False, "observations": []} for side in SIDE_NAMES}
    row_value["operation_reachability"] = {side: {"observed": False, "observations": []} for side in SIDE_NAMES}
    row_value["reopen_state"] = {side: {"observed": False, "status": "blocked"} for side in SIDE_NAMES}
    row_value["no_effect_evidence"] = {side: {"observed": False, "observations": []} for side in SIDE_NAMES}
    row_value["authority_correlated"] = {side: False for side in SIDE_NAMES}
    row_value["runner_attempt_binding"] = {
        "run_id": f"matrix/{row_id}",
        "row_id": row_id,
        "case_id": row_id,
        "attempt_id": row_value["attempt_id"],
        "attempt_index": 0,
    }
    row_value["matrix_row"] = matrix_row
    row_value["matrix_binding"] = binding
    try:
        receipt = _append_jsonl(ledger_path, row_value)
    except (OSError, TypeError, ValueError) as error:
        raise RunnerError(f"cannot append matrix compatibility ledger: {error}") from error
    print(
        json.dumps(
            {
                "status": "blocked",
                "row_id": row_id,
                "oracle_kind": row["oracle_kind"],
                "planned_runs": planned,
                "ledger": receipt,
            },
            sort_keys=True,
        )
    )
    return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", dest="matrix_case", help="frozen readiness matrix row id compatibility mode")
    parser.add_argument("--ledger", dest="matrix_ledger", type=Path, help="append-only ledger path for matrix compatibility mode")
    parser.add_argument("--matrix", dest="matrix_path", type=Path, help="frozen readiness matrix path")
    parser.add_argument("--reference", dest="matrix_reference", type=Path, help="reference binary for matrix compatibility metadata")
    parser.add_argument("--candidate", dest="matrix_candidate", type=Path, help="candidate binary for matrix compatibility metadata")
    subparsers = parser.add_subparsers(dest="command", required=False)
    validate = subparsers.add_parser("validate", help="validate the contract and case input without executing a binary")
    validate.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    validate.add_argument("--cases", type=Path)
    validate.add_argument("--operation", action="append", default=[])
    validate.add_argument("--readiness-mode", choices=READINESS_MODES, default="comparison")
    validate.add_argument("--readiness-matrix", type=Path, dest="readiness_matrix_path")
    reference = subparsers.add_parser("verify-reference", help="verify an existing pristine 570 checkout")
    reference.add_argument("--checkout", type=Path, required=True)
    run = subparsers.add_parser("run", aliases=["compare"], help="run cases against independent original and product binaries")
    run.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    run.add_argument("--cases", type=Path, required=True)
    run.add_argument("--reference-checkout", type=Path, required=True)
    run.add_argument("--original-binary", type=Path, required=True)
    run.add_argument("--product-binary", type=Path, required=True)
    run.add_argument("--artifact-root", type=Path, required=True)
    run.add_argument("--operation", action="append", default=[])
    run.add_argument(
        "--readiness-mode",
        choices=READINESS_MODES,
        default="comparison",
        help="readiness rejects filtered or incomplete route suites; comparison permits focused cases",
    )
    run.add_argument("--readiness-matrix", type=Path, dest="readiness_matrix_path")
    run.add_argument("--run-id")
    run.add_argument("--timeout-seconds", type=float, default=120.0)
    run.add_argument("--terminate-seconds", type=float, default=2.0)
    run.add_argument("--kill-seconds", type=float, default=2.0)
    run.add_argument(
        "--product-source-revision",
        help="attested candidate source revision; omitted means read from --product-source-root",
    )
    run.add_argument(
        "--product-source-root",
        type=Path,
        help="candidate checkout source root; defaults to the checkout containing runner.py",
    )
    run.add_argument(
        "--original-build-attestation",
        type=Path,
        required=True,
        help="build-owner JSON attestation binding the 570 binary to the verified reference checkout",
    )
    run.add_argument(
        "--product-build-attestation",
        type=Path,
        required=True,
        help="build-owner JSON attestation binding the candidate binary to its source revision",
    )
    run.add_argument(
        "--ncm-worker-attestation",
        type=Path,
        help="runner-owned trusted NCM worker manifest; required for normal NCM worker execution",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command is None:
        if args.matrix_case is None or args.matrix_ledger is None:
            parser.error("a command or both --case and --ledger are required")
        return _run_matrix_compatibility(
            row_id=args.matrix_case,
            ledger_path=args.matrix_ledger,
            matrix_path=args.matrix_path,
            reference_binary=args.matrix_reference,
            candidate_binary=args.matrix_candidate,
        )
    if args.matrix_case is not None or args.matrix_ledger is not None:
        parser.error("--case/--ledger compatibility options cannot be combined with a subcommand")
    if args.command == "verify-reference":
        print(json.dumps(verify_reference_checkout(args.checkout), sort_keys=True))
        return 0
    contract = load_contract(args.contract)
    if args.command == "validate":
        if args.readiness_mode == "readiness" and not args.cases:
            raise RunnerError(
                "readiness validation requires --cases containing every frozen matrix row"
            )
        if args.cases:
            cases = validate_readiness_selection(
                load_cases(args.cases),
                args.operation,
                readiness_mode=args.readiness_mode,
                readiness_matrix=(
                    load_readiness_matrix(args.readiness_matrix_path)
                    if args.readiness_mode == "readiness"
                    else None
                ),
            )
            for case in cases:
                validate_case(case, contract)
            print(json.dumps({"status": "pass", "cases": len(cases), "contract": contract["contract_id"]}, sort_keys=True))
        else:
            print(json.dumps({"status": "pass", "contract": contract["contract_id"]}, sort_keys=True))
        return 0
    cases = load_cases(args.cases)
    report = run_suite(
        contract=contract,
        cases=cases,
        reference_checkout=args.reference_checkout,
        original_binary=args.original_binary,
        product_binary=args.product_binary,
        artifact_root=args.artifact_root,
        required_operations=args.operation,
        timeouts=ProcessTimeouts(args.timeout_seconds, args.terminate_seconds, args.kill_seconds),
        run_id=args.run_id,
        product_source_revision=args.product_source_revision,
        product_source_root=args.product_source_root,
        original_build_attestation=args.original_build_attestation,
        product_build_attestation=args.product_build_attestation,
        ncm_worker_attestation=args.ncm_worker_attestation,
        readiness_mode=args.readiness_mode,
        readiness_matrix_path=args.readiness_matrix_path,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["status"] == "pass" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RunnerError as error:
        print(f"native-original runner: {error}", file=sys.stderr)
        raise SystemExit(2)
