#!/usr/bin/env python3
"""Measure the pinned Biomem/NCM surface without claiming conformance.

The probe deliberately separates three kinds of evidence:

* source observations from an immutable Git revision;
* real HTTP fallback behavior with a synthetic command handler; and
* optional model-backed ``TextMemory`` behavior in an isolated child process.

Every measurement is classified as ``measured``, ``blocked``, or
``unsupported``.  A source symbol, a successful import, or a HTTP response is
never treated as proof that the provider contract is satisfied.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import concurrent.futures
import contextlib
import http.client
import importlib
import importlib.util
import inspect
import io
import ipaddress
import json
import math
import os
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import types
import urllib.request
from pathlib import Path, PurePosixPath
from typing import Any, Callable, NoReturn, Sequence


SCHEMA_VERSION = 2
PROBE_ID = "tracedecay.ncm.surface-probe.v2"
REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
AVAILABILITIES = frozenset(("measured", "blocked", "unsupported"))
CONCURRENCY_LEVELS = (1, 2, 4, 8)
MAX_DIAGNOSTIC_CHARS = 600
MAX_JSON_STRING_CHARS = 1_000
MAX_JSON_ITEMS = 64
MAX_HTTP_RESPONSE_BYTES = 64 * 1024
MAX_ARCHIVE_BYTES = 128 * 1024 * 1024
HTTP_DELAY_SECONDS = 0.075
DISCONNECT_DELAY_SECONDS = 0.2
SURFACE_PROBES = (
    "python_syntax",
    "callable_surface_inventory",
    "http_health_identity",
    "http_parallel_requests",
    "client_disconnect",
)
HTTP_PROBES = SURFACE_PROBES[2:]
CORE_PROBES = (
    "health_load_state_identity",
    "observation_retry_effects",
    "bounded_recall",
    "core_parallel_operations",
    "cancellation_deadline_observation",
    "cross_scope_leakage",
    "restart_equivalence",
    "interrupted_save_restore_incompatibility",
)
PROBE_SEQUENCE = SURFACE_PROBES + CORE_PROBES
CORE_REQUIREMENTS = {
    "health_load_state_identity": ("get_stats", "load"),
    "observation_retry_effects": ("store_record", "get_stats", "list_memories"),
    "bounded_recall": ("search",),
    "core_parallel_operations": ("search", "store_record"),
    "cancellation_deadline_observation": ("search",),
    "cross_scope_leakage": ("store_record", "list_memories"),
    "restart_equivalence": ("save", "load", "list_memories"),
    "interrupted_save_restore_incompatibility": ("restore", "list_memories"),
}


class ProbeError(RuntimeError):
    """The probe could not establish trustworthy input or execution state."""


class DiscardingTextSink(io.TextIOBase):
    """Bound dependency chatter by discarding it instead of buffering it."""

    def write(self, value: str) -> int:
        return len(value)

    def flush(self) -> None:
        return None


def fail(message: str) -> NoReturn:
    raise ProbeError(message)


def minimal_child_environment() -> dict[str, str]:
    allowed = (
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "PATH",
        "SYSTEMROOT",
        "TMPDIR",
        "WINDIR",
    )
    return {key: os.environ[key] for key in allowed if key in os.environ}


def install_loopback_network_guard() -> None:
    """Deny Python socket connections outside loopback in internal workers."""

    def ensure_loopback(address: object) -> None:
        if not isinstance(address, tuple) or not address:
            return
        host = str(address[0]).split("%", 1)[0]
        if host.lower() == "localhost":
            return
        try:
            if ipaddress.ip_address(host).is_loopback:
                return
        except ValueError:
            pass
        raise OSError("NCM probe internal workers deny non-loopback network access")

    original_socket = socket.socket
    original_create_connection = socket.create_connection

    class GuardedSocket(original_socket):
        def connect(self, address: object) -> None:
            ensure_loopback(address)
            return super().connect(address)

        def connect_ex(self, address: object) -> int:
            ensure_loopback(address)
            return super().connect_ex(address)

    def guarded_create_connection(
        address: tuple[str, int],
        timeout: float | object = socket._GLOBAL_DEFAULT_TIMEOUT,
        source_address: tuple[str, int] | None = None,
        *,
        all_errors: bool = False,
    ) -> socket.socket:
        ensure_loopback(address)
        return original_create_connection(
            address,
            timeout,
            source_address,
            all_errors=all_errors,
        )

    socket.socket = GuardedSocket
    socket.create_connection = guarded_create_connection


def bounded_text(value: object, limit: int = MAX_DIAGNOSTIC_CHARS) -> str:
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def redact_text(value: object, replacements: Sequence[tuple[str, str]]) -> str:
    result = str(value)
    for raw, replacement in replacements:
        if raw:
            result = result.replace(raw, replacement)
    return bounded_text(result)


def bounded_json(value: Any, *, depth: int = 0) -> Any:
    """Return a deterministic, bounded JSON-compatible observation."""
    if depth >= 8:
        return "<maximum-depth>"
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, str):
        return bounded_text(value, MAX_JSON_STRING_CHARS)
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        entries = sorted(
            value.items(),
            key=lambda item: (
                bounded_text(item[0], 120),
                type(item[0]).__name__,
                repr(item[0]),
            ),
        )
        for key, item in entries[:MAX_JSON_ITEMS]:
            normalized = bounded_text(key, 120)
            candidate = normalized
            suffix = 2
            while candidate in result:
                candidate = bounded_text(f"{normalized}#{suffix}", 120)
                suffix += 1
            result[candidate] = bounded_json(item, depth=depth + 1)
        if len(value) > MAX_JSON_ITEMS:
            result["_truncated_items"] = len(value) - MAX_JSON_ITEMS
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        items = (
            sorted(value, key=lambda item: (type(item).__name__, repr(item)))
            if isinstance(value, (set, frozenset))
            else list(value)
        )
        result = [
            bounded_json(item, depth=depth + 1) for item in items[:MAX_JSON_ITEMS]
        ]
        if len(items) > MAX_JSON_ITEMS:
            result.append({"_truncated_items": len(items) - MAX_JSON_ITEMS})
        return result
    return bounded_text(repr(value), MAX_JSON_STRING_CHARS)


def measurement(
    probe_id: str,
    availability: str,
    *,
    claim_scope: str,
    expectation: str,
    observed: dict[str, Any] | None = None,
    diagnostic: str | None = None,
    elapsed_ms: int | None = None,
) -> dict[str, Any]:
    if probe_id not in PROBE_SEQUENCE:
        fail(f"invalid measurement probe_id: {probe_id!r}")
    if availability not in AVAILABILITIES:
        fail(f"invalid measurement availability: {availability}")
    if not isinstance(claim_scope, str) or not claim_scope.strip():
        fail(f"measurement {probe_id!r} has an invalid claim_scope")
    if not isinstance(expectation, str) or not expectation.strip():
        fail(f"measurement {probe_id!r} has an invalid expectation")
    if elapsed_ms is not None and (
        isinstance(elapsed_ms, bool)
        or not isinstance(elapsed_ms, int)
        or elapsed_ms < 0
    ):
        fail(f"measurement {probe_id!r} has an invalid elapsed_ms")
    if availability == "measured":
        if not isinstance(observed, dict) or not observed:
            fail(f"measured probe {probe_id!r} must carry a non-empty observation")
        if diagnostic is not None:
            fail(f"measured probe {probe_id!r} must not carry a diagnostic")
    else:
        if observed is not None:
            fail(f"{availability} probe {probe_id!r} must not carry an observation")
        if not isinstance(diagnostic, str) or not diagnostic.strip():
            fail(f"{availability} probe {probe_id!r} must carry a diagnostic")
    return {
        "probe_id": probe_id,
        "availability": availability,
        "claim_scope": claim_scope,
        "expectation": bounded_text(expectation, MAX_JSON_STRING_CHARS),
        "observed": bounded_json(observed) if observed is not None else None,
        "diagnostic": bounded_text(diagnostic) if diagnostic else None,
        "elapsed_ms": elapsed_ms,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Emit bounded JSON measurements for a pinned Biomem/NCM surface.",
    )
    parser.add_argument(
        "--biomem-repo",
        "--source",
        dest="biomem_repo",
        type=Path,
        required=True,
        help="Local Biomem Git repository used only as an immutable object source.",
    )
    parser.add_argument(
        "--expected-revision",
        "--expected-immutable-revision",
        dest="expected_revision",
        required=True,
        help="Exact 40-character lowercase Git commit to probe.",
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        help="Existing caller-owned directory beneath which an isolated child is created.",
    )
    parser.add_argument(
        "--model-cache",
        type=Path,
        help="Optional caller-owned offline model cache for model-backed probes.",
    )
    parser.add_argument(
        "--core-mode",
        choices=("auto", "skip"),
        default="auto",
        help="Attempt isolated model-backed probes, or report them as blocked.",
    )
    parser.add_argument(
        "--core-timeout-seconds",
        type=int,
        default=120,
        help="Hard deadline for the complete model-backed child (5..600 seconds).",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=3.0,
        help="Per-request HTTP probe timeout (0.1..30 seconds).",
    )
    parser.add_argument(
        "--max-recall-results",
        type=int,
        default=8,
        help="Maximum result bound exercised by recall probes (1..32).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit compact JSON instead of indented JSON.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> tuple[Path, Path | None, Path | None]:
    repo = args.biomem_repo.resolve()
    if not repo.is_dir() or not (repo / ".git").exists():
        fail("--biomem-repo must be a local Git checkout")
    if not REVISION_RE.fullmatch(args.expected_revision):
        fail("--expected-revision must be a 40-character lowercase hexadecimal commit")
    if not 5 <= args.core_timeout_seconds <= 600:
        fail("--core-timeout-seconds must be between 5 and 600")
    if not 0.1 <= args.request_timeout_seconds <= 30:
        fail("--request-timeout-seconds must be between 0.1 and 30")
    if not math.isfinite(args.request_timeout_seconds):
        fail("--request-timeout-seconds must be finite")
    if not 1 <= args.max_recall_results <= 32:
        fail("--max-recall-results must be between 1 and 32")

    state_root = args.state_root.resolve() if args.state_root else None
    if state_root is not None and not state_root.is_dir():
        fail("--state-root must name an existing directory")
    model_cache = args.model_cache.resolve() if args.model_cache else None
    if model_cache is not None and not model_cache.is_dir():
        fail("--model-cache must name an existing directory")
    return repo, state_root, model_cache


def run_git(
    repo: Path, arguments: Sequence[str], *, binary: bool = False
) -> str | bytes:
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    }
    environment.update(
        {
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_NO_LAZY_FETCH": "1",
        }
    )
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            check=False,
            capture_output=True,
            text=not binary,
            timeout=60,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        fail(f"git command could not complete: {bounded_text(exc)}")
    if result.returncode != 0:
        raw = result.stderr or result.stdout or b"git returned no diagnostic"
        if isinstance(raw, bytes):
            detail = raw.decode("utf-8", errors="replace")
        else:
            detail = raw
        fail(f"git command failed: {bounded_text(detail)}")
    return result.stdout


def resolve_revision(repo: Path, expected_revision: str) -> str:
    resolved = str(
        run_git(repo, ["rev-parse", "--verify", f"{expected_revision}^{{commit}}"])
    ).strip()
    if resolved != expected_revision:
        fail("Git resolved a commit other than --expected-revision")
    return resolved


def materialize_source(repo: Path, revision: str, destination: Path) -> None:
    archive = run_git(
        repo,
        ["archive", "--format=tar", revision, "--", "src"],
        binary=True,
    )
    if not isinstance(archive, bytes):
        fail("Git archive unexpectedly returned text")
    if len(archive) > MAX_ARCHIVE_BYTES:
        fail(f"Git source archive exceeds the {MAX_ARCHIVE_BYTES}-byte safety bound")
    destination.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as bundle:
            members = bundle.getmembers()
            for member in members:
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts:
                    fail("Git archive contains an unsafe path")
                if not (member.isdir() or member.isfile()):
                    fail("Git archive contains a non-regular source entry")
            for member in members:
                target = destination.joinpath(*PurePosixPath(member.name).parts)
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                source = bundle.extractfile(member)
                if source is None:
                    fail("Git archive source entry could not be read")
                with source, target.open("wb") as output:
                    shutil.copyfileobj(source, output)
    except (OSError, tarfile.TarError) as exc:
        fail(f"could not materialize immutable source: {bounded_text(exc)}")


def make_tree_read_only(root: Path) -> None:
    for path in sorted(root.rglob("*"), reverse=True):
        path.chmod(0o555 if path.is_dir() else 0o444)
    root.chmod(0o555)


def syntax_measurement(source_root: Path) -> dict[str, Any]:
    started = time.monotonic()
    files = sorted((source_root / "src").rglob("*.py"))
    errors: list[dict[str, Any]] = []
    for path in files:
        relative = path.relative_to(source_root).as_posix()
        try:
            compile(path.read_bytes(), relative, "exec", dont_inherit=True)
        except (OSError, SyntaxError, UnicodeError) as exc:
            errors.append(
                {
                    "path": relative,
                    "line": getattr(exc, "lineno", None),
                    "error_type": type(exc).__name__,
                    "diagnostic": bounded_text(exc, 240),
                }
            )
    return measurement(
        "python_syntax",
        "measured",
        claim_scope="immutable_biomem_python_source",
        expectation="Every Python source file compiles without writing bytecode.",
        observed={
            "files_checked": len(files),
            "errors": errors,
            "error_count": len(errors),
        },
        elapsed_ms=round((time.monotonic() - started) * 1_000),
    )


def class_methods(
    path: Path, class_name: str
) -> tuple[dict[str, list[str]], str | None]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.name)
    except (OSError, SyntaxError, UnicodeError) as exc:
        return {}, bounded_text(f"{type(exc).__name__}: {exc}", 240)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            methods: dict[str, list[str]] = {}
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    arguments = [
                        argument.arg
                        for argument in (*item.args.posonlyargs, *item.args.args)
                    ]
                    if item.args.vararg is not None:
                        arguments.append(f"*{item.args.vararg.arg}")
                    arguments.extend(argument.arg for argument in item.args.kwonlyargs)
                    if item.args.kwarg is not None:
                        arguments.append(f"**{item.args.kwarg.arg}")
                    methods[item.name] = arguments
            return methods, None
    return {}, f"class {class_name} is not declared in {path.name}"


def source_inventory(source_root: Path) -> tuple[dict[str, list[str]], dict[str, Any]]:
    text_memory = source_root / "src/memory_module/text_memory.py"
    http_fallback = source_root / "src/memory_module/http_fallback.py"
    methods, text_memory_diagnostic = class_methods(text_memory, "TextMemory")
    http_methods, http_diagnostic = class_methods(http_fallback, "BDBMHTTPHandler")
    required = sorted(
        {method for values in CORE_REQUIREMENTS.values() for method in values}
    )
    observed = {
        "inventory_kind": "declared_methods_only",
        "text_memory_parse_diagnostic": text_memory_diagnostic,
        "http_parse_diagnostic": http_diagnostic,
        "text_memory_methods": {
            method: {
                "present": method in methods,
                "parameters": methods.get(method, []),
            }
            for method in required
        },
        "http_methods": {
            method: {
                "present": method in http_methods,
                "parameters": http_methods.get(method, []),
            }
            for method in (
                "_handle_quick_status",
                "_submit_command",
                "do_GET",
                "do_POST",
            )
        },
        "cancellation_parameter_present": any(
            "cancel" in parameter.lower()
            for method in required
            for parameter in methods.get(method, [])
        ),
        "deadline_parameter_present": any(
            "deadline" in parameter.lower() or "timeout" in parameter.lower()
            for method in required
            for parameter in methods.get(method, [])
        ),
    }
    return methods, measurement(
        "callable_surface_inventory",
        "measured",
        claim_scope="immutable_biomem_source_signatures",
        expectation="Required probe entrypoints are inventoried without treating presence as support.",
        observed=observed,
        elapsed_ms=None,
    )


def package_version(source_root: Path) -> str:
    init_path = source_root / "src/memory_module/__init__.py"
    try:
        tree = ast.parse(init_path.read_text(encoding="utf-8"), filename=init_path.name)
    except (OSError, SyntaxError, UnicodeError):
        return "unknown"
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in node.targets
        ):
            continue
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            return node.value.value
    return "unknown"


def load_http_surface(source_root: Path) -> types.ModuleType:
    package = types.ModuleType("memory_module")
    package.__path__ = [str(source_root / "src/memory_module")]
    package.__package__ = "memory_module"
    package.__version__ = package_version(source_root)

    protocol = types.ModuleType("memory_module.protocol")
    protocol.CommandHandler = object
    security = types.ModuleType("memory_module.security")
    security.SecurityManager = object

    for name in tuple(sys.modules):
        if name == "memory_module" or name.startswith("memory_module."):
            del sys.modules[name]
    sys.modules["memory_module"] = package
    sys.modules["memory_module.protocol"] = protocol
    sys.modules["memory_module.security"] = security

    path = source_root / "src/memory_module/http_fallback.py"
    spec = importlib.util.spec_from_file_location("memory_module.http_fallback", path)
    if spec is None or spec.loader is None:
        fail("could not create an import specification for HTTPFallbackServer")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class SyntheticCommandHandler:
    """A bounded handler used only to measure the actual HTTP transport."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.delay_seconds = 0.0
        self.active = 0
        self.max_active = 0
        self.started = 0
        self.completed = 0
        self.cancelled = 0
        self.started_event = threading.Event()
        self.completed_event = threading.Event()

    def reset(self, delay_seconds: float) -> None:
        with self._lock:
            if self.active:
                raise ProbeError("synthetic handler reset while requests were active")
            self.delay_seconds = delay_seconds
            self.max_active = 0
            self.started = 0
            self.completed = 0
            self.cancelled = 0
            self.started_event = threading.Event()
            self.completed_event = threading.Event()

    async def handle(self, message: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self.active += 1
            self.started += 1
            self.max_active = max(self.max_active, self.active)
            self.started_event.set()
        try:
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)
            if message.get("command") == "status":
                result = {"status": "success", "stats": {"writes": 0, "reads": 0}}
            else:
                result = {
                    "status": "success",
                    "command": str(message.get("command", ""))[:80],
                }
        except asyncio.CancelledError:
            with self._lock:
                self.cancelled += 1
            raise
        else:
            with self._lock:
                self.completed += 1
                self.completed_event.set()
            return result
        finally:
            with self._lock:
                self.active -= 1


class SyntheticSecurity:
    def is_allowed_origin(self, _origin: str | None) -> bool:
        return True


def http_json(
    method: str,
    url: str,
    *,
    timeout_seconds: float,
    payload: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {} if body is None else {"Content-Type": "application/json"}
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    # Do not let ambient proxy variables route a loopback-only probe elsewhere.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=timeout_seconds) as response:
        raw = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
        if len(raw) > MAX_HTTP_RESPONSE_BYTES:
            fail("HTTP probe response exceeded the bounded response limit")
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            fail("HTTP probe response was not a JSON object")
        return response.status, value


def execute_http_worker_measurements(
    source_root: Path,
    request_timeout_seconds: float,
    replacements: Sequence[tuple[str, str]],
) -> list[dict[str, Any]]:
    started_all = time.monotonic()
    try:
        module = load_http_surface(source_root)
        # A disconnected probe intentionally causes a server-side write error.
        # Suppress socketserver's unbounded traceback without changing request logic.
        module.ThreadingHTTPServer.handle_error = lambda *_args, **_kwargs: None
        handler = SyntheticCommandHandler()
        server = module.HTTPFallbackServer(
            handler=handler,
            security=SyntheticSecurity(),
            host="127.0.0.1",
            port=0,
        )
        server.start()
    except Exception as exc:
        diagnostic = redact_text(exc, replacements)
        return [
            measurement(
                probe_id,
                "blocked",
                claim_scope="actual_biomem_http_transport_with_synthetic_command_handler",
                expectation=expectation,
                diagnostic=diagnostic,
                elapsed_ms=round((time.monotonic() - started_all) * 1_000),
            )
            for probe_id, expectation in (
                (
                    "http_health_identity",
                    "HTTP health must distinguish product liveness from loaded-state readiness and identity.",
                ),
                (
                    "http_parallel_requests",
                    "The HTTP transport is measured at 1, 2, 4, and 8 concurrent callers.",
                ),
                (
                    "client_disconnect",
                    "Client timeout/disconnect must be observed separately from server cancellation and effect completion.",
                ),
            )
        ]

    base_url = f"http://127.0.0.1:{server.bound_port}"
    results: list[dict[str, Any]] = []
    try:
        handler.reset(0.0)
        started = time.monotonic()
        status, payload = http_json(
            "GET",
            f"{base_url}/api/health",
            timeout_seconds=request_timeout_seconds,
        )
        identity_keys = (
            "implementation_identity",
            "implementation_identity_sha256",
            "state_schema_version",
            "state_generation",
            "config_identity",
            "loaded_state_identity",
        )
        present = sorted(key for key in identity_keys if key in payload)
        identity_complete = len(present) == len(identity_keys)
        results.append(
            measurement(
                "http_health_identity",
                "measured",
                claim_scope="actual_biomem_http_transport_with_synthetic_status_handler",
                expectation=(
                    "HTTP health must distinguish product liveness from loaded-state "
                    "readiness and expose build/config/state identity before claiming ready."
                ),
                observed={
                    "http_status": status,
                    "status": payload.get("status"),
                    "product": payload.get("product"),
                    "version": payload.get("version"),
                    "protocol_version": payload.get("protocol_version"),
                    "transport": payload.get("transport"),
                    "ready": payload.get("ready"),
                    "loaded_state_identity_fields_present": present,
                    "loaded_state_identity_complete": identity_complete,
                    "ready_without_loaded_state_identity": bool(payload.get("ready"))
                    and not identity_complete,
                    "handler_status_was_synthetic": True,
                },
                elapsed_ms=round((time.monotonic() - started) * 1_000),
            )
        )

        matrix: list[dict[str, Any]] = []
        for level in CONCURRENCY_LEVELS:
            handler.reset(HTTP_DELAY_SECONDS)
            started = time.monotonic()

            def invoke(ordinal: int) -> tuple[int, str | None]:
                try:
                    response_status, response = http_json(
                        "POST",
                        f"{base_url}/api",
                        timeout_seconds=request_timeout_seconds,
                        payload={"command": "probe_delay", "ordinal": ordinal},
                    )
                    return response_status, str(response.get("status"))
                except Exception as exc:
                    return 0, bounded_text(type(exc).__name__, 80)

            with concurrent.futures.ThreadPoolExecutor(max_workers=level) as executor:
                responses = list(executor.map(invoke, range(level)))
            elapsed_ms = round((time.monotonic() - started) * 1_000)
            completed = sum(
                status == 200 and result == "success" for status, result in responses
            )
            matrix.append(
                {
                    "parallel_requests": level,
                    "attempted": level,
                    "completed": completed,
                    "errors": level - completed,
                    "elapsed_ms": elapsed_ms,
                    "max_active": handler.max_active,
                }
            )
        results.append(
            measurement(
                "http_parallel_requests",
                "measured",
                claim_scope="actual_biomem_http_transport_with_bounded_synthetic_handler",
                expectation=(
                    "Measure transport-level concurrency without inferring that the real "
                    "model or persistence layer executes concurrently."
                ),
                observed={
                    "matrix": matrix,
                    "concurrency_levels": list(CONCURRENCY_LEVELS),
                    "real_memory_backend_used": False,
                },
                elapsed_ms=sum(row["elapsed_ms"] for row in matrix),
            )
        )

        handler.reset(DISCONNECT_DELAY_SECONDS)
        connection = http.client.HTTPConnection(
            "127.0.0.1",
            server.bound_port,
            timeout=min(0.03, request_timeout_seconds),
        )
        timeout_seen = False
        disconnect_started = time.monotonic()
        try:
            body = json.dumps({"command": "probe_delay"})
            connection.request(
                "POST",
                "/api",
                body=body,
                headers={"Content-Type": "application/json"},
            )
            connection.getresponse()
        except (TimeoutError, socket.timeout):
            timeout_seen = True
        finally:
            connection.close()
        client_elapsed_ms = round((time.monotonic() - disconnect_started) * 1_000)
        completion_started = time.monotonic()
        completed_after_disconnect = handler.completed_event.wait(
            timeout=max(request_timeout_seconds, DISCONNECT_DELAY_SECONDS * 3)
        )
        completion_wait_ms = round((time.monotonic() - completion_started) * 1_000)
        results.append(
            measurement(
                "client_disconnect",
                "measured",
                claim_scope="actual_biomem_http_transport_with_bounded_synthetic_handler",
                expectation=(
                    "A client timeout must not be reported as server-side cancellation; "
                    "completion after disconnect remains an unknown-effect risk for mutations."
                ),
                observed={
                    "timeout_seen": timeout_seen,
                    "elapsed_ms": client_elapsed_ms,
                    "server_completed_after_disconnect": completed_after_disconnect,
                    "server_observed_cancellation": handler.cancelled > 0,
                    "server_completion_wait_ms": completion_wait_ms,
                    "handler_started": handler.started,
                    "handler_completed": handler.completed,
                    "handler_cancelled": handler.cancelled,
                },
                elapsed_ms=client_elapsed_ms + completion_wait_ms,
            )
        )
    except Exception as exc:
        completed_ids = {result["probe_id"] for result in results}
        diagnostic = redact_text(exc, replacements)
        for probe_id, expectation in (
            (
                "http_health_identity",
                "HTTP health must distinguish liveness from loaded-state readiness.",
            ),
            (
                "http_parallel_requests",
                "Measure the HTTP transport at 1, 2, 4, and 8 concurrent callers.",
            ),
            (
                "client_disconnect",
                "Observe client timeout separately from server cancellation and completion.",
            ),
        ):
            if probe_id not in completed_ids:
                results.append(
                    measurement(
                        probe_id,
                        "blocked",
                        claim_scope="actual_biomem_http_transport_with_synthetic_command_handler",
                        expectation=expectation,
                        diagnostic=diagnostic,
                        elapsed_ms=None,
                    )
                )
    finally:
        with contextlib.suppress(Exception):
            server.stop()
    return results


def blocked_http_measurements(
    diagnostic: str, elapsed_ms: int | None = None
) -> list[dict[str, Any]]:
    return [
        measurement(
            probe_id,
            "blocked",
            claim_scope="actual_biomem_http_transport_with_synthetic_command_handler",
            expectation=expectation,
            diagnostic=diagnostic,
            elapsed_ms=elapsed_ms,
        )
        for probe_id, expectation in (
            (
                "http_health_identity",
                "HTTP health must distinguish liveness from loaded-state readiness.",
            ),
            (
                "http_parallel_requests",
                "Measure the HTTP transport at 1, 2, 4, and 8 concurrent callers.",
            ),
            (
                "client_disconnect",
                "Observe client timeout separately from server cancellation and completion.",
            ),
        )
    ]


def http_surface_measurements(
    source_root: Path,
    request_timeout_seconds: float,
    replacements: Sequence[tuple[str, str]],
) -> list[dict[str, Any]]:
    config = {
        "source_root": str(source_root),
        "request_timeout_seconds": request_timeout_seconds,
        "replacements": [list(item) for item in replacements],
    }
    started = time.monotonic()
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-S",
                str(Path(__file__).resolve()),
                "--internal-http-worker",
            ],
            input=json.dumps(config, allow_nan=False),
            check=False,
            capture_output=True,
            text=True,
            timeout=max(15.0, request_timeout_seconds * 5),
            env=minimal_child_environment(),
            cwd=source_root,
        )
    except subprocess.TimeoutExpired:
        return blocked_http_measurements(
            "HTTP probe child exceeded its hard deadline",
            round((time.monotonic() - started) * 1_000),
        )
    except OSError as exc:
        return blocked_http_measurements(redact_text(exc, replacements))

    marker = "__TRACEDECAY_NCM_HTTP_JSON__"
    payload_line = next(
        (
            line[len(marker) :]
            for line in reversed(result.stdout.splitlines())
            if line.startswith(marker)
        ),
        None,
    )
    if payload_line is None:
        detail = result.stderr or result.stdout or f"child exited {result.returncode}"
        return blocked_http_measurements(redact_text(detail, replacements))
    try:
        payload = json.loads(payload_line)
    except json.JSONDecodeError as exc:
        return blocked_http_measurements(redact_text(exc, replacements))
    if not isinstance(payload, dict) or not payload.get("initialized"):
        diagnostic = (
            payload.get("diagnostic")
            if isinstance(payload, dict)
            else "invalid child output"
        )
        return blocked_http_measurements(redact_text(diagnostic, replacements))
    raw_measurements = payload.get("measurements")
    if not isinstance(raw_measurements, list):
        return blocked_http_measurements("HTTP probe child omitted measurements")
    raw_ids = tuple(
        raw.get("probe_id") if isinstance(raw, dict) else None
        for raw in raw_measurements
    )
    if raw_ids != HTTP_PROBES:
        return blocked_http_measurements(
            "HTTP probe child emitted duplicate, unknown, missing, or reordered measurements"
        )
    results: list[dict[str, Any]] = []
    for raw in raw_measurements:
        if not isinstance(raw, dict):
            return blocked_http_measurements(
                "HTTP probe child emitted a non-object measurement"
            )
        availability = raw.get("availability")
        if availability not in AVAILABILITIES:
            return blocked_http_measurements(
                "HTTP probe child emitted an invalid availability"
            )
        probe_id = raw.get("probe_id")
        raw_observed = raw.get("observed")
        raw_diagnostic = raw.get("diagnostic")
        raw_elapsed = raw.get("elapsed_ms")
        if raw_elapsed is not None and (
            isinstance(raw_elapsed, bool)
            or not isinstance(raw_elapsed, int)
            or raw_elapsed < 0
        ):
            return blocked_http_measurements(
                "HTTP probe child emitted an invalid elapsed_ms"
            )
        if availability == "measured":
            if not isinstance(raw_observed, dict) or not raw_observed or raw_diagnostic:
                return blocked_http_measurements(
                    "HTTP probe child emitted an invalid measured envelope"
                )
        elif (
            raw_observed is not None
            or not isinstance(raw_diagnostic, str)
            or not raw_diagnostic
        ):
            return blocked_http_measurements(
                "HTTP probe child emitted an invalid unavailable envelope"
            )
        results.append(
            measurement(
                probe_id,
                availability,
                claim_scope=str(
                    raw.get("claim_scope") or "actual_biomem_http_transport"
                ),
                expectation=str(
                    raw.get("expectation") or "Measure actual HTTP transport behavior."
                ),
                observed=raw_observed if availability == "measured" else None,
                diagnostic=(
                    redact_text(raw_diagnostic, replacements)
                    if raw_diagnostic
                    else None
                ),
                elapsed_ms=raw_elapsed,
            )
        )
    order = {
        "http_health_identity": 0,
        "http_parallel_requests": 1,
        "client_disconnect": 2,
    }
    return sorted(results, key=lambda item: order[item["probe_id"]])


def core_blocked_measurements(
    methods: dict[str, list[str]],
    diagnostic: str,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for probe_id in CORE_PROBES:
        missing = [
            method for method in CORE_REQUIREMENTS[probe_id] if method not in methods
        ]
        if missing:
            availability = "blocked"
            reason = (
                "required TextMemory callables were not declared in the inspected class: "
                f"{', '.join(missing)}; runtime inheritance was not initialized"
            )
        else:
            availability = "blocked"
            reason = diagnostic
        results.append(
            measurement(
                probe_id,
                availability,
                claim_scope="actual_biomem_text_memory",
                expectation=core_expectation(probe_id),
                diagnostic=reason,
                elapsed_ms=None,
            )
        )
    return results


def core_probe_id_diagnostic(prefix: str, observed_ids: Sequence[object]) -> str:
    observed_strings = [value for value in observed_ids if isinstance(value, str)]
    missing = [probe_id for probe_id in CORE_PROBES if probe_id not in observed_strings]
    unknown = [probe_id for probe_id in observed_strings if probe_id not in CORE_PROBES]
    duplicates = sorted(
        {
            probe_id
            for probe_id in observed_strings
            if observed_strings.count(probe_id) > 1
        }
    )
    invalid_positions = [
        index
        for index, probe_id in enumerate(observed_ids)
        if not isinstance(probe_id, str)
    ]
    detail = {
        "missing": missing,
        "unknown": unknown,
        "duplicates": duplicates,
        "invalid_positions": invalid_positions,
        "expected": list(CORE_PROBES),
        "received": list(observed_ids),
    }
    return bounded_text(
        f"{prefix}: {json.dumps(bounded_json(detail), sort_keys=True, separators=(',', ':'))}"
    )


def canonical_core_measurements(
    measurements: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    observed_ids = tuple(measurement.get("probe_id") for measurement in measurements)
    if (
        len(observed_ids) != len(CORE_PROBES)
        or any(observed_ids.count(probe_id) != 1 for probe_id in CORE_PROBES)
        or any(probe_id not in CORE_PROBES for probe_id in observed_ids)
    ):
        raise ProbeError(
            core_probe_id_diagnostic(
                "internal core worker produced an invalid measurement ID set",
                observed_ids,
            )
        )
    by_id = {measurement["probe_id"]: measurement for measurement in measurements}
    return [by_id[probe_id] for probe_id in CORE_PROBES]


def core_expectation(probe_id: str) -> str:
    return {
        "health_load_state_identity": (
            "Health/load evidence exposes actual loaded-state, build, config, schema, and generation identity."
        ),
        "observation_retry_effects": (
            "An identical observation retry is measured for effect count and reinforcement, not assumed idempotent."
        ),
        "bounded_recall": (
            "Side-effect-free search returns no more than the explicit result bound with stable IDs and provenance."
        ),
        "core_parallel_operations": (
            "Actual TextMemory reads and writes are measured at 1, 2, 4, and 8 concurrent callers."
        ),
        "cancellation_deadline_observation": (
            "Client timeout/cancel outcome is separated from server completion and committed-effect certainty."
        ),
        "cross_scope_leakage": (
            "Two explicit isolated state paths do not expose each other's admitted memory IDs."
        ),
        "restart_equivalence": (
            "A save/restart cycle preserves the admitted memory identity and bounded recall product."
        ),
        "interrupted_save_restore_incompatibility": (
            "Interrupted publication and incompatible restore fail explicitly without silent reset or false success."
        ),
    }[probe_id]


def run_core_child(
    source_root: Path,
    state_root: Path,
    model_cache: Path,
    methods: dict[str, list[str]],
    *,
    timeout_seconds: int,
    max_recall_results: int,
    replacements: Sequence[tuple[str, str]],
    interpreter: str | Path | None = None,
) -> list[dict[str, Any]]:
    config = {
        "source_root": str(source_root),
        "state_root": str(state_root),
        "model_cache": str(model_cache),
        "max_recall_results": max_recall_results,
    }
    hub_cache = model_cache / "hub"
    environment = {
        **minimal_child_environment(),
        "HF_HOME": str(model_cache),
        "HF_HUB_CACHE": str(hub_cache),
        "HUGGINGFACE_HUB_CACHE": str(hub_cache),
        "TORCH_HOME": str(model_cache / "torch"),
        "XDG_CACHE_HOME": str(model_cache / "xdg"),
        "HF_HUB_OFFLINE": "1",
        "TRANSFORMERS_OFFLINE": "1",
        "HF_DATASETS_OFFLINE": "1",
        "HF_HUB_DISABLE_TELEMETRY": "1",
        "TOKENIZERS_PARALLELISM": "false",
        "OMP_NUM_THREADS": "1",
        "HTTP_PROXY": "",
        "HTTPS_PROXY": "",
        "ALL_PROXY": "",
        "NO_PROXY": "127.0.0.1,localhost,::1",
    }
    try:
        result = subprocess.run(
            [
                str(interpreter or sys.executable),
                "-s",
                str(Path(__file__).resolve()),
                "--internal-core-worker",
            ],
            input=json.dumps(config, allow_nan=False),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=environment,
            cwd=source_root,
        )
    except subprocess.TimeoutExpired:
        return core_blocked_measurements(
            methods,
            f"model-backed child exceeded the {timeout_seconds}s hard deadline",
        )
    except OSError as exc:
        return core_blocked_measurements(methods, redact_text(exc, replacements))

    marker = "__TRACEDECAY_NCM_PROBE_JSON__"
    payload_line = next(
        (
            line[len(marker) :]
            for line in reversed(result.stdout.splitlines())
            if line.startswith(marker)
        ),
        None,
    )
    if payload_line is None:
        detail = result.stderr or result.stdout or f"child exited {result.returncode}"
        return core_blocked_measurements(methods, redact_text(detail, replacements))
    try:
        payload = json.loads(payload_line)
    except json.JSONDecodeError as exc:
        return core_blocked_measurements(methods, redact_text(exc, replacements))
    if not isinstance(payload, dict) or not payload.get("initialized"):
        diagnostic = (
            payload.get("diagnostic")
            if isinstance(payload, dict)
            else "invalid child output"
        )
        return core_blocked_measurements(methods, redact_text(diagnostic, replacements))

    raw_measurements = payload.get("measurements")
    if not isinstance(raw_measurements, list):
        return core_blocked_measurements(
            methods, "model-backed child omitted measurements"
        )
    raw_ids = tuple(
        value.get("probe_id") if isinstance(value, dict) else None
        for value in raw_measurements
    )
    if raw_ids != CORE_PROBES:
        return core_blocked_measurements(
            methods,
            core_probe_id_diagnostic(
                "model-backed child measurement ID sequence mismatch", raw_ids
            ),
        )
    by_id = {
        value.get("probe_id"): value
        for value in raw_measurements
        if isinstance(value, dict) and isinstance(value.get("probe_id"), str)
    }
    results: list[dict[str, Any]] = []
    for probe_id in CORE_PROBES:
        raw = by_id.get(probe_id)
        if not isinstance(raw, dict):
            return core_blocked_measurements(
                methods, "model-backed child emitted a non-object measurement"
            )
        availability = raw.get("availability")
        if availability not in AVAILABILITIES:
            return core_blocked_measurements(
                methods, "model-backed child emitted an invalid availability"
            )
        raw_observed = raw.get("observed")
        raw_diagnostic = raw.get("diagnostic")
        raw_elapsed = raw.get("elapsed_ms")
        if raw_elapsed is not None and (
            isinstance(raw_elapsed, bool)
            or not isinstance(raw_elapsed, int)
            or raw_elapsed < 0
        ):
            return core_blocked_measurements(
                methods, "model-backed child emitted an invalid elapsed_ms"
            )
        if availability == "measured":
            if not isinstance(raw_observed, dict) or not raw_observed or raw_diagnostic:
                return core_blocked_measurements(
                    methods, "model-backed child emitted an invalid measured envelope"
                )
        elif (
            raw_observed is not None
            or not isinstance(raw_diagnostic, str)
            or not raw_diagnostic
        ):
            return core_blocked_measurements(
                methods, "model-backed child emitted an invalid unavailable envelope"
            )
        results.append(
            measurement(
                probe_id,
                availability,
                claim_scope="actual_biomem_text_memory",
                expectation=core_expectation(probe_id),
                observed=raw_observed if availability == "measured" else None,
                diagnostic=(
                    redact_text(raw_diagnostic, replacements)
                    if raw_diagnostic
                    else None
                ),
                elapsed_ms=raw_elapsed,
            )
        )
    return results


def child_measure(
    probe_id: str,
    function: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    started = time.monotonic()
    try:
        observed = function()
    except Exception as exc:
        return {
            "probe_id": probe_id,
            "availability": "blocked",
            "observed": None,
            "diagnostic": bounded_text(f"{type(exc).__name__}: {exc}"),
            "elapsed_ms": round((time.monotonic() - started) * 1_000),
        }
    return {
        "probe_id": probe_id,
        "availability": "measured",
        "observed": bounded_json(observed),
        "diagnostic": None,
        "elapsed_ms": round((time.monotonic() - started) * 1_000),
    }


def execute_core_worker(config: dict[str, Any]) -> dict[str, Any]:
    source_root = Path(str(config["source_root"])).resolve()
    state_root = Path(str(config["state_root"])).resolve()
    max_recall_results = int(config["max_recall_results"])
    module_root = source_root / "src/memory_module"
    package = types.ModuleType("memory_module")
    package.__path__ = [str(module_root)]
    package.__package__ = "memory_module"
    package.__version__ = package_version(source_root)
    sys.modules["memory_module"] = package
    # Loading the exact core module through this synthetic package avoids
    # importing unrelated daemon/autostart surfaces from memory_module.__init__.
    text_memory_module = importlib.import_module("memory_module.text_memory")
    MemoryConfig = text_memory_module.MemoryConfig
    TextMemory = text_memory_module.TextMemory

    # TextMemory calls get_data_dir() even when state_file is explicit. Keep the
    # real implementation from touching its default user-global ~/.bdbm path.
    text_memory_module.get_data_dir = lambda _custom_dir="": state_root

    def new_memory(scope: str, *, auto_load: bool = False) -> Any:
        directory = state_root / scope
        directory.mkdir(parents=True, exist_ok=True)
        config_value = MemoryConfig(
            n_ltm_centers=64,
            n_stm_centers=32,
            terrain_resolution=8,
            auto_save=False,
            data_dir=str(directory),
            state_file="memory-state.bdbm",
        )
        return TextMemory(
            config=config_value,
            state_file=str(directory / "memory-state.bdbm"),
            device="cpu",
            auto_load=auto_load,
        )

    memory = new_memory("scope-a")
    measurements: list[dict[str, Any]] = []

    measurements.append(
        child_measure(
            "health_load_state_identity",
            lambda: _core_health(memory),
        )
    )
    measurements.append(
        child_measure(
            "observation_retry_effects",
            lambda: _core_retry(memory),
        )
    )
    measurements.append(
        child_measure(
            "bounded_recall",
            lambda: _core_recall(memory, max_recall_results),
        )
    )
    measurements.append(
        child_measure(
            "core_parallel_operations",
            lambda: _core_parallel(memory, max_recall_results),
        )
    )
    measurements.append(
        child_measure(
            "cross_scope_leakage",
            lambda: _core_scope(memory, new_memory("scope-b")),
        )
    )

    state_path = state_root / "scope-a/memory-state.bdbm"

    def restart() -> dict[str, Any]:
        before = _memory_ids(memory)
        before_recall = _recall_identity_product(memory, max_recall_results)
        memory.save(str(state_path))
        restarted = new_memory("scope-a", auto_load=True)
        after = _memory_ids(restarted)
        after_recall = _recall_identity_product(restarted, max_recall_results)
        return {
            "before_count": len(before),
            "after_count": len(after),
            "same_memory_ids": before == after,
            "missing_after_restart": sorted(set(before) - set(after)),
            "unexpected_after_restart": sorted(set(after) - set(before)),
            "bounded_recall_before": before_recall,
            "bounded_recall_after": after_recall,
            "same_bounded_recall_product": before_recall == after_recall,
        }

    measurements.append(child_measure("restart_equivalence", restart))

    def incompatible_restore() -> dict[str, Any]:
        corrupt = state_root / "scope-a/interrupted-save.bdbm"
        corrupt.write_bytes(b"BIOMEM-PROBE-TRUNCATED")
        before = _memory_ids(memory)
        raised = False
        error_type = None
        try:
            memory.restore(str(corrupt))
        except Exception as exc:
            raised = True
            error_type = type(exc).__name__
        after = _memory_ids(memory)
        return {
            "interrupted_save": {
                "availability": "blocked",
                "diagnostic": "no deterministic interruption hook exists; process-kill injection was not fabricated",
            },
            "incompatible_restore": {
                "availability": "measured",
                "raised": raised,
                "error_type": error_type,
                "state_unchanged": before == after,
                "before_count": len(before),
                "after_count": len(after),
            },
        }

    interrupted_restore = child_measure(
        "interrupted_save_restore_incompatibility", incompatible_restore
    )
    measurements.append(interrupted_restore)
    # Run the non-cancellable observation last. If the actual call outlives the
    # bounded wait, its daemon thread cannot stall or taint later probes.
    measurements.append(
        child_measure(
            "cancellation_deadline_observation",
            lambda: _core_cancel(memory, max_recall_results),
        )
    )
    return {
        "initialized": True,
        "measurements": canonical_core_measurements(measurements),
    }


def _memory_ids(memory: Any) -> list[str]:
    result = []
    for record in memory.list_memories(source="both", limit=64):
        memory_id = record.get("memory_id")
        if isinstance(memory_id, str):
            result.append(memory_id)
    return sorted(result)


def _recall_identity_product(memory: Any, limit: int) -> list[dict[str, Any]]:
    results = memory.search("ncm probe retry key", top_k=limit, source="both")
    return [
        {
            "memory_id": item.get("memory_id"),
            "key": item.get("key"),
            "value": item.get("value"),
            "source": item.get("source"),
        }
        for item in results
    ]


def _core_health(memory: Any) -> dict[str, Any]:
    state_path = Path(memory.state_file)
    state_existed_before_load = state_path.exists()
    load_result = memory.load()
    stats = memory.get_stats()
    expected_identity_fields = (
        "implementation_identity_sha256",
        "state_schema_version",
        "state_generation",
        "config_identity",
        "loaded_state_identity",
    )
    present = sorted(field for field in expected_identity_fields if field in stats)
    return {
        "stats_keys": sorted(stats),
        "identity_fields_present": present,
        "loaded_state_identity_complete": len(present) == len(expected_identity_fields),
        "load_invoked": True,
        "state_existed_before_load": state_existed_before_load,
        "load_result_type": type(load_result).__name__,
        "device": stats.get("device"),
    }


def _core_retry(memory: Any) -> dict[str, Any]:
    before = memory.get_stats()
    identity = "tdmem-probe-retry"
    first = memory.store_record(
        "ncm probe retry key",
        "ncm probe retry value",
        memory_id=identity,
        provenance={
            "source_class": "probe",
            "origin": "isolated",
            "session_id": "retry",
        },
    )
    middle = memory.get_stats()
    second = memory.store_record(
        "ncm probe retry key",
        "ncm probe retry value",
        memory_id=identity,
        provenance={
            "source_class": "probe",
            "origin": "isolated",
            "session_id": "retry",
        },
    )
    after = memory.get_stats()
    matching = [
        record
        for record in memory.list_memories(limit=64)
        if record.get("memory_id") == identity
    ]
    evidence_fields = sorted(
        {
            key
            for record in (first, second)
            if isinstance(record, dict)
            for key in record
            if "receipt" in str(key).lower() or "idempot" in str(key).lower()
        }
    )
    return {
        "attempted": 2,
        "completed": 2,
        "writes_before": before.get("writes"),
        "writes_after_first": middle.get("writes"),
        "writes_after_retry": after.get("writes"),
        "matching_memory_count": len(matching),
        "first_index": first.get("index") if isinstance(first, dict) else None,
        "retry_index": second.get("index") if isinstance(second, dict) else None,
        "provider_receipt_or_idempotency_fields_present": evidence_fields,
    }


def _core_recall(memory: Any, limit: int) -> dict[str, Any]:
    results = memory.search("ncm probe retry key", top_k=limit, source="both")
    return {
        "requested_top_k": limit,
        "returned": len(results),
        "bounded": len(results) <= limit,
        "memory_ids_present": sum(
            isinstance(item.get("memory_id"), str) for item in results
        ),
        "provenance_present": sum(
            isinstance(item.get("provenance"), dict) for item in results
        ),
        "native_scores_present": sum(
            isinstance(item.get("similarity"), (int, float)) for item in results
        ),
    }


def _run_parallel(level: int, function: Callable[[int], Any]) -> dict[str, Any]:
    active = 0
    max_active = 0
    lock = threading.Lock()

    def invoke(ordinal: int) -> tuple[bool, str | None]:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            function(ordinal)
            return True, None
        except Exception as exc:
            return False, type(exc).__name__
        finally:
            with lock:
                active -= 1

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=level) as executor:
        outcomes = list(executor.map(invoke, range(level)))
    return {
        "parallel_callers": level,
        "attempted": level,
        "completed": sum(ok for ok, _error in outcomes),
        "errors": sum(not ok for ok, _error in outcomes),
        "error_types": sorted({error for ok, error in outcomes if not ok and error}),
        "max_callers_inflight": max_active,
        "elapsed_ms": round((time.monotonic() - started) * 1_000),
    }


def _core_parallel(memory: Any, limit: int) -> dict[str, Any]:
    read_matrix = [
        _run_parallel(
            level,
            lambda _ordinal: memory.search(
                "ncm probe retry key", top_k=limit, source="both"
            ),
        )
        for level in CONCURRENCY_LEVELS
    ]
    write_matrix = [
        _run_parallel(
            level,
            lambda ordinal, level=level: memory.store_record(
                f"parallel write {level} {ordinal}",
                f"parallel value {level} {ordinal}",
                memory_id=f"tdmem-probe-parallel-{level}-{ordinal}",
                provenance={"source_class": "probe", "origin": "isolated"},
            ),
        )
        for level in CONCURRENCY_LEVELS
    ]
    return {
        "concurrency_levels": list(CONCURRENCY_LEVELS),
        "read_matrix": read_matrix,
        "write_matrix": write_matrix,
    }


def _core_cancel(memory: Any, limit: int) -> dict[str, Any]:
    started = time.monotonic()
    settled = threading.Event()
    outcome: dict[str, Any] = {}

    def invoke() -> None:
        try:
            result = memory.search("ncm probe retry key", limit, "both")
            outcome["returned"] = len(result)
        except Exception as exc:
            outcome["error_type"] = type(exc).__name__
        finally:
            settled.set()

    worker = threading.Thread(target=invoke, daemon=True, name="ncm-probe-deadline")
    worker.start()
    timeout_seen = not settled.wait(0.001)
    settled_after_timeout = timeout_seen and settled.wait(1.0)
    settled_now = settled.is_set()
    outcome_snapshot = dict(outcome) if settled_now else None
    parameters = inspect.signature(memory.search).parameters
    return {
        "cancellation_parameter_present": any(
            "cancel" in parameter.lower() for parameter in parameters
        ),
        "deadline_parameter_present": any(
            "deadline" in parameter.lower() or "timeout" in parameter.lower()
            for parameter in parameters
        ),
        "caller_wait_timeout_seen": timeout_seen,
        "operation_settled_after_caller_timeout": settled_after_timeout,
        "normal_return_observed": bool(
            outcome_snapshot is not None and "returned" in outcome_snapshot
        ),
        "error_observed": bool(
            outcome_snapshot is not None and "error_type" in outcome_snapshot
        ),
        "operation_still_running_after_followup_wait": timeout_seen
        and not settled_after_timeout,
        "provider_cancellation_observed": None,
        "committed_effect": None,
        "operation_kind": "read_only",
        "call_outcome": outcome_snapshot if outcome_snapshot else None,
        "elapsed_ms": round((time.monotonic() - started) * 1_000),
    }


def _core_scope(scope_a: Any, scope_b: Any) -> dict[str, Any]:
    identity = "tdmem-probe-scope-a-only"
    scope_a.store_record(
        "scope a private key",
        "scope a private value",
        memory_id=identity,
        provenance={"source_class": "probe", "origin": "scope-a"},
    )
    a_ids = _memory_ids(scope_a)
    b_ids = _memory_ids(scope_b)
    return {
        "scope_a_count": len(a_ids),
        "scope_b_count": len(b_ids),
        "scope_a_identity_present": identity in a_ids,
        "leaked_identity_count": int(identity in b_ids),
        "isolated_state_paths_used": True,
    }


def internal_http_worker() -> int:
    marker = "__TRACEDECAY_NCM_HTTP_JSON__"
    try:
        raw = sys.stdin.read(256 * 1024)
        config = json.loads(raw)
        if not isinstance(config, dict):
            raise ProbeError("HTTP worker configuration must be a JSON object")
        source_root = Path(str(config["source_root"])).resolve()
        request_timeout_seconds = float(config["request_timeout_seconds"])
        raw_replacements = config.get("replacements", [])
        replacements = tuple(
            (str(item[0]), str(item[1]))
            for item in raw_replacements
            if isinstance(item, list) and len(item) == 2
        )
        sys.dont_write_bytecode = True
        install_loopback_network_guard()
        sink = DiscardingTextSink()
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            measurements = execute_http_worker_measurements(
                source_root,
                request_timeout_seconds,
                replacements,
            )
        result = {"initialized": True, "measurements": measurements}
    except Exception as exc:
        result = {
            "initialized": False,
            "diagnostic": bounded_text(f"{type(exc).__name__}: {exc}"),
        }
    sys.stdout.write(
        marker
        + json.dumps(bounded_json(result), sort_keys=True, allow_nan=False)
        + "\n"
    )
    return 0


def internal_core_worker() -> int:
    marker = "__TRACEDECAY_NCM_PROBE_JSON__"
    try:
        raw = sys.stdin.read(256 * 1024)
        config = json.loads(raw)
        if not isinstance(config, dict):
            raise ProbeError("worker configuration must be a JSON object")
        sys.dont_write_bytecode = True
        install_loopback_network_guard()
        sink = DiscardingTextSink()
        with (
            contextlib.redirect_stdout(sink),
            contextlib.redirect_stderr(sink),
        ):
            result = execute_core_worker(config)
    except Exception as exc:
        result = {
            "initialized": False,
            "diagnostic": bounded_text(f"{type(exc).__name__}: {exc}"),
        }
    sys.stdout.write(
        marker
        + json.dumps(bounded_json(result), sort_keys=True, allow_nan=False)
        + "\n"
    )
    return 0


def summarize(measurements: Sequence[dict[str, Any]]) -> dict[str, int]:
    summary = {availability: 0 for availability in sorted(AVAILABILITIES)}
    for item in measurements:
        availability = item.get("availability")
        if availability in summary:
            summary[availability] += 1
    summary["total"] = len(measurements)
    return summary


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo, state_parent, caller_model_cache = validate_args(args)
    revision = resolve_revision(repo, args.expected_revision)
    with tempfile.TemporaryDirectory(
        prefix="tracedecay-ncm-probe-",
        dir=state_parent,
    ) as temporary:
        temporary_root = Path(temporary)
        source_root = temporary_root / "immutable-source"
        http_source_root = temporary_root / "http-source"
        core_source_root = temporary_root / "core-source"
        state_root = temporary_root / "state"
        state_root.mkdir()
        model_cache = caller_model_cache or (temporary_root / "model-cache")
        model_cache.mkdir(parents=True, exist_ok=True)
        materialize_source(repo, revision, source_root)
        shutil.copytree(source_root, http_source_root)
        shutil.copytree(source_root, core_source_root)
        make_tree_read_only(http_source_root)
        make_tree_read_only(core_source_root)

        replacements = (
            (str(repo), "<biomem-repo>"),
            (str(source_root), "<immutable-source>"),
            (str(http_source_root), "<http-source>"),
            (str(core_source_root), "<core-source>"),
            (str(state_root), "<isolated-state>"),
            (str(temporary_root), "<probe-temporary>"),
            (str(model_cache), "<model-cache>"),
        )
        measurements: list[dict[str, Any]] = [syntax_measurement(source_root)]
        methods, inventory = source_inventory(source_root)
        measurements.append(inventory)
        measurements.extend(
            http_surface_measurements(
                http_source_root,
                args.request_timeout_seconds,
                replacements,
            )
        )
        if args.core_mode == "skip":
            measurements.extend(
                core_blocked_measurements(methods, "caller selected --core-mode skip")
            )
        else:
            measurements.extend(
                run_core_child(
                    core_source_root,
                    state_root,
                    model_cache,
                    methods,
                    timeout_seconds=args.core_timeout_seconds,
                    max_recall_results=args.max_recall_results,
                    replacements=replacements,
                )
            )

    emitted_probe_ids = tuple(
        item.get("probe_id") if isinstance(item, dict) else None
        for item in measurements
    )
    if emitted_probe_ids != PROBE_SEQUENCE:
        fail(
            "probe implementation emitted an unexpected measurement sequence: "
            f"{emitted_probe_ids!r}"
        )
    summary = summarize(measurements)
    if (
        sum(summary[availability] for availability in AVAILABILITIES)
        != summary["total"]
    ):
        fail(
            "probe measurement summary does not conserve the emitted measurement count"
        )

    command = [
        "python3",
        "scripts/product/probe-ncm-surface.py",
        "--biomem-repo",
        "<biomem-repo>",
        "--expected-revision",
        revision,
    ]
    if args.core_mode == "skip":
        command.extend(("--core-mode", "skip"))
    return {
        "schema_version": SCHEMA_VERSION,
        "probe_id": PROBE_ID,
        "command": command,
        "input": {
            "expected_revision": revision,
            "source_materialization": "git_archive_of_expected_revision",
            "state_isolation": (
                "unique_temporary_child_beneath_caller_owned_root"
                if state_parent is not None
                else "unique_process_temporary_directory"
            ),
            "model_network_policy": (
                "library_offline_hints_and_non_loopback_python_socket_denial; "
                "native_dependency_code_is_not_os_sandboxed"
            ),
            "core_mode": args.core_mode,
            "max_recall_results": args.max_recall_results,
            "concurrency_levels": list(CONCURRENCY_LEVELS),
        },
        "limits": {
            "core_timeout_seconds": args.core_timeout_seconds,
            "request_timeout_seconds": args.request_timeout_seconds,
            "diagnostic_characters": MAX_DIAGNOSTIC_CHARS,
            "http_response_bytes": MAX_HTTP_RESPONSE_BYTES,
            "source_archive_bytes": MAX_ARCHIVE_BYTES,
            "json_items_per_container": MAX_JSON_ITEMS,
        },
        "measurements": measurements,
        "summary": summary,
    }


def main() -> int:
    if sys.argv[1:] == ["--internal-http-worker"]:
        return internal_http_worker()
    if sys.argv[1:] == ["--internal-core-worker"]:
        return internal_core_worker()
    try:
        args = parse_args()
        document = run(args)
    except (ProbeError, OSError, shutil.Error) as exc:
        error = {
            "schema_version": SCHEMA_VERSION,
            "probe_id": PROBE_ID,
            "error": {
                "type": type(exc).__name__,
                "diagnostic": bounded_text(exc),
            },
        }
        print(json.dumps(error, indent=2, sort_keys=True, allow_nan=False))
        return 2
    indent = None if args.json else 2
    print(
        json.dumps(
            document,
            indent=indent,
            sort_keys=True,
            separators=(",", ":") if args.json else None,
            allow_nan=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
