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
import hashlib
import json
import math
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


CONTRACT_FORMAT = "tracedecay.native-original.case-contract.v1"
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
ROUTE_AVAILABILITY = ("supported", "external_harness", "unsupported", "unknown")
SIDE_NAMES = ("original", "product")
COMPOSITIONS = {"original": "direct_original", "product": "product_native"}
READINESS_MODES = ("comparison", "readiness")
READINESS_MATRIX_ID = "native-ncm-semantic-readiness.v1"
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
READINESS_REQUIRED_ROUTES = (
    REQUIRED_NATIVE_ROUTES | REQUIRED_SESSION_LCM_ROUTES | REQUIRED_HOST_EXTENSION_ROUTES
)
LEDGER_REQUIRED_FIELDS = (
    "actual",
    "attempt_id",
    "attempt_index",
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
    }
)
# `${name}` is the published contract spelling.  Braced `{name}` is accepted
# for old hand-authored fixtures so they fail with a useful unknown-key error
# instead of being passed literally to a child; generated contract examples
# use the canonical `${name}` form.
_PLACEHOLDER = re.compile(r"\$\{([A-Za-z0-9_.-]+)\}|\{([A-Za-z0-9_.-]+)\}")


class RunnerError(RuntimeError):
    """A setup, contract, transport, or comparison preparation failure."""


class ContractError(RunnerError):
    """The case contract or a case does not satisfy the published shape."""


class ReferenceError(RunnerError):
    """The protected 570 source checkout is absent, dirty, or mis-pinned."""


class BinaryError(RunnerError):
    """A selected runtime binary cannot be used by the comparison."""


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


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


def _append_jsonl(path: Path, value: Any) -> dict[str, Any]:
    """Append one fsynced immutable ledger row and return its file receipt."""

    encoded = _json_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as stream:
        offset = stream.tell()
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    return {
        "path": str(path),
        "offset": offset,
        "bytes": len(encoded),
        "sha256": bytes_digest(encoded),
    }


def _require(condition: bool, message: str, error_type: type[Exception] = ContractError) -> None:
    if not condition:
        raise error_type(message)


def _slug(value: str) -> str:
    result = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-")
    return result or "case"


def _git(path: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(path), *arguments],
        check=False,
        capture_output=True,
        text=True,
    )


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

    root = Path(checkout).expanduser()
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
    return {
        "path": str(root),
        "revision": actual_revision,
        "clean": True,
        "status": "",
        "git_root": toplevel.stdout.strip() if toplevel.returncode == 0 else None,
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

    root = Path(source_root).expanduser()
    if not root.is_absolute():
        root = root.resolve()
    _require(root.is_dir(), f"{side} source root is absent: {root}", RunnerError)
    resolved = root.resolve()
    if reference_root is not None:
        _require(
            resolved != reference_root.resolve(),
            "original and product source roots are the same; refusing a shared source comparison",
            RunnerError,
        )
    revision = _git(resolved, "rev-parse", "--verify", "HEAD")
    observed_revision = revision.stdout.strip().lower() if revision.returncode == 0 else None
    return {
        "side": side,
        "path": str(resolved),
        "revision": observed_revision,
        "git_readable": revision.returncode == 0,
        "status": "clean-check-not-required",
    }


def resolve_product_revision(source_root: Path, supplied: str | None) -> str:
    """Return the attested candidate revision, or an explicit unknown state."""

    if supplied is not None:
        value = str(supplied).strip().lower()
        return value or "unknown"
    revision = _git(source_root, "rev-parse", "--verify", "HEAD")
    if revision.returncode == 0 and revision.stdout.strip():
        return revision.stdout.strip().lower()
    return "unknown"


def verify_binary(binary: str | os.PathLike[str], side: str) -> dict[str, Any]:
    """Validate and fingerprint a separately built executable."""

    path = Path(binary).expanduser()
    if not path.is_absolute():
        path = path.resolve()
    if not path.is_file():
        raise BinaryError(f"{side} binary is missing: {path}")
    if not os.access(path, os.X_OK):
        raise BinaryError(f"{side} binary is not executable: {path}")
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
            size += len(chunk)
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
        Path(original["path"]).resolve().parent != Path(product["path"]).resolve().parent,
        "original and product binaries share a binary root; refusing a non-isolated comparison",
        BinaryError,
    )
    return original, product


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(f"cannot read JSON artifact {path}: {error}") from error


def load_contract(path: str | os.PathLike[str] = DEFAULT_CONTRACT) -> dict[str, Any]:
    contract_path = Path(path)
    value = _load_json(contract_path)
    validate_contract(value)
    return value


def load_cases(path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    value = _load_json(Path(path))
    if isinstance(value, dict) and "cases" in value:
        value = value["cases"]
    elif isinstance(value, dict):
        value = [value]
    _require(isinstance(value, list), "cases artifact must be an object with cases or an array")
    cases = [copy.deepcopy(case) for case in value]
    return cases


def _route_map(contract: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {route["id"]: route for route in contract["routes"]}


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


def validate_contract(contract: Mapping[str, Any]) -> None:
    _require(contract.get("format") == CONTRACT_FORMAT, "case contract format is not the accepted Native original contract")
    _require(contract.get("contract_id") == CONTRACT_FORMAT, "case contract id is not the accepted Native original contract")
    _require(contract.get("status") == "accepted", "case contract is not marked accepted")
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
    runner = contract.get("runner")
    _require(isinstance(runner, dict), "case contract is missing runner description")
    _require(runner.get("entrypoints", {}).get("cli_tool", {}).get("transport") == "process", "cli_tool entrypoint must be a process")
    _require(runner.get("entrypoints", {}).get("mcp_stdio", {}).get("transport") == "mcp_jsonl", "mcp_stdio entrypoint must use MCP JSONL")
    outcome = runner.get("outcomes")
    _require(isinstance(outcome, dict), "case contract is missing outcome semantics")
    _require(tuple(outcome.get("statuses", ())) == OUTCOME_STATUSES, "case contract outcome statuses changed")
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
    _require(
        readiness.get("filtered_suite_policy") == "reject_as_readiness_evidence",
        "readiness mode must reject filtered suites as evidence",
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
    required_routes = REQUIRED_NATIVE_ROUTES | REQUIRED_SESSION_LCM_ROUTES | REQUIRED_HOST_EXTENSION_ROUTES
    missing_routes = sorted(required_routes - seen)
    _require(
        not missing_routes,
        "case contract is missing required Native/session/LCM/host routes: " + ", ".join(missing_routes),
    )


def validate_case(case: Mapping[str, Any], contract: Mapping[str, Any]) -> None:
    routes = _route_map(contract)
    case_id = case.get("id", case.get("case_id"))
    _require(isinstance(case_id, str) and bool(case_id.strip()), "case id is required")
    classification = case.get("classification", "original_native")
    _require(classification in ("original_native", "host_extension", "state_regression", "session_lcm"), f"invalid case classification: {case_id}")
    actions = case.get("actions")
    _require(isinstance(actions, list) and actions, f"case has no actions: {case_id}")
    action_ids: set[str] = set()
    for action in actions:
        _require(isinstance(action, dict), f"case action is not an object: {case_id}")
        action_id = action.get("id", action.get("action_id"))
        _require(isinstance(action_id, str) and bool(action_id.strip()) and action_id not in action_ids, f"invalid or duplicate action id in {case_id}")
        action_ids.add(action_id)
        route_id = action.get("route", action.get("operation"))
        _require(isinstance(route_id, str) and route_id in routes, f"case {case_id} references unknown route {route_id!r}")
        route = routes[route_id]
        availability = route.get("availability")
        supported = availability if isinstance(availability, str) else availability.get("original")
        if supported in ("supported", "external_harness"):
            _require(isinstance(action.get("request", {}), dict), f"supported action request must be an object: {case_id}/{action_id}")
        entrypoint = action.get("entrypoint")
        if entrypoint is None and supported not in ("unsupported", "unknown"):
            entrypoint = "command" if supported == "external_harness" else "cli_tool"
        if entrypoint is not None:
            _require(entrypoint in ("cli_tool", "mcp_stdio", "command"), f"unknown action entrypoint: {case_id}/{action_id}")
            _require(
                entrypoint in route.get("entrypoints", {}) or supported == "external_harness",
                f"route {route_id} does not publish the {entrypoint} entrypoint: {case_id}/{action_id}",
            )
        if entrypoint == "mcp_stdio":
            endpoint = route.get("entrypoints", {}).get("mcp_stdio", {})
            _require(
                isinstance(action.get("tool", endpoint.get("tool")), str)
                and bool(action.get("tool", endpoint.get("tool"))),
                f"mcp_stdio action requires a published tool name: {case_id}/{action_id}",
            )
        if entrypoint == "command" and supported in ("supported", "external_harness"):
            argv = action.get("argv", route.get("entrypoints", {}).get("command", {}).get("argv_template"))
            _require(isinstance(argv, list) and argv, f"command action requires argv: {case_id}/{action_id}")
        _validate_output_bindings(action.get("output_bindings"), f"{case_id}/{action_id}")
        _validate_comparison(action.get("comparison"), f"{case_id}/{action_id}")
        if action.get("comparison") is not None:
            _validate_effect_declaration(action.get("comparison"), route, f"{case_id}/{action_id}")
        for phase in ("before", "after", "reopened"):
            specs = action.get("checkpoints", {}).get(phase, []) if isinstance(action.get("checkpoints", {}), dict) else []
            if isinstance(specs, dict):
                specs = [specs]
            _require(isinstance(specs, list), f"checkpoint list is invalid: {case_id}/{action_id}/{phase}")
            for spec in specs:
                _require(isinstance(spec, dict), f"checkpoint must be an object: {case_id}/{action_id}/{phase}")
                checkpoint_route = spec.get("route", spec.get("operation"))
                _require(
                    isinstance(checkpoint_route, str) and checkpoint_route in routes,
                    f"checkpoint references unknown route: {case_id}/{action_id}/{phase}",
                )
                checkpoint_entrypoint = spec.get("entrypoint")
                if checkpoint_entrypoint is not None:
                    _require(
                        checkpoint_entrypoint in routes[checkpoint_route].get("entrypoints", {}),
                        f"checkpoint references unpublished entrypoint: {case_id}/{action_id}/{phase}",
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
            _validate_aux_action(spec, contract, f"{case_id}/side_setup/{side}/{setup_index}")

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
                    _validate_aux_action(check, contract, check_label)
                    _validate_proof_observables(check, check_label)
            else:
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
                _validate_aux_action(close_spec.get("action"), contract, f"{case_id}/lifecycle/close/{side}")
            reopen_spec = reopen.get(side)
            _require(isinstance(reopen_spec, Mapping), f"case lifecycle.reopen lacks {side}: {case_id}")
            if reopen_spec.get("mode", "action") == "action":
                _validate_aux_action(reopen_spec.get("action"), contract, f"{case_id}/lifecycle/reopen/{side}")


def _validate_proof_observables(spec: Mapping[str, Any], label: str) -> None:
    required = spec.get("required_json_pointers", ())
    _require(isinstance(required, list) and required, f"composition proof requires observable pointers: {label}")
    for pointer in required:
        _pointer_parts(pointer)
    equals = spec.get("equals", {})
    _require(isinstance(equals, Mapping) and bool(equals), f"composition proof requires at least one exact value: {label}")
    for pointer in equals:
        _pointer_parts(pointer)

def _validate_aux_action(spec: Any, contract: Mapping[str, Any], label: str) -> None:
    _require(isinstance(spec, Mapping), f"{label} must be an action object")
    routes = _route_map(contract)
    route_id = spec.get("route", spec.get("operation"))
    _require(isinstance(route_id, str) and route_id in routes, f"{label} references unknown route")
    route = routes[route_id]
    availability = route.get("availability")
    original_availability = availability if isinstance(availability, str) else availability.get("original")
    entrypoint = spec.get("entrypoint")
    if entrypoint is None and original_availability not in ("unsupported", "unknown"):
        entrypoint = "command" if original_availability == "external_harness" else "cli_tool"
    if entrypoint is not None:
        _require(entrypoint in ("cli_tool", "mcp_stdio", "command"), f"{label} has invalid entrypoint")
        _require(entrypoint in route.get("entrypoints", {}) or original_availability == "external_harness", f"{label} entrypoint is not published")
    _require(isinstance(spec.get("request", {}), dict), f"{label} request must be an object")
    _validate_output_bindings(spec.get("output_bindings"), label)
    _validate_comparison(spec.get("comparison"), label)
    if spec.get("comparison") is not None:
        _validate_effect_declaration(spec.get("comparison"), route, label)


def _owned_relative_path(value: Any, *, side_dir: Path, field: str, default: str) -> Path:
    """Resolve a case path inside the side's private root.

    Comparison cases are deliberately relocatable.  Relative paths stay under
    the side root; absolute paths are accepted only when they already resolve
    inside that same private root.  This prevents ``..`` traversal or a
    fixture symlink from touching operator state.
    """

    raw = default if value is None else value
    _require(isinstance(raw, str) and bool(raw), f"{field} must be a non-empty relative path")
    candidate = Path(raw)
    resolved_side = side_dir.resolve()
    resolved = candidate.resolve() if candidate.is_absolute() else (side_dir / candidate).resolve()
    _require(resolved == resolved_side or resolved.is_relative_to(resolved_side), f"{field} escapes the side root: {raw!r}")
    return resolved


def select_relevant_cases(
    cases: Sequence[Mapping[str, Any]],
    required_operations: Iterable[str] = (),
) -> list[dict[str, Any]]:
    selected = [copy.deepcopy(dict(case)) for case in cases]
    required = set(required_operations)
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


def validate_readiness_selection(
    cases: Sequence[Mapping[str, Any]],
    required_operations: Iterable[str] = (),
    *,
    readiness_mode: str = "comparison",
) -> list[dict[str, Any]]:
    """Select cases while preventing a filtered suite from claiming readiness.

    The readiness matrix is a complete route obligation.  A focused
    comparison is useful for development, but it cannot be reported as
    readiness evidence when ``--operation`` filters away the accepted route
    set.  Readiness mode therefore rejects every filtered invocation and also
    checks that the supplied cases cover each accepted route.
    """

    _require(readiness_mode in READINESS_MODES, f"invalid readiness mode: {readiness_mode!r}")
    required = set(required_operations)
    if readiness_mode == "readiness" and required:
        raise RunnerError(
            "readiness evidence cannot use a filtered operation suite; "
            "run the complete accepted route set without --operation"
        )
    selected = select_relevant_cases(cases, required)
    if readiness_mode != "readiness":
        return selected
    covered = {
        action.get("route", action.get("operation"))
        for case in selected
        for action in case.get("actions", ())
        if isinstance(action, Mapping)
    }
    missing = sorted(READINESS_REQUIRED_ROUTES - covered)
    if missing:
        raise RunnerError(
            "readiness evidence is incomplete; missing accepted routes: "
            + ", ".join(missing)
        )
    return selected


def _render(value: Any, context: Mapping[str, Any]) -> Any:
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


def _pointer_parts(pointer: str) -> list[str]:
    if pointer == "":
        return []
    _require(pointer.startswith("/"), f"JSON pointer must start with '/': {pointer}")
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]


def json_pointer(value: Any, pointer: str) -> Any:
    current = value
    for part in _pointer_parts(pointer):
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
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
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return
    final = parts[-1]
    if isinstance(current, dict):
        current.pop(final, None)
    elif isinstance(current, list) and final.isdigit() and int(final) < len(current):
        current.pop(int(final))


def _replace_pointer(value: Any, pointer: str, replacement: Any) -> bool:
    parts = _pointer_parts(pointer)
    if not parts:
        return False
    current = value
    for part in parts[:-1]:
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return False
    final = parts[-1]
    if isinstance(current, dict) and final in current:
        current[final] = replacement
        return True
    if isinstance(current, list) and final.isdigit() and int(final) < len(current):
        current[int(final)] = replacement
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
    source = copy.deepcopy(response)
    for pointer in ignored:
        _drop_pointer(source, pointer)

    # A case may explicitly identify values that are expected to differ
    # because the two stores were seeded independently (for example, a
    # generated fact id).  Replace only the declared response paths with a
    # symbolic identity token; all other fields, including score/order,
    # receipts and omissions, remain exact comparison material.
    for mapping in comparison.get("identity_mappings", ()):
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


@dataclass(frozen=True)
class ProcessTimeouts:
    action_seconds: float = 120.0
    terminate_seconds: float = 2.0
    kill_seconds: float = 2.0

    def __post_init__(self) -> None:
        if any(not math.isfinite(float(value)) or value <= 0 or not float(value) == value for value in asdict(self).values()):
            raise ValueError("process timeouts must be positive finite numbers")


def _daemon_socket_path(side_dir: Path) -> Path:
    """Return a private, bounded-length endpoint for one comparison side."""

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
    temp_root = Path(tempfile.gettempdir())
    candidate = temp_root / basename / "daemon.sock"
    if len(os.fsencode(str(candidate))) >= 80:
        candidate = Path("/tmp") / basename / "daemon.sock"
    return candidate


def _prepare_private_socket_parent(parent: Path) -> None:
    """Create and validate the short Unix socket directory used by a side."""

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
    if os.name != "nt":
        return (
            isinstance(endpoint, Mapping)
            and endpoint.get("kind") == "unix"
            and isinstance(endpoint.get("address"), str)
            and Path(endpoint["address"]).resolve() == socket_path.resolve()
        )
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

        artifact_dir.mkdir(parents=True, exist_ok=False)
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
        except (OSError, ValueError):
            stdout_file.close()
            stderr_file.close()
            raise
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
                record = json.loads(self.authority_path.read_text(encoding="utf-8"))
                endpoint = record.get("endpoint")
                if (
                    isinstance(record, Mapping)
                    and record.get("pid") == self.process.pid
                    and _endpoint_matches(endpoint, self.socket_path)
                    and Path(str(record.get("profile_root", ""))).resolve() == self.profile_root
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
                    if _endpoint_connectable(endpoint):
                        self.authority = dict(record)
                        self.identity = self._identity(record)
                        return
                    last_error = "authority identity published but endpoint is not connectable"
                else:
                    last_error = "authority identity did not match the owned process/profile/endpoint"
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as error:
                last_error = f"authority record is not ready: {error}"
            time.sleep(0.05)
        raise RunnerError(f"{self.side} foreground daemon readiness timed out: {last_error}")

    def _identity(self, record: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "side": self.side,
            "pid": self.process.pid,
            "process_group_id": self.process.pid if os.name != "nt" else None,
            "process_run_id": record.get("process_run_id"),
            "epoch": record.get("epoch"),
            "version": record.get("version"),
            "endpoint": copy.deepcopy(record.get("endpoint")),
            "profile_root": str(self.profile_root),
            "process_root": str(self.process_root),
            "authority_path": str(self.authority_path),
        }

    def current_identity(self) -> dict[str, Any] | None:
        if self.process.poll() is not None:
            return None
        try:
            record = json.loads(self.authority_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(record, Mapping) or record.get("pid") != self.process.pid:
            return None
        if not _endpoint_matches(record.get("endpoint"), self.socket_path):
            return None
        if Path(str(record.get("profile_root", ""))).resolve() != self.profile_root:
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
            for field in ("pid", "process_run_id", "epoch", "version", "endpoint", "profile_root", "authority_path"):
                if identity.get(field) != self.identity.get(field):
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
        if self.process.poll() is not None:
            return True
        try:
            self.process.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            return False
        return True

    def stop(self, timeouts: ProcessTimeouts, *, reason: str) -> dict[str, Any]:
        if self.cleanup is not None:
            return self.cleanup
        sent = None
        if self.process.poll() is None:
            sent = "SIGTERM" if os.name != "nt" else "terminate"
            _signal_group(self.process, signal.SIGTERM, self.process.pid if os.name != "nt" else None)
            if os.name == "nt":
                self.process.terminate()
            if not self.wait_for_exit(timeouts.terminate_seconds):
                sent = "SIGKILL" if os.name != "nt" else "kill"
                _signal_group(self.process, signal.SIGKILL, self.process.pid if os.name != "nt" else None)
                if os.name == "nt":
                    self.process.kill()
                self.wait_for_exit(timeouts.kill_seconds)
        if self.process.poll() is None:
            try:
                self.process.kill()
            except OSError:
                pass
            self.process.wait()
        returncode = self.process.returncode
        cleanup_errors: list[str] = []
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
    result = subprocess.run(
        ["ps", "-axo", "pid=,pgid="],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
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
    if group_id is None or os.name == "nt" or group_id == os.getpgrp():
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

    artifact_dir.mkdir(parents=True, exist_ok=False)
    started_wall = time.time_ns()
    started_monotonic = time.monotonic_ns()
    process: subprocess.Popen[bytes] | None = None
    stdout = b""
    stderr = b""
    timed_out = False
    spawn_error: str | None = None
    process_group_id: int | None = None
    returncode: int | None = None
    try:
        process = _spawn(argv, cwd, environment)
        process_group_id = process.pid if os.name != "nt" else None
        try:
            stdout, stderr = process.communicate(input=input_bytes, timeout=timeout.action_seconds)
            returncode = process.returncode
        except subprocess.TimeoutExpired as error:
            timed_out = True
            stdout = error.output or b""
            stderr = error.stderr or b""
            _signal_group(process, signal.SIGTERM, process_group_id)
            try:
                stdout, stderr = process.communicate(timeout=timeout.terminate_seconds)
            except subprocess.TimeoutExpired:
                _signal_group(process, signal.SIGKILL, process_group_id)
                try:
                    stdout, stderr = process.communicate(timeout=timeout.kill_seconds)
                except subprocess.TimeoutExpired:
                    process.kill()
                    stdout, stderr = process.communicate()
            returncode = process.returncode
    except (OSError, ValueError) as error:
        spawn_error = f"{type(error).__name__}: {error}"
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
    stdout_ref = _write_bytes(artifact_dir / "stdout.bin", stdout)
    stderr_ref = _write_bytes(artifact_dir / "stderr.bin", stderr)
    if spawn_error:
        status = "unknown"
        reason = spawn_error
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
    process_record = {
        "argv": list(argv),
        "cwd": str(cwd),
        "phase": phase,
        "pid": process.pid if process is not None else None,
        "process_group_id": process_group_id,
        "start_wall_time_ns": started_wall,
        "end_wall_time_ns": time.time_ns(),
        "elapsed_ns": ended_monotonic - started_monotonic,
        "returncode": returncode,
        "status": status,
        "reason": reason,
        "detached_process_possible": detached_process_possible,
        "process_root": environment.get("TRACEDECAY_COMPARISON_PROCESS_ROOT"),
        "remaining_owned_children": len(members),
        "process_group_observed": group_observation,
    }
    process_ref = _write_json(artifact_dir / "process.json", process_record)
    return {
        "status": status,
        "reason": reason,
        "returncode": returncode,
        "stdout": stdout_ref,
        "stderr": stderr_ref,
        "process": process_record,
        "process_artifact": process_ref,
        "cleanup": {
            "status": cleanup_status,
            "reason": (
                "CLI may have contacted a daemon outside this process group"
                if detached_process_possible
                else (None if not members else "owned process-group members remained after cleanup")
            ),
            "remaining_owned_children": len(members),
            "process_group_id": process_group_id,
        },
        "timing": {"start_monotonic_ns": started_monotonic, "end_monotonic_ns": ended_monotonic, "elapsed_ns": ended_monotonic - started_monotonic},
    }


def _parse_json_bytes(value: bytes) -> tuple[Any | None, str | None]:
    if not value.strip():
        return None, "empty stdout"
    try:
        return json.loads(value.decode("utf-8", errors="strict")), None
    except (UnicodeDecodeError, json.JSONDecodeError) as first_error:
        # Preserve the raw stream, but tolerate an informational line before
        # the CLI's one JSON result.  A result is accepted only when exactly
        # one nonempty line decodes as JSON; arbitrary text remains unknown.
        decoded = value.decode("utf-8", errors="replace")
        candidates = []
        for line in decoded.splitlines():
            try:
                candidates.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        if len(candidates) == 1:
            return candidates[0], None
        return None, f"stdout is not one JSON response: {type(first_error).__name__}: {first_error}"


def _mcp_response(value: bytes, request_id: str) -> tuple[Any | None, str | None]:
    responses = []
    for line in value.splitlines():
        try:
            row = json.loads(line.decode("utf-8", errors="strict"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(row, dict) and str(row.get("id")) == request_id:
            responses.append(row)
    if len(responses) == 1:
        return responses[0], None
    if not responses:
        return None, f"MCP response id {request_id} was not observed"
    return None, f"MCP response id {request_id} was repeated"


def _response_status(process_result: Mapping[str, Any], response: Any | None, parse_error: str | None) -> str:
    if process_result["status"] == "censored":
        return "censored"
    if process_result["status"] == "unknown":
        return "unknown"
    if response is None:
        return "unknown"
    return "completed" if process_result["status"] == "completed" else "error"


def _availability(route: Mapping[str, Any], side: str) -> str:
    raw = route.get("availability")
    return raw if isinstance(raw, str) else raw.get(side, "unknown")


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
    for path in (project_root, profile_root, state_root):
        path.mkdir(parents=True, exist_ok=True)
    process_root = (side_dir / "process").resolve()
    process_root.mkdir(parents=True, exist_ok=True)
    resolved_source_root = source_root.resolve() if source_root is not None else None
    binary_root = binary.resolve().parent
    daemon_socket = _daemon_socket_path(side_dir)
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
        "bindings": copy.deepcopy(dict(bindings or {})),
        "reference_revision": REFERENCE_REVISION,
        "product_revision": PRODUCT_REVISION,
    }


def _environment(
    side_dir: Path,
    action: Mapping[str, Any],
    context: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    home = side_dir / "home"
    data = side_dir / "data"
    config = side_dir / "config"
    runtime = side_dir / "runtime"
    for path in (home, data, config, runtime):
        path.mkdir(parents=True, exist_ok=True)
    environment = {key: value for key, value in os.environ.items() if key not in _ISOLATION_ENV_KEYS}
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
        }
    )
    overrides = action.get("environment", {})
    _require(isinstance(overrides, dict), "action environment must be an object")
    rendered = _render(overrides, context or {})
    forbidden = sorted(set(rendered).intersection(_ISOLATION_ENV_KEYS))
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
    endpoint = route.get("entrypoints", {}).get(entrypoint, {})
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
) -> bytes:
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
    endpoint = (route or {}).get("entrypoints", {}).get("mcp_stdio", {})
    tool_name = action.get("tool", endpoint.get("tool"))
    _require(isinstance(tool_name, str) and tool_name, "mcp_stdio action requires tool")
    call = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": request},
    }
    notification = {"jsonrpc": "2.0", "method": "notifications/initialized"}
    return b"".join(_json_bytes(item) + b"\n" for item in (initialize, notification, call))


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
    route_id = action.get("route", action.get("operation"))
    route = _route_map(contract).get(route_id)
    if route is None:
        artifact_dir.mkdir(parents=True, exist_ok=False)
        result = {
            "status": "invalid",
            "reason": f"unknown route {route_id!r}",
            "provider_contacted": False,
            "composition_reached": False,
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
            "provider_contacted": False,
            "composition_reached": False,
            "request_sent": False,
            "route": route_id,
            "operation_kind": route.get("operation_kind"),
            "effect_policy": _route_effect_policy(route),
            "request": request,
            "request_sha256": request_ref["sha256"],
            "response": None,
        }
        _write_json(artifact_dir / "result.json", result)
        return result
    request = action.get("request", {})
    _require(isinstance(request, dict), f"action request must be an object: {case.get('id')}/{action.get('id', action.get('action_id'))}")
    request_path = artifact_dir / "request.json"
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
    )
    rendered_request = _render(request, context)
    request_ref = _write_json(request_path, rendered_request)
    entrypoint = action.get("entrypoint")
    if entrypoint is None:
        entrypoint = "command" if availability == "external_harness" else "cli_tool"
    if availability == "external_harness":
        entrypoint = "command"
    argv = _argv_for(route, action, entrypoint, context)
    environment = _environment(side_dir, action, context)
    process_input = _mcp_input(action, context, rendered_request, route) if entrypoint == "mcp_stdio" else None
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
        response, parse_error = _mcp_response(Path(process["stdout"]["path"]).read_bytes(), response_id)
    else:
        response, parse_error = _parse_json_bytes(Path(process["stdout"]["path"]).read_bytes())
    status = _response_status(process, response, parse_error)
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
        "status": status,
        "process_status": process["status"],
        "reason": process.get("reason") or parse_error,
        "provider_contacted": None,
        "composition_reached": None,
        "request_sent": True,
        "route": route_id,
        "operation_kind": route.get("operation_kind"),
        "effect_policy": _route_effect_policy(route),
        "entrypoint": entrypoint,
        "logical_request": copy.deepcopy(request),
        "request": rendered_request,
        "request_sha256": request_ref["sha256"],
        "response": response,
        "response_parse_error": parse_error,
        "output_bindings": copy.deepcopy(output_bindings),
        "bindings": captured_bindings,
        "binding_errors": binding_errors,
        "composition_proof": action.get("composition_proof"),
        "roots": {
            "source_root": context.get("source_root"),
            "binary_root": context.get("binary_root"),
            "process_root": context.get("process_root"),
            "store_root": context.get("store_root"),
            "artifact_root": str(artifact_dir.resolve()),
        },
        "daemon_identity": copy.deepcopy(owned_daemon.current_identity())
        if owned_daemon is not None
        else None,
        "process": process,
        "cleanup": copy.deepcopy(process.get("cleanup")),
    }
    if binding_errors and status in ("completed", "error"):
        result["status"] = "unknown"
        result["reason"] = "declared output binding was absent: " + ", ".join(binding_errors)
    _write_json(artifact_dir / "result.json", result)
    return result


def _effect_pointers(comparison: Mapping[str, Any]) -> dict[str, list[str]]:
    return {
        field: [str(pointer) for pointer in comparison.get(field, ())]
        for field in (
            "effect_json_pointers",
            "receipt_json_pointers",
            "state_json_pointers",
            "no_effect_json_pointers",
        )
    }


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
        return None
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
    declared = [pointer for field in fields for pointer in pointers[field]]
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
    for pointer in declared:
        left = json_pointer(original.get("response"), pointer)
        right = json_pointer(product.get("response"), pointer)
        if left is _MISSING:
            missing_original.append(pointer)
        else:
            original_values[pointer] = copy.deepcopy(left)
        if right is _MISSING:
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
    if original_values != product_values:
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

    left_status, right_status = original.get("status"), product.get("status")
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
    if left_projection != right_projection:
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
    checkpoints = action.get("checkpoints", {})
    if not isinstance(checkpoints, dict):
        return []
    value = checkpoints.get(phase, [])
    if isinstance(value, dict):
        return [copy.deepcopy(value)]
    return [copy.deepcopy(item) for item in value]


def _status_priority(statuses: Iterable[str]) -> str:
    observed = set(statuses)
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
    response = result.get("response")
    if result.get("status") != "completed" or response is None:
        return {
            "status": "unknown",
            "composition_reached": False,
            "reason": f"{side} composition proof did not return a completed response",
            "side_status": result.get("status"),
        }
    required = proof.get("required_json_pointers", ())
    missing = [pointer for pointer in required if json_pointer(response, pointer) is _MISSING]
    if missing:
        return {
            "status": "unknown",
            "composition_reached": False,
            "reason": f"{side} composition proof omitted required observables",
            "missing": missing,
        }
    mismatches = []
    for pointer, expected in proof.get("equals", {}).items():
        actual = json_pointer(response, pointer)
        if actual is _MISSING or actual != expected:
            mismatches.append({"pointer": pointer, "expected": expected, "actual": None if actual is _MISSING else actual})
    if mismatches:
        return {
            "status": "fail",
            "composition_reached": False,
            "reason": f"{side} composition proof did not match its declared evidence",
            "mismatches": mismatches,
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
            if result.get("status") in ("unknown", "invalid", "censored", "error"):
                comparison_statuses.append("unknown" if result["status"] == "error" else result["status"])

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
            if close_row.get("status") != "pass":
                reopen_rows[side] = {
                    "status": "unknown",
                    "reason": "fresh daemon was not started because owned close was not verified",
                    "close": close_row,
                }
                comparison_statuses.append("unknown")
                continue
            old_daemon = (daemons or {}).get(side)
            if old_daemon is None or not isinstance(daemons, dict):
                reopen_rows[side] = {
                    "status": "unknown",
                    "reason": "runner-owned daemon state is unavailable for reopen",
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
                    "status": "unknown",
                    "reason": f"fresh daemon could not be started: {error}",
                    "previous_daemon": copy.deepcopy(old_daemon.identity),
                }
                comparison_statuses.append("unknown")
                continue
            if reopen_spec.get("mode", "action") != "action" or not isinstance(reopen_action, Mapping):
                reopen_rows[side] = {
                    "status": "invalid",
                    "reason": "fresh-process reopen action is required",
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
                "status": "pass"
                if reopen_result.get("status") == "completed" and identity_ok
                else "unknown",
                "reason": (
                    "fresh action completed under a new runner-owned daemon identity"
                    if reopen_result.get("status") == "completed" and identity_ok
                    else "fresh action did not prove a new runner-owned daemon identity"
                ),
                "result": reopen_result,
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
        reopen_comparison_spec = (
            original_reopen_action.get("comparison")
            if isinstance(original_reopen_action, Mapping)
            else None
        )
        if reopen_comparison_spec is None and isinstance(product_reopen_action, Mapping):
            reopen_comparison_spec = product_reopen_action.get("comparison")
        reopen_route = (
            original_reopen_action.get("route", original_reopen_action.get("operation"))
            if isinstance(original_reopen_action, Mapping)
            else None
        )
        reopen_route_obj = _route_map(contract).get(reopen_route)
        if not isinstance(original_reopen, Mapping) or not isinstance(product_reopen, Mapping):
            reopen_comparison = {
                "status": "unknown",
                "reason": "reopen outputs were not produced on both sides",
            }
        elif reopen_comparison_spec is None:
            reopen_comparison = {
                "status": "effect_unknown",
                "reason": "reopen outputs require an explicit paired comparison declaration",
            }
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


def _side_operation_digest(
    result: Mapping[str, Any],
    side: str,
    fields: Sequence[str],
) -> str | None:
    observations: list[dict[str, Any]] = []
    for action in result.get("actions", ()):
        if not isinstance(action, Mapping):
            continue
        side_result = action.get(side)
        if not isinstance(side_result, Mapping):
            continue
        input_action = action.get("input", {})
        comparison = input_action.get("comparison", {}) if isinstance(input_action, Mapping) else {}
        if not isinstance(comparison, Mapping):
            continue
        pointers = [
            pointer
            for field in fields
            for pointer in comparison.get(field, ())
        ]
        if not pointers:
            continue
        response = side_result.get("response")
        values: dict[str, Any] = {}
        for pointer in pointers:
            value = json_pointer(response, pointer)
            if value is _MISSING:
                return None
            values[pointer] = copy.deepcopy(value)
        observations.append({"action_id": action.get("action_id"), "values": values})
    return json_digest(observations) if observations else None


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
) -> dict[str, Any]:
    """Materialize one complete append-only readiness attempt row.

    Values that a process did not expose remain ``None`` or a typed evidence
    object.  The row is still complete in shape, so an absent receipt/state
    cannot be mistaken for a successful mutation.
    """

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
    actions = [
        action
        for action in result.get("actions", ())
        if isinstance(action, Mapping)
    ]
    commands: dict[str, list[Any]] = {side: [] for side in SIDE_NAMES}
    for action in actions:
        for side in SIDE_NAMES:
            side_result = action.get(side)
            process = side_result.get("process", {}) if isinstance(side_result, Mapping) else {}
            process_record = process.get("process", {}) if isinstance(process, Mapping) else {}
            argv = process_record.get("argv") if isinstance(process_record, Mapping) else None
            if isinstance(argv, list):
                commands[side].append(copy.deepcopy(argv))
    source_sha = {
        "reference": REFERENCE_REVISION,
        "candidate": result.get("product_source_revision") or "unknown",
    }
    scope_values = {
        side: {
            key: profile_paths[side].get(key)
            for key in ("project_root", "profile_root", "state_root", "store_root")
        }
        for side in SIDE_NAMES
    }
    mutation_fields = ("effect_json_pointers", "receipt_json_pointers", "state_json_pointers")
    no_effect_fields = ("no_effect_json_pointers",)
    reopen = result.get("lifecycle", {})
    reopen_value = reopen.get("reopen_comparison") if isinstance(reopen, Mapping) else None
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
        "candidate_binary_sha256": product_binary_ref["sha256"],
        "candidate_process_identity": copy.deepcopy(process_identity.get("product")),
        "candidate_store_identity": copy.deepcopy(profile_paths.get("product")),
        "command": commands,
        "composition_reached": copy.deepcopy(result.get("composition_reached", {side: False for side in SIDE_NAMES})),
        "effect_digest": {
            side: _side_operation_digest(result, side, mutation_fields)
            for side in SIDE_NAMES
        },
        "evidence_paths": [str(result.get("artifact_directory", ""))],
        "expected": expected,
        "failure_class": None if result.get("status") == "pass" else result.get("reason", result.get("status")),
        "feature_set": {
            "reference": contract.get("baseline", {}).get("reference_features"),
            "candidate": contract.get("baseline", {}).get("candidate_features"),
        },
        "first_causal_stage": None if result.get("status") == "pass" else "comparison",
        "fixture_seed": json_digest(case.get("fixtures", {})),
        "model_sha256": None,
        "oracle_kind": "independent_570_candidate_production",
        "outcome": result.get("status", "unknown") if result.get("status") in OUTCOME_STATUSES else "unknown",
        "profile_paths": profile_paths,
        "protocol_version": "native-original.v1",
        "provider_id": "tracedecay.native",
        "provider_implementation_sha256": None,
        "receipt_digest": {
            side: _side_operation_digest(result, side, ("receipt_json_pointers",))
            for side in SIDE_NAMES
        },
        "reference_binary_sha256": original_binary_ref["sha256"],
        "reference_process_identity": copy.deepcopy(process_identity.get("original")),
        "reference_store_identity": copy.deepcopy(profile_paths.get("original")),
        "reopen_or_no_effect": {
            "reopen_comparison": copy.deepcopy(reopen_value),
            "no_effect_digest": {
                side: _side_operation_digest(result, side, no_effect_fields)
                for side in SIDE_NAMES
            },
        },
        "request_digest": json_digest(case),
        "result_digest": json_digest(result),
        "row_id": str(result.get("case_id", case.get("id", case.get("case_id")))),
        "scope_digest": json_digest(scope_values),
        "source_sha": source_sha,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "state_digest": {
            side: _side_operation_digest(result, side, ("state_json_pointers",))
            for side in SIDE_NAMES
        },
        "state_generation": None,
        "state_schema_version": None,
        "tokenizer_sha256": None,
    }
    missing = sorted(set(LEDGER_REQUIRED_FIELDS) - row.keys())
    _require(not missing, "attempt ledger row is missing required fields: " + ", ".join(missing), RunnerError)
    return row


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
            failed_result = {
                "case_id": case.get("id", case.get("case_id")),
                "classification": case.get("classification", "original_native"),
                "status": "unknown",
                "reason": reason,
                "daemon_cleanup": copy.deepcopy(daemon_cleanup),
                "artifact_directory": str(case_artifact_dir),
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
                        )
                    ]
                except (RunnerError, OSError, TypeError, ValueError):
                    failed_result["attempt_ledger"] = []
            try:
                _write_json(case_artifact_dir / "case-result.json", failed_result)
            except (OSError, TypeError, ValueError):
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
                )
            ]
        _write_json(case_artifact_dir / "case-result.json", result)
    return result


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
    readiness_mode: str = "comparison",
) -> dict[str, Any]:
    validate_contract(contract)
    required = tuple(required_operations)
    selected = validate_readiness_selection(
        cases,
        required,
        readiness_mode=readiness_mode,
    )
    for case in selected:
        validate_case(case, contract)
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
    root = Path(artifact_root).expanduser()
    if not root.is_absolute():
        root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    token = run_id or f"run-{time.time_ns()}-{os.getpid()}"
    run_dir = root / _slug(token)
    run_dir.mkdir()
    manifest = {
        "format": CONTRACT_FORMAT,
        "contract_id": contract["contract_id"],
        "run_id": token,
        "reference": reference,
        "source_roots": {
            "original": reference,
            "product": product_source,
        },
        "binaries": {"original": original, "product": product},
        "binary_roots": {
            "original": str(Path(original["path"]).resolve().parent),
            "product": str(Path(product["path"]).resolve().parent),
        },
        "expected_product_revision": PRODUCT_REVISION,
        "product_source_revision": product_source_revision,
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
            "accepted_route_set": sorted(READINESS_REQUIRED_ROUTES),
            "filtered_suite_rejected": readiness_mode == "readiness" and bool(required),
        },
        "timeouts": asdict(timeouts),
        "composition": COMPOSITIONS,
    }
    _write_json(run_dir / "run-manifest.json", manifest)
    case_results = []
    for index, case in enumerate(selected):
        case_id = case.get("id", case.get("case_id"))
        case_dir = run_dir / f"{index:03d}-{_slug(str(case_id))}"
        case_result = run_case(
                case,
                contract=contract,
                original_binary=Path(original["path"]),
                product_binary=Path(product["path"]),
                case_artifact_dir=case_dir,
                timeouts=timeouts,
                candidate_product_revision=product_source_revision,
                source_roots={
                    "original": reference_source_root,
                    "product": Path(product_source["path"]),
                },
                attempt_id=f"{token}/{index}",
                attempt_index=index,
            )
        case_results.append(case_result)
        ledger_rows = case_result.get("attempt_ledger", ())
        if not isinstance(ledger_rows, list) or not ledger_rows:
            raise RunnerError(f"case {case_id} did not produce an attempt ledger row")
        ledger_ref = _append_jsonl(run_dir / "attempts.jsonl", ledger_rows[0])
        case_result["attempt_ledger_receipt"] = ledger_ref
        _write_json(
            case_dir / "attempt.json",
            {"row": ledger_rows[0], "receipt": ledger_ref},
        )
    report = {
        **manifest,
        "status": _status_priority(result["status"] for result in case_results),
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
    _write_json(run_dir / "report.json", report)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate", help="validate the contract and case input without executing a binary")
    validate.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    validate.add_argument("--cases", type=Path)
    validate.add_argument("--operation", action="append", default=[])
    validate.add_argument("--readiness-mode", choices=READINESS_MODES, default="comparison")
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "verify-reference":
        print(json.dumps(verify_reference_checkout(args.checkout), sort_keys=True))
        return 0
    contract = load_contract(args.contract)
    if args.command == "validate":
        if args.cases:
            cases = validate_readiness_selection(
                load_cases(args.cases),
                args.operation,
                readiness_mode=args.readiness_mode,
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
        readiness_mode=args.readiness_mode,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["status"] == "pass" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RunnerError as error:
        print(f"native-original runner: {error}", file=sys.stderr)
        raise SystemExit(2)
