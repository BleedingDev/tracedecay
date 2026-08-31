#!/usr/bin/env python3
"""Run a product-owned, isolated upstream convergence train.

The train is deliberately ref-oriented.  ``prepare`` creates a new branch at
the configured product branch and starts a non-committing merge of one exact
upstream commit.  Conflict decisions live in ``state.json`` in the caller's
train directory.  ``finalize`` writes the resolved tree, floor metadata, and a
convergence receipt into one commit and publishes that commit with a Git
compare-and-swap transaction.  The product branch is only ever verified; it
is never an update target.

All Git calls use argv vectors and bounded diagnostics.  The state directory
is intentionally outside the repository's administrative stores: it is
workflow state supplied by the operator, not a Beads receipt.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Iterable, Sequence


SHA_RE = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
ZERO_SHA = "0" * 40
SCHEMA_VERSION = 1
STATE_KIND = "tracedecay.upstream.sync-train.v1"
RECEIPT_CONTRACT_ID = "tracedecay.upstream-sync-train-receipt.v1"
RECEIPT_SCHEMA_URL = (
    "https://tracedecay.dev/schemas/product/upstream/sync-train-receipt.schema.json"
)
POLICY_REVISION = "sync-train.v1"
WORKFLOW_NAME = "run-upstream-sync-train"
TRAIN_BEAD_ID = "tdmem-1205"
MAX_DIAGNOSTIC_CHARS = 2_048
MAX_FIELD_CHARS = 4_096
MAX_CONFLICTS = 1_000
DEFAULT_PRODUCT_BRANCH = "refs/heads/feat/pluggable-memory-providers-v2"
DEFAULT_SYNC_PREFIX = "refs/heads/sync/upstream/"
DEFAULT_POLICY = "product/upstream/sync-policy.json"
DEFAULT_FLOOR_METADATA = "product/upstream/tracedecay-v2-pr707.json"
DEFAULT_RECEIPT_TEMPLATE = "product/upstream/sync-train-receipts/sync-train-{short_sha}.json"
DEFAULT_PRODUCT_REPOSITORY = "BleedingDev/tracedecay"
DEFAULT_UPSTREAM_REPOSITORY = "ScriptedAlchemy/tracedecay"
GATE_ORDER = (
    "upstream_required",
    "product_contracts",
    "native_parity",
    "provider_conformance",
    "scope_crash_security",
    "generated_drift",
)
GATE_PHASES = {
    "upstream_required": "upstream",
    "product_contracts": "product",
    "native_parity": "product",
    "provider_conformance": "product",
    "scope_crash_security": "product",
    "generated_drift": "product",
}
RESOLUTION_RE = re.compile(r"^[a-z][a-z0-9._-]*$")
CONFLICT_OWNER_ALIASES = frozenset({"product", "upstream", "shared"})


class SyncTrainError(RuntimeError):
    """A validation, Git, or state transition failure."""


def bounded(value: str, limit: int = MAX_DIAGNOSTIC_CHARS) -> str:
    """Keep diagnostics useful without allowing command output to flood JSON."""

    value = value.strip()
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 16)] + "... [truncated]"


def require_nonempty(value: object, label: str, *, max_chars: int = MAX_FIELD_CHARS) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SyncTrainError(f"{label} must be a non-empty string")
    result = value.strip()
    if len(result) > max_chars:
        raise SyncTrainError(f"{label} exceeds the {max_chars}-character limit")
    return result


def require_sha(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA_RE.fullmatch(value):
        raise SyncTrainError(f"{label} must be a lowercase 40-character Git SHA")
    return value


def require_repository(value: object, label: str) -> str:
    result = require_nonempty(value, label)
    if REPOSITORY_RE.fullmatch(result) is None:
        raise SyncTrainError(f"{label} must use owner/name form")
    return result


def policy_conflict_owners(policy: dict[str, Any]) -> list[str]:
    """Return the finite set of policy-owned conflict authorities.

    The receipt schema intentionally leaves ``owner`` open-ended, but a train
    must not accept an arbitrary caller-controlled owner.  Product policy owns
    the authority list; the short role aliases keep the CLI useful in local
    rehearsals and fixtures.
    """

    owners: set[str] = set(CONFLICT_OWNER_ALIASES)
    ownership = policy.get("ownership")
    if isinstance(ownership, dict):
        for key in ("sync_owner", "review_owner"):
            value = ownership.get(key)
            if isinstance(value, str) and value.strip():
                owners.add(value.strip())
        patch_owners = ownership.get("product_patch_owners")
        if isinstance(patch_owners, list):
            for value in patch_owners:
                if isinstance(value, str) and value.strip():
                    owners.add(value.strip())
    return sorted(owners)


def validate_conflict_owner(value: object, state: dict[str, Any], label: str) -> str:
    owner = require_nonempty(value, label)
    owners = state.get("conflict_owners")
    if not isinstance(owners, list) or owner not in owners:
        raise SyncTrainError(f"{label} is not an owner allowed by sync policy")
    return owner


def utc_now() -> str:
    """Return the schema's RFC-3339 UTC representation."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def validate_resolution(value: object, label: str) -> str:
    result = require_nonempty(value, label)
    if RESOLUTION_RE.fullmatch(result) is None:
        raise SyncTrainError(
            f"{label} must be a lowercase token matching [a-z][a-z0-9._-]*"
        )
    return result


def initial_gates(owner: str = "BleedingDev") -> list[dict[str, Any]]:
    return [
        {
            "id": gate_id,
            "phase": GATE_PHASES[gate_id],
            "owner": owner,
            "required": True,
            "status": "not_run",
            "command": f"{gate_id} (not recorded)",
            "evidence": ["not run"],
        }
        for gate_id in GATE_ORDER
    ]


def decode(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def git(
    repo: Path,
    arguments: Sequence[str],
    *,
    allowed_statuses: frozenset[int] = frozenset({0}),
    input_data: bytes | None = None,
    index_file: Path | None = None,
) -> subprocess.CompletedProcess[bytes]:
    """Run Git with a fixed argv, timeout, and bounded failure detail."""

    argv = ["git", "-C", str(repo), *[str(argument) for argument in arguments]]
    environment = None
    if index_file is not None:
        environment = os.environ.copy()
        environment["GIT_INDEX_FILE"] = str(index_file)
    try:
        result = subprocess.run(
            argv,
            input=input_data,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise SyncTrainError(f"Git command could not run: {type(error).__name__}") from error
    if result.returncode not in allowed_statuses:
        detail = bounded(decode(result.stderr) or decode(result.stdout) or "no diagnostic")
        shown = " ".join(argv[1:])
        raise SyncTrainError(f"git {shown} exited {result.returncode}: {detail}")
    return result


def git_text(
    repo: Path,
    arguments: Sequence[str],
    *,
    allowed_statuses: frozenset[int] = frozenset({0}),
    input_data: bytes | None = None,
    index_file: Path | None = None,
) -> str:
    return decode(
        git(
            repo,
            arguments,
            allowed_statuses=allowed_statuses,
            input_data=input_data,
            index_file=index_file,
        ).stdout
    ).strip()


def resolve_repo(path: Path) -> Path:
    candidate = path.expanduser().resolve()
    if not candidate.exists() or not candidate.is_dir():
        raise SyncTrainError(f"repository does not exist: {candidate}")
    top = git_text(candidate, ["rev-parse", "--show-toplevel"])
    return Path(top).resolve()


def normalize_branch_ref(value: str, label: str) -> str:
    value = require_nonempty(value, label)
    if not value.startswith("refs/"):
        value = f"refs/heads/{value}"
    return value


def validate_ref(repo: Path, value: str, label: str) -> str:
    value = require_nonempty(value, label)
    result = git(
        repo,
        ["check-ref-format", value],
        allowed_statuses=frozenset({0, 1}),
    )
    if result.returncode != 0:
        raise SyncTrainError(f"{label} is not a valid Git ref: {value!r}")
    return value


def resolve_direct_ref(repo: Path, ref: str, label: str, *, missing_ok: bool = False) -> str | None:
    result = git(
        repo,
        ["show-ref", "--verify", "--hash", ref],
        # Git versions differ for a syntactically valid but absent nested
        # ref: some return 1, while others return 128 with a "not a valid
        # ref" diagnostic.  The ref was already checked with
        # check-ref-format, so both are ordinary absence here.
        allowed_statuses=frozenset({0, 1, 128}),
    )
    if result.returncode != 0:
        if missing_ok:
            return None
        raise SyncTrainError(f"{label} {ref!r} does not resolve to a local ref")
    return require_sha(decode(result.stdout).strip(), label)


def resolve_commit(repo: Path, reference: str, label: str) -> str:
    value = git_text(
        repo,
        ["rev-parse", "--verify", "--end-of-options", f"{reference}^{{commit}}"],
    )
    return require_sha(value, label)


def is_ancestor(repo: Path, ancestor: str, descendant: str, label: str) -> None:
    result = git(
        repo,
        ["merge-base", "--is-ancestor", ancestor, descendant],
        allowed_statuses=frozenset({0, 1}),
    )
    if result.returncode == 1:
        raise SyncTrainError(f"{label}: {ancestor} is not an ancestor of {descendant}")


def merge_base(repo: Path, left: str, right: str, label: str) -> str:
    result = git(
        repo,
        ["merge-base", left, right],
        allowed_statuses=frozenset({0, 1}),
    )
    if result.returncode == 1:
        raise SyncTrainError(f"{label}: commits have no common ancestry")
    return require_sha(decode(result.stdout).strip(), label)


def source_relationship(repo: Path, floor_sha: str, source_sha: str) -> tuple[str, str]:
    base = merge_base(repo, floor_sha, source_sha, "source merge base")
    if base == floor_sha:
        return "descendant_of_floor", base
    if base == source_sha:
        return "behind_floor", base
    return "diverged_from_floor", base


def repo_relative(repo: Path, value: str | Path, label: str) -> str:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = repo / candidate
    candidate = candidate.resolve()
    root = repo.resolve()
    try:
        relative = candidate.relative_to(root)
    except ValueError as error:
        raise SyncTrainError(f"{label} must resolve inside the repository") from error
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise SyncTrainError(f"{label} must name a repository-relative file")
    return relative.as_posix()


def load_policy(repo: Path, policy_argument: str | Path) -> tuple[dict[str, Any], str]:
    policy_path = repo_relative(repo, policy_argument, "sync policy")
    policy = load_json(repo / policy_path, "sync policy")
    if policy.get("schema_version") != SCHEMA_VERSION:
        raise SyncTrainError("sync policy schema_version must be 1")
    if policy.get("authority") != "product-owned":
        raise SyncTrainError("sync policy authority must be product-owned")
    if policy.get("policy_revision") != POLICY_REVISION:
        raise SyncTrainError("sync policy revision must be sync-train.v1")
    workflow = policy.get("workflow")
    if not isinstance(workflow, dict) or workflow.get("name") != WORKFLOW_NAME:
        raise SyncTrainError("sync policy workflow name is not run-upstream-sync-train")
    if workflow.get("receipt_schema_path") != "product/upstream/sync-train-receipt.schema.json":
        raise SyncTrainError("sync policy receipt schema path is not canonical")
    receipt_template = workflow.get("receipt_path_template")
    if not isinstance(receipt_template, str) or "{train_id}" not in receipt_template:
        raise SyncTrainError("sync policy receipt path template must contain {train_id}")
    if workflow.get("sync_branch_template") != "refs/heads/sync/upstream/{candidate_short_sha}":
        raise SyncTrainError("sync policy sync branch template is not canonical")
    if workflow.get("candidate_must_be_immutable_sha") is not True:
        raise SyncTrainError("sync policy must require immutable candidate SHAs")
    if workflow.get("moving_refs_are_discovery_only") is not True:
        raise SyncTrainError("sync policy must treat moving refs as discovery only")
    strategies = workflow.get("allowed_strategies")
    if not isinstance(strategies, list) or "merge" not in strategies:
        raise SyncTrainError("sync policy must allow the merge strategy")
    refs = policy.get("refs")
    if not isinstance(refs, dict):
        raise SyncTrainError("sync policy refs must be an object")
    product_branch = require_nonempty(refs.get("product_branch"), "sync policy product branch")
    sync_prefix = require_nonempty(refs.get("sync_branch_prefix"), "sync policy sync branch prefix")
    if not product_branch.startswith("refs/heads/") or not sync_prefix.startswith("refs/heads/") or not sync_prefix.endswith("/"):
        raise SyncTrainError("sync policy refs must use full heads refs")
    discovery = refs.get("upstream_discovery")
    if not isinstance(discovery, list) or not discovery or any(
        not isinstance(ref, str)
        or not ref.startswith("refs/remotes/")
        or validate_ref(repo, ref, "sync policy upstream discovery ref") != ref
        for ref in discovery
    ):
        raise SyncTrainError("sync policy upstream discovery refs are invalid")
    floor = policy.get("floor")
    if not isinstance(floor, dict):
        raise SyncTrainError("sync policy floor must be an object")
    require_nonempty(floor.get("metadata"), "sync policy floor metadata")
    require_sha(floor.get("sha"), "sync policy floor SHA")
    if floor.get("immutable_until_approved_train") is not True:
        raise SyncTrainError("sync policy floor must be immutable until an approved train")
    preflight = policy.get("preflight")
    if not isinstance(preflight, dict) or preflight.get("requires_clean_worktree") is not True or preflight.get("requires_floor_ancestor") is not True:
        raise SyncTrainError("sync policy preflight must require a clean tree and floor ancestry")
    forbidden = preflight.get("forbidden_direct_targets")
    if not isinstance(forbidden, list) or any(
        not isinstance(ref, str) or not ref.startswith("refs/")
        for ref in forbidden
    ):
        raise SyncTrainError("sync policy forbidden direct targets are invalid")
    if any(ref not in forbidden for ref in ("refs/heads/main", "refs/heads/master")):
        raise SyncTrainError("sync policy must forbid direct main/master targets")
    if product_branch in forbidden:
        raise SyncTrainError("sync policy product branch is a forbidden direct target")
    conflicts = policy.get("conflicts")
    if (
        not isinstance(conflicts, dict)
        or conflicts.get("required_fields") != [
        "path",
        "source",
        "owner",
        "resolution",
        "rationale",
        ]
        or conflicts.get("receipt_required_even_when_empty") is not True
        or conflicts.get("unresolved_conflict_is_terminal_failure") is not True
    ):
        raise SyncTrainError("sync policy conflict receipt fields have drifted")
    gates = policy.get("gates")
    if not isinstance(gates, dict) or gates.get("upstream_required_first") is not True or gates.get("fail_closed") is not True:
        raise SyncTrainError("sync policy gates must be ordered and fail closed")
    if gates.get("required_order") != list(GATE_ORDER) or gates.get("required_gate_status") != "passed":
        raise SyncTrainError("sync policy gate order/status has drifted")
    finalization = policy.get("finalization")
    cas = finalization.get("cas") if isinstance(finalization, dict) else None
    if (
        not isinstance(finalization, dict)
        or finalization.get("method") != "compare_and_swap"
        or finalization.get("sync_train_publication_target") != "isolated_sync_ref"
        or finalization.get("released_branch_update_in_this_workflow") != "unchanged"
        or finalization.get("force_update_allowed") is not False
        or finalization.get("non_fast_forward_update_allowed") is not False
        or not isinstance(cas, dict)
        or cas.get("required") is not True
    ):
        raise SyncTrainError("sync policy finalization must use compare-and-swap without force")
    compare_refs = cas.get("compare_refs")
    if not isinstance(compare_refs, list) or product_branch not in compare_refs:
        raise SyncTrainError("sync policy CAS must compare the configured product branch")
    if cas.get("compare_values") != ["product.starting_head_sha", "product.starting_floor_sha"]:
        raise SyncTrainError("sync policy CAS must compare product head and floor")
    released_refs = finalization.get("released_refs")
    if not isinstance(released_refs, list) or product_branch not in released_refs:
        raise SyncTrainError("sync policy released refs must include the product branch")
    bundle = finalization.get("same_commit_bundle")
    if (
        not isinstance(bundle, dict)
        or bundle.get("required") is not True
        or bundle.get("members") != ["code", "floor_metadata", "convergence_receipt"]
        or bundle.get("metadata_path") != floor["metadata"]
        or bundle.get("receipt_schema_path") != "product/upstream/sync-train-receipt.schema.json"
    ):
        raise SyncTrainError("sync policy same-commit bundle is invalid")
    return policy, policy_path


def train_directory(value: Path) -> Path:
    path = value.expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise SyncTrainError(f"train directory is not a directory: {path}")
    return path


def state_path(train_dir: Path) -> Path:
    return train_dir / "state.json"


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def json_bytes(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def write_json(path: Path, value: dict[str, Any]) -> None:
    atomic_write(path, json_bytes(value))


def temporary_index(repo: Path, train_dir: Path) -> Path:
    """Copy the live index so finalize can stage without a pre-CAS mutation."""

    index_value = git_text(repo, ["rev-parse", "--git-path", "index"])
    index_path = Path(index_value)
    if not index_path.is_absolute():
        index_path = repo / index_path
    if not index_path.exists():
        raise SyncTrainError("the repository index is missing")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".sync-train-index.", dir=train_dir)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        shutil.copyfile(index_path, temporary)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise
    return temporary


def hash_blob(repo: Path, data: bytes) -> str:
    return require_sha(
        git_text(repo, ["hash-object", "-w", "--stdin"], input_data=data),
        "receipt blob SHA",
    )


def stage_blob(repo: Path, index_file: Path, path: str, data: bytes) -> None:
    blob = hash_blob(repo, data)
    git(
        repo,
        ["update-index", "--add", "--cacheinfo", "100644", blob, path],
        index_file=index_file,
    )


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SyncTrainError(f"could not load {label}: {error}") from error
    if type(value) is not dict:
        raise SyncTrainError(f"{label} must be a JSON object")
    return value


def load_state(train_dir: Path) -> dict[str, Any]:
    path = state_path(train_dir)
    state = load_json(path, "sync-train state")
    if state.get("schema_version") != SCHEMA_VERSION or state.get("kind") != STATE_KIND:
        raise SyncTrainError("sync-train state schema or kind is unsupported")
    status = state.get("status")
    if status not in {"prepared", "conflicted", "failed", "finalized", "aborted"}:
        raise SyncTrainError("sync-train state has an invalid status")
    for key in ("product_head_sha", "source_sha", "sync_base_sha"):
        require_sha(state.get(key), f"sync-train state.{key}")
    require_nonempty(state.get("product_branch"), "sync-train state.product_branch")
    require_nonempty(state.get("source_ref"), "sync-train state.source_ref")
    require_nonempty(state.get("sync_ref"), "sync-train state.sync_ref")
    require_nonempty(state.get("floor_metadata"), "sync-train state.floor_metadata")
    require_sha(state.get("floor_sha"), "sync-train state.floor_sha")
    if state.get("bead_id") != TRAIN_BEAD_ID:
        raise SyncTrainError(f"sync-train state.bead_id must be {TRAIN_BEAD_ID}")
    if state.get("policy_revision") != POLICY_REVISION:
        raise SyncTrainError("sync-train state policy revision is unsupported")
    require_repository(state.get("product_repository"), "sync-train state.product_repository")
    require_repository(state.get("upstream_repository"), "sync-train state.upstream_repository")
    require_nonempty(state.get("policy_path"), "sync-train state.policy_path")
    for key in ("started_at", "selected_at"):
        require_nonempty(state.get(key), f"sync-train state.{key}")
    if state.get("strategy") != "merge":
        raise SyncTrainError("sync-train state.strategy must be 'merge'")
    conflicts = state.get("conflicts", [])
    if not isinstance(conflicts, list) or len(conflicts) > MAX_CONFLICTS:
        raise SyncTrainError("sync-train state.conflicts must be a bounded array")
    detected = state.get("detected_conflict_paths", [])
    if not isinstance(detected, list) or any(
        not isinstance(path, str) or not path.strip() for path in detected
    ) or len(set(detected)) != len(detected):
        raise SyncTrainError("sync-train state.detected_conflict_paths must be unique paths")
    owners = state.get("conflict_owners")
    if not isinstance(owners, list) or not owners or any(
        not isinstance(owner, str) or not owner.strip() for owner in owners
    ) or len(set(owners)) != len(owners):
        raise SyncTrainError("sync-train state.conflict_owners must be a unique owner list")
    gates = state.get("gates")
    if not isinstance(gates, list) or len(gates) != len(GATE_ORDER):
        raise SyncTrainError("sync-train state.gates must contain the required ordered gates")
    gate_ids = [gate.get("id") if isinstance(gate, dict) else None for gate in gates]
    if gate_ids != list(GATE_ORDER):
        raise SyncTrainError("sync-train state.gates are not in the required policy order")
    for gate in gates:
        if not isinstance(gate, dict):
            raise SyncTrainError("every sync-train gate must be an object")
        require_nonempty(gate.get("owner"), f"gate {gate.get('id')} owner")
        if gate.get("required") is not True:
            raise SyncTrainError(f"gate {gate.get('id')} must be required")
        if gate.get("phase") != GATE_PHASES[gate["id"]]:
            raise SyncTrainError(f"gate {gate['id']} has the wrong phase")
        if gate.get("status") not in {"passed", "failed", "skipped", "not_run"}:
            raise SyncTrainError(f"gate {gate['id']} has an invalid status")
        require_nonempty(gate.get("command"), f"gate {gate['id']} command")
        evidence = gate.get("evidence")
        if not isinstance(evidence, list) or any(
            not isinstance(item, str) or not item.strip() for item in evidence
        ):
            raise SyncTrainError(f"gate {gate['id']} evidence must be non-empty strings")
    return state


def floor_sha_from_bytes(data: bytes, label: str) -> str:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SyncTrainError(f"{label} is not valid UTF-8 JSON: {error}") from error
    if type(value) is not dict:
        raise SyncTrainError(f"{label} must be a JSON object")
    candidates: list[object] = []
    for key in ("pinned_floor_sha", "floor_sha"):
        if key in value:
            candidates.append(value[key])
    for key in ("pinned_floor", "floor"):
        nested = value.get(key)
        if isinstance(nested, dict) and "sha" in nested:
            candidates.append(nested["sha"])
    valid = [item for item in candidates if isinstance(item, str) and SHA_RE.fullmatch(item)]
    if not valid:
        raise SyncTrainError(
            f"{label} does not contain a recognized 40-character floor SHA"
        )
    if len(set(valid)) != 1:
        raise SyncTrainError(f"{label} contains conflicting floor SHA values")
    return valid[0]


def update_floor_bytes(data: bytes, old_floor: str, new_floor: str, label: str) -> bytes:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SyncTrainError(f"{label} is not valid UTF-8 JSON: {error}") from error
    if type(value) is not dict:
        raise SyncTrainError(f"{label} must be a JSON object")

    locations: list[tuple[dict[str, Any], str]] = []
    for key in ("pinned_floor_sha", "floor_sha"):
        if key in value:
            locations.append((value, key))
    for key in ("pinned_floor", "floor"):
        nested = value.get(key)
        if isinstance(nested, dict) and "sha" in nested:
            locations.append((nested, "sha"))
    if not locations:
        raise SyncTrainError(f"{label} does not contain a recognized floor SHA")
    current_values = [mapping[key] for mapping, key in locations]
    if any(item not in {old_floor, new_floor} for item in current_values):
        raise SyncTrainError(f"{label} floor SHA changed outside this sync train")
    for mapping, key in locations:
        mapping[key] = new_floor
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def blob_bytes(repo: Path, commit: str, path: str) -> bytes | None:
    existence = git(
        repo,
        ["cat-file", "-e", f"{commit}:{path}"],
        allowed_statuses=frozenset({0, 1}),
    )
    if existence.returncode == 1:
        return None
    return git(repo, ["show", f"{commit}:{path}"]).stdout


def blob_sha(repo: Path, commit: str, path: str) -> str | None:
    data = blob_bytes(repo, commit, path)
    if data is None:
        return None
    return require_sha(git_text(repo, ["rev-parse", f"{commit}:{path}"]), "source blob SHA")


def upstream_rename_source_paths(
    repo: Path,
    product_sha: str,
    source_sha: str,
    conflict_path: str,
) -> set[str]:
    """Return candidate-tree paths Git relates to a rename conflict path."""

    fields = git(
        repo,
        ["diff", "--name-status", "-z", "-M", product_sha, source_sha],
    ).stdout.split(b"\0")
    allowed: set[str] = set()
    index = 0
    while index < len(fields) and fields[index]:
        status = decode(fields[index])
        index += 1
        if status.startswith(("R", "C")):
            if index + 1 >= len(fields):
                raise SyncTrainError("Git returned a truncated rename record")
            old_path = decode(fields[index])
            new_path = decode(fields[index + 1])
            index += 2
            if conflict_path in {old_path, new_path}:
                allowed.add(new_path)
        else:
            index += 1
    return allowed


def metadata_from_commit(repo: Path, commit: str, path: str) -> bytes:
    data = blob_bytes(repo, commit, path)
    if data is None:
        raise SyncTrainError(f"floor metadata {path!r} is absent from product head")
    return data


def current_branch(repo: Path) -> str | None:
    result = git(
        repo,
        ["symbolic-ref", "--quiet", "HEAD"],
        allowed_statuses=frozenset({0, 1}),
    )
    if result.returncode == 1:
        return None
    return decode(result.stdout).strip()


def status_bytes(repo: Path, *, index_file: Path | None = None) -> bytes:
    return git(
        repo,
        ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        index_file=index_file,
    ).stdout


def status_records(repo: Path, *, index_file: Path | None = None) -> list[str]:
    values = status_bytes(repo, index_file=index_file).split(b"\0")
    records: list[str] = []
    for raw in values:
        if not raw:
            continue
        records.append(decode(raw))
    return records


def unmerged_paths(repo: Path, *, index_file: Path | None = None) -> list[str]:
    raw = git(repo, ["ls-files", "-u", "-z", "--"], index_file=index_file).stdout
    paths: set[str] = set()
    for record in raw.split(b"\0"):
        if not record:
            continue
        _, separator, path = record.partition(b"\t")
        if separator:
            paths.add(decode(path))
    return sorted(paths)


def merge_head_path(repo: Path) -> Path:
    value = git_text(repo, ["rev-parse", "--git-path", "MERGE_HEAD"])
    path = Path(value)
    return path if path.is_absolute() else repo / path


def merge_heads(repo: Path) -> list[str]:
    path = merge_head_path(repo)
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except FileNotFoundError:
        return []
    except OSError as error:
        raise SyncTrainError(f"could not inspect merge state: {error}") from error
    return [require_sha(line.strip(), "MERGE_HEAD") for line in lines if line.strip()]


def conflict_entry(
    repo: Path,
    *,
    path: str,
    source_path: str,
    source_ref: str,
    source_sha: str,
    product_sha: str,
) -> dict[str, Any]:
    return {
        "path": path,
        "source": {
            "commit_sha": source_sha,
            "sha": source_sha,
            "ref": source_ref,
            "path": source_path,
            "blob_sha": blob_sha(repo, source_sha, source_path),
        },
        "owner": None,
        "resolution": None,
        "rationale": None,
        "product_blob_sha": blob_sha(repo, product_sha, path),
    }


def sorted_conflicts(conflicts: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(conflicts, key=lambda item: str(item.get("path", "")).encode("utf-8"))


def assert_product_unchanged(repo: Path, state: dict[str, Any]) -> str:
    product_sha = resolve_direct_ref(repo, state["product_branch"], "product branch")
    assert product_sha is not None
    if product_sha != state["product_head_sha"]:
        raise SyncTrainError(
            "product branch moved since the train was prepared; refusing the operation"
        )
    return product_sha


def assert_sync_head(repo: Path, state: dict[str, Any]) -> str:
    sync_sha = resolve_direct_ref(repo, state["sync_ref"], "sync branch")
    if sync_sha is None:
        raise SyncTrainError("sync branch no longer exists")
    if sync_sha != state["sync_base_sha"]:
        raise SyncTrainError("sync branch moved since the train was prepared")
    return sync_sha


def make_receipt(
    state: dict[str, Any],
    *,
    completed_at: str,
    terminal_state: str,
    terminal_reason: str,
    cas_attempted: bool,
    cas_result: str,
    released_head_sha: str | None,
    released_update_mode: str,
    released_old_sha: str | None,
    released_new_sha: str | None,
) -> dict[str, Any]:
    conflicts: list[dict[str, Any]] = []
    for entry in validate_conflicts(state):
        source = entry["source"]
        source_sha = require_sha(source["commit_sha"], "conflict source SHA")
        source_ref = require_nonempty(source["ref"], "conflict source ref")
        source_path = require_nonempty(source["path"], "conflict source path")
        path = require_nonempty(entry["path"], "conflict path")
        conflicts.append(
            {
                "path": path,
                "source": f"{source_ref}:{source_sha}",
                "owner": require_nonempty(entry["owner"], f"conflict {path} owner"),
                "resolution": validate_resolution(
                    entry["resolution"], f"conflict {path} resolution"
                ),
                "rationale": require_nonempty(
                    entry["rationale"], f"conflict {path} rationale"
                ),
                "source_path": source_path,
                "source_sha": source_sha,
            }
        )
    gates = validate_gates(state, require_passed=terminal_state == "succeeded")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": RECEIPT_SCHEMA_URL,
        "contract_id": RECEIPT_CONTRACT_ID,
        "schema_version": SCHEMA_VERSION,
        "receipt_id": f"sync-train-{state['source_sha'][:12]}",
        "bead_id": state["bead_id"],
        "workflow": {
            "name": WORKFLOW_NAME,
            "policy_path": "product/upstream/sync-policy.json",
            "receipt_schema_path": "product/upstream/sync-train-receipt.schema.json",
        },
        "policy_revision": state["policy_revision"],
        "started_at": state["started_at"],
        "completed_at": completed_at,
        "product": {
            "repository": state["product_repository"],
            "starting_ref": state["product_branch"],
            "released_ref": state["product_branch"],
            "starting_head_sha": state["product_head_sha"],
            "starting_floor_sha": state["floor_sha"],
            "floor_metadata_path": state["floor_metadata"],
        },
        "upstream": {
            "repository": state["upstream_repository"],
            "candidate_ref": state["source_ref"],
            "candidate_sha": state["source_sha"],
            "selected_at": state["selected_at"],
        },
        "sync": {
            "branch_ref": state["sync_ref"],
            "base_product_head_sha": state["sync_base_sha"],
            "base_floor_sha": state["floor_sha"],
            "strategy": state["strategy"],
            "created_at": state["started_at"],
        },
        "conflicts": conflicts,
        "gates": gates,
        "terminal": {"state": terminal_state, "reason": terminal_reason},
        "finalization": {
            "outcome": "published" if terminal_state == "succeeded" else "not_published",
            # A commit cannot embed its own SHA. The actual sync head is
            # recorded in durable train state and command output after CAS.
            "sync_ref": state["sync_ref"] if terminal_state == "succeeded" else None,
            "sync_head_sha": None,
            "released_ref": state["product_branch"],
            "released_head_sha": released_head_sha,
            "released_ref_update": {
                "mode": released_update_mode,
                "old_sha": released_old_sha,
                "new_sha": released_new_sha,
            },
            "cas": {
                "required": True,
                "attempted": cas_attempted,
                "result": cas_result,
                "expected_product_head_sha": state["product_head_sha"],
                "observed_product_head_sha": released_head_sha,
            },
            "same_commit": {
                "required": True,
                "verified": terminal_state == "succeeded",
                # The receipt is itself a member of the bundle, so embedding
                # the containing commit's SHA would be a cryptographic
                # self-reference.  The schema permits null here; the exact
                # bundle SHA is recorded in workflow state and command output
                # after the CAS publishes it, while the finalizer verifies
                # membership from the resulting tree.
                "bundle_commit_sha": None,
                "members": ["code", "floor_metadata", "convergence_receipt"],
            },
        },
    }


def write_terminal_receipt(
    train_dir: Path,
    state: dict[str, Any],
    *,
    terminal_state: str,
    reason: str,
    released_head_sha: str,
) -> Path:
    """Persist terminal failure/abort evidence outside canonical Git state."""

    terminal_state_copy = json.loads(json.dumps(state))
    fallback_owner = terminal_state_copy["conflict_owners"][0]
    for entry in terminal_state_copy["conflicts"]:
        entry["owner"] = entry.get("owner") or fallback_owner
        entry["resolution"] = entry.get("resolution") or "unresolved"
        entry["rationale"] = entry.get("rationale") or reason
    receipt = make_receipt(
        terminal_state_copy,
        completed_at=utc_now(),
        terminal_state=terminal_state,
        terminal_reason=reason,
        cas_attempted=False,
        cas_result="not_attempted",
        released_head_sha=released_head_sha,
        released_update_mode="unchanged",
        released_old_sha=released_head_sha,
        released_new_sha=released_head_sha,
    )
    path = train_dir / "terminal-receipt.json"
    write_json(path, receipt)
    return path


def ensure_no_untracked(repo: Path, *, allowed_paths: set[str] | None = None) -> None:
    allowed_paths = allowed_paths or set()
    for path in status_records(repo):
        if path.startswith("?? "):
            relative = path[3:]
            if relative not in allowed_paths:
                raise SyncTrainError(f"untracked file is outside the train: {relative}")


def conflict_values(entry: dict[str, Any], path: str, state: dict[str, Any]) -> tuple[str, str, str]:
    if not isinstance(entry, dict):
        raise SyncTrainError(f"conflict record for {path!r} must be an object")
    owner = validate_conflict_owner(entry.get("owner"), state, f"conflict {path} owner")
    resolution = validate_resolution(entry.get("resolution"), f"conflict {path} resolution")
    rationale = require_nonempty(entry.get("rationale"), f"conflict {path} rationale")
    return owner, resolution, rationale


def validate_conflicts(state: dict[str, Any], repo: Path | None = None) -> list[dict[str, Any]]:
    """Validate complete conflict provenance and reject duplicate sources/paths."""

    conflicts = state.get("conflicts")
    if not isinstance(conflicts, list):
        raise SyncTrainError("sync-train conflicts must be an array")
    detected = state.get("detected_conflict_paths", [])
    if not isinstance(detected, list):
        raise SyncTrainError("sync-train detected conflict paths must be an array")
    detected_paths = set(detected)
    seen_paths: set[str] = set()
    seen_sources: set[tuple[str, str]] = set()
    validated: list[dict[str, Any]] = []
    for entry in conflicts:
        if not isinstance(entry, dict):
            raise SyncTrainError("every conflict receipt must be an object")
        path = require_nonempty(entry.get("path"), "conflict path")
        if path not in detected_paths:
            raise SyncTrainError(
                f"conflict {path!r} was not produced by the pinned upstream merge"
            )
        if path in seen_paths:
            raise SyncTrainError(f"duplicate conflict path {path!r}")
        seen_paths.add(path)
        source = entry.get("source")
        if not isinstance(source, dict):
            raise SyncTrainError(f"conflict {path!r} is missing original source")
        source_sha = require_sha(source.get("commit_sha"), f"conflict {path} source commit")
        if source.get("sha") != source_sha or source_sha != state["source_sha"]:
            raise SyncTrainError(f"conflict {path!r} does not retain the pinned upstream source")
        source_path = require_nonempty(source.get("path"), f"conflict {path} source path")
        if source.get("ref") != state["source_ref"]:
            raise SyncTrainError(f"conflict {path!r} does not retain the pinned upstream ref")
        source_key = (source_sha, source_path)
        if source_key in seen_sources:
            raise SyncTrainError(
                f"duplicate original upstream source {source_path!r} at {source_sha}"
            )
        seen_sources.add(source_key)
        conflict_values(entry, path, state)
        if repo is not None:
            expected_source_blob = blob_sha(repo, source_sha, source_path)
            if source.get("blob_sha") != expected_source_blob:
                raise SyncTrainError(
                    f"conflict {path!r} original upstream blob provenance does not match Git"
                )
            expected_product_blob = blob_sha(repo, state["product_head_sha"], path)
            if entry.get("product_blob_sha") != expected_product_blob:
                raise SyncTrainError(
                    f"conflict {path!r} product blob provenance does not match Git"
                )
        validated.append(entry)
    if seen_paths != detected_paths:
        missing = sorted(detected_paths - seen_paths)
        extra = sorted(seen_paths - detected_paths)
        detail = f"missing={missing!r}" if missing else f"extra={extra!r}"
        raise SyncTrainError(f"conflict records do not exactly cover Git's merge conflicts ({detail})")
    return sorted_conflicts(validated)


def validate_gates(state: dict[str, Any], *, require_passed: bool) -> list[dict[str, Any]]:
    gates = state.get("gates")
    if not isinstance(gates, list) or [gate.get("id") for gate in gates if isinstance(gate, dict)] != list(GATE_ORDER):
        raise SyncTrainError("sync-train gates are not in the required policy order")
    for index, gate in enumerate(gates):
        if not isinstance(gate, dict):
            raise SyncTrainError("every sync-train gate must be an object")
        if require_passed and gate.get("status") != "passed":
            raise SyncTrainError(
                f"required gate {GATE_ORDER[index]!r} is {gate.get('status')!r}; all ordered gates must pass"
            )
    return gates


def prepare(args: argparse.Namespace) -> dict[str, Any]:
    repo = resolve_repo(Path(args.repo or "."))
    train_dir = train_directory(Path(args.train_dir))
    existing = state_path(train_dir)
    if existing.exists():
        previous = load_state(train_dir)
        if previous.get("status") not in {"aborted", "finalized"}:
            raise SyncTrainError("train directory already contains an active sync train")

    policy, policy_path = load_policy(repo, args.policy)
    policy_refs = policy["refs"]
    configured_product_branch = policy_refs["product_branch"]
    product_branch_value = args.product_branch or configured_product_branch
    product_branch = validate_ref(
        repo,
        normalize_branch_ref(product_branch_value, "product branch"),
        "product branch",
    )
    if product_branch != configured_product_branch:
        raise SyncTrainError("configured product branch differs from sync policy")
    sync_prefix = args.sync_prefix or policy_refs["sync_branch_prefix"]
    if sync_prefix != policy_refs["sync_branch_prefix"]:
        raise SyncTrainError("configured sync branch prefix differs from sync policy")
    source_ref = validate_ref(
        repo,
        require_nonempty(args.source_ref, "upstream source ref"),
        "upstream source ref",
    )
    if source_ref not in policy_refs["upstream_discovery"]:
        raise SyncTrainError("upstream source ref is not an approved policy discovery ref")
    product_sha = resolve_direct_ref(repo, product_branch, "product branch")
    assert product_sha is not None
    if args.product_head is not None:
        expected_product = require_sha(args.product_head, "configured product head")
        if expected_product != product_sha:
            raise SyncTrainError("configured product head does not match product branch")
    source_sha = resolve_direct_ref(repo, source_ref, "upstream source ref")
    assert source_sha is not None
    floor_path = repo_relative(
        repo,
        args.floor_metadata or policy["floor"]["metadata"],
        "floor metadata",
    )
    if floor_path != policy["floor"]["metadata"]:
        raise SyncTrainError("configured floor metadata differs from sync policy")
    floor_data = metadata_from_commit(repo, product_sha, floor_path)
    floor_sha = floor_sha_from_bytes(floor_data, "floor metadata")
    if floor_sha != policy["floor"]["sha"]:
        raise SyncTrainError("floor metadata SHA differs from sync policy")
    is_ancestor(repo, floor_sha, product_sha, "floor ancestry check failed")
    relationship, source_base = source_relationship(repo, floor_sha, source_sha)
    if relationship != "descendant_of_floor":
        raise SyncTrainError(
            "upstream source must descend from the pinned floor before a sync train can start"
        )

    short_sha = source_sha[:12]
    sync_ref = validate_ref(repo, f"{sync_prefix}{short_sha}", "sync branch")
    if resolve_direct_ref(repo, sync_ref, "sync branch", missing_ok=True) is not None:
        raise SyncTrainError(f"sync branch already exists: {sync_ref}")
    if status_bytes(repo):
        raise SyncTrainError("working tree must be clean before preparing a sync train")

    template = policy["workflow"]["receipt_path_template"]
    train_id = f"sync-train-{short_sha}"
    try:
        receipt_path_value = template.format(train_id=train_id, short_sha=short_sha)
    except (KeyError, ValueError) as error:
        raise SyncTrainError(f"sync policy receipt template is invalid: {error}") from error
    receipt_path = repo_relative(repo, receipt_path_value, "convergence receipt")
    if args.receipt_path is not None:
        requested_receipt = repo_relative(repo, args.receipt_path, "convergence receipt")
        if requested_receipt != receipt_path:
            raise SyncTrainError("configured receipt path differs from sync policy")
    if receipt_path == floor_path:
        raise SyncTrainError("floor metadata and convergence receipt must be different files")

    started_at = utc_now()
    remotes = policy.get("remotes")
    if not isinstance(remotes, dict):
        raise SyncTrainError("sync policy remotes must be an object")
    product_remote = remotes.get("product")
    upstream_remote = remotes.get("upstream")
    if not isinstance(product_remote, dict) or not isinstance(upstream_remote, dict):
        raise SyncTrainError("sync policy remotes are invalid")
    product_repository = require_repository(
        product_remote.get("repository"), "sync policy product repository"
    )
    upstream_repository = require_repository(
        upstream_remote.get("repository"), "sync policy upstream repository"
    )
    bead_id = require_nonempty(args.bead_id, "bead id")
    if bead_id != TRAIN_BEAD_ID:
        raise SyncTrainError(f"bead id must be {TRAIN_BEAD_ID}")

    branch_name = sync_ref.removeprefix("refs/heads/")
    # Use the already-resolved exact commit as the start point.  This avoids
    # branch-name ambiguity in Git versions that treat a full refs/heads name
    # as a second switch target, while still proving the configured product
    # branch was the source above.
    git(repo, ["switch", "--create", branch_name, product_sha])
    if current_branch(repo) != sync_ref:
        raise SyncTrainError("Git did not attach the newly created isolated sync branch")

    state: dict[str, Any] = {
        "$schema": "tracedecay-upstream-sync-train.schema.json",
        "schema_version": SCHEMA_VERSION,
        "kind": STATE_KIND,
        "status": "prepared",
        "repo": str(repo),
        "bead_id": bead_id,
        "policy_revision": policy["policy_revision"],
        "policy_path": policy_path,
        "product_repository": product_repository,
        "upstream_repository": upstream_repository,
        "product_branch": product_branch,
        "product_head_sha": product_sha,
        "source_ref": source_ref,
        "source_sha": source_sha,
        "source_relationship": relationship,
        "source_merge_base": source_base,
        "sync_ref": sync_ref,
        "sync_base_sha": product_sha,
        "floor_metadata": floor_path,
        "floor_sha": floor_sha,
        "floor_metadata_sha256": hashlib.sha256(floor_data).hexdigest(),
        "receipt_path": receipt_path,
        "conflicts": [],
        "detected_conflict_paths": [],
        "conflict_owners": policy_conflict_owners(policy),
        "gates": initial_gates(
            str(policy.get("ownership", {}).get("sync_owner", "BleedingDev"))
            if isinstance(policy.get("ownership"), dict)
            else "BleedingDev"
        ),
        "started_at": started_at,
        "selected_at": started_at,
        "strategy": "merge",
        "merge_in_progress": False,
    }
    write_json(state_path(train_dir), state)

    result = git(
        repo,
        ["merge", "--no-commit", "--no-ff", "--no-edit", "--no-stat", source_sha],
        allowed_statuses=frozenset({0, 1}),
    )
    paths = unmerged_paths(repo)
    if result.returncode == 1 and not paths:
        # A non-conflict merge failure is not a usable train.  Clean the
        # isolated branch while leaving the product branch untouched.
        git(
            repo,
            ["merge", "--abort"],
            allowed_statuses=frozenset({0, 1}),
        )
        raise SyncTrainError("upstream merge failed without Git conflict entries")
    if paths:
        state["status"] = "conflicted"
        state["merge_in_progress"] = True
        state["detected_conflict_paths"] = list(paths)
        state["conflicts"] = sorted_conflicts(
            conflict_entry(
                repo,
                path=path,
                source_path=path,
                source_ref=source_ref,
                source_sha=source_sha,
                product_sha=product_sha,
            )
            for path in paths
        )
    else:
        state["merge_in_progress"] = bool(merge_heads(repo))
    write_json(state_path(train_dir), state)
    return {
        "ok": True,
        "action": "prepare",
        "status": state["status"],
        "product_branch": product_branch,
        "product_head_sha": product_sha,
        "source_ref": source_ref,
        "source_sha": source_sha,
        "sync_ref": sync_ref,
        "sync_head_sha": resolve_direct_ref(repo, sync_ref, "sync branch"),
        "floor_sha": floor_sha,
        "conflict_paths": [entry["path"] for entry in state["conflicts"]],
        "train_dir": str(train_dir),
    }


def state_repo(args: argparse.Namespace, state: dict[str, Any]) -> Path:
    requested = Path(args.repo) if args.repo is not None else Path(state["repo"])
    repo = resolve_repo(requested)
    if Path(state["repo"]).resolve() != repo:
        raise SyncTrainError("requested repository differs from the repository recorded in state")
    return repo


def validate_state_policy(repo: Path, state: dict[str, Any]) -> dict[str, Any]:
    policy, _ = load_policy(repo, state["policy_path"])
    refs = policy["refs"]
    if refs["product_branch"] != state["product_branch"]:
        raise SyncTrainError("sync policy product branch changed during the train")
    if refs["sync_branch_prefix"] != state["sync_ref"].rsplit("/", 1)[0] + "/":
        raise SyncTrainError("sync policy sync branch prefix changed during the train")
    if state["source_ref"] not in refs["upstream_discovery"]:
        raise SyncTrainError("pinned source ref is no longer approved by sync policy")
    workflow = policy["workflow"]
    expected_sync_ref = workflow["sync_branch_template"].format(
        candidate_short_sha=state["source_sha"][:12]
    )
    if state["sync_ref"] != expected_sync_ref:
        raise SyncTrainError("sync branch does not match the configured policy template")
    if policy["floor"]["metadata"] != state["floor_metadata"] or policy["floor"]["sha"] != state["floor_sha"]:
        raise SyncTrainError("sync policy floor changed during the train")
    if policy_conflict_owners(policy) != state.get("conflict_owners"):
        raise SyncTrainError("sync policy conflict owners changed during the train")
    return policy


def record_conflict(args: argparse.Namespace) -> dict[str, Any]:
    train_dir = train_directory(Path(args.train_dir))
    state = load_state(train_dir)
    if state["status"] not in {"prepared", "conflicted"}:
        raise SyncTrainError(f"cannot record a conflict in a {state['status']} train")
    repo = state_repo(args, state)
    validate_state_policy(repo, state)
    assert_product_unchanged(repo, state)
    if current_branch(repo) != state["sync_ref"]:
        raise SyncTrainError("conflict recording requires the isolated sync branch")
    path = repo_relative(repo, args.path, "conflict path")
    source_path = repo_relative(repo, args.source_path or path, "original upstream source path")
    owner = validate_conflict_owner(args.owner, state, "conflict owner")
    resolution = validate_resolution(args.resolution, "conflict resolution")
    rationale = require_nonempty(args.rationale, "conflict rationale")
    conflicts = list(state["conflicts"])
    selected: dict[str, Any] | None = None
    for entry in conflicts:
        if entry.get("path") == path:
            selected = entry
            break
    if selected is None:
        raise SyncTrainError(
            f"conflict {path!r} was not produced by the pinned upstream merge"
        )
    else:
        source = selected.get("source")
        if (
            not isinstance(source, dict)
            or source.get("commit_sha") != state["source_sha"]
            or source.get("sha") != state["source_sha"]
        ):
            raise SyncTrainError(f"conflict {path!r} has an invalid original source")
        if source_path != source.get("path"):
            allowed_sources = upstream_rename_source_paths(
                repo,
                state["product_head_sha"],
                state["source_sha"],
                path,
            )
            if source_path not in allowed_sources:
                raise SyncTrainError(
                    "corrected original source path is not Git rename provenance for the conflict"
                )
            source_blob = blob_sha(repo, state["source_sha"], source_path)
            if source_blob is None:
                raise SyncTrainError(
                    "corrected original source path does not exist at the pinned upstream SHA"
                )
            source["path"] = source_path
            source["blob_sha"] = source_blob
    selected["owner"] = owner
    selected["resolution"] = resolution
    selected["rationale"] = rationale
    # Re-validate the complete set after updating the selected entry.  This
    # keeps state edits fail-closed and makes synthetic or duplicate conflict
    # paths impossible even if state.json was hand-edited.
    state["conflicts"] = sorted_conflicts(conflicts)
    validate_conflicts(state, repo)
    state["status"] = "conflicted"
    write_json(state_path(train_dir), state)
    return {
        "ok": True,
        "action": "record-conflict",
        "status": state["status"],
        "path": path,
        "source": selected["source"],
        "owner": owner,
        "resolution": resolution,
        "rationale": rationale,
        "train_dir": str(train_dir),
    }


def parse_command_json(value: str) -> list[str]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise SyncTrainError(f"gate command must be a JSON argv array: {error}") from error
    if (
        not isinstance(parsed, list)
        or not parsed
        or any(not isinstance(item, str) or not item or "\0" in item for item in parsed)
    ):
        raise SyncTrainError("gate command must be a non-empty JSON array of argv strings")
    return parsed


def execute_gate(command: list[str]) -> tuple[int, list[str]]:
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=300,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return 1, [bounded(f"gate command could not run: {type(error).__name__}")]
    output = bounded(decode(result.stdout) or decode(result.stderr) or f"exit code {result.returncode}")
    return result.returncode, [output]


def record_gate(args: argparse.Namespace) -> dict[str, Any]:
    train_dir = train_directory(Path(args.train_dir))
    state = load_state(train_dir)
    if state["status"] not in {"prepared", "conflicted"}:
        raise SyncTrainError(f"cannot record a gate in a {state['status']} train")
    repo = state_repo(args, state)
    validate_state_policy(repo, state)
    assert_product_unchanged(repo, state)
    if current_branch(repo) != state["sync_ref"]:
        raise SyncTrainError("gate recording requires the isolated sync branch")
    unresolved = unmerged_paths(repo)
    if unresolved:
        raise SyncTrainError(
            "required gates cannot run while Git conflicts remain unresolved: "
            + ", ".join(unresolved[:16])
        )
    validate_conflicts(state, repo)
    gate_id = require_nonempty(args.id, "gate id")
    if gate_id not in GATE_ORDER:
        raise SyncTrainError(f"unknown required gate {gate_id!r}")
    index = GATE_ORDER.index(gate_id)
    gates = state["gates"]
    if any(gates[offset]["status"] == "passed" for offset in range(index + 1, len(gates))):
        raise SyncTrainError(
            f"gate {gate_id!r} cannot be re-recorded after a later ordered gate passed"
        )
    if any(gates[offset]["status"] != "passed" for offset in range(index)):
        raise SyncTrainError(
            f"gate {gate_id!r} cannot run before all earlier required gates pass"
        )
    command: list[str]
    if args.command_json is not None:
        command = parse_command_json(args.command_json)
        exit_code, evidence = execute_gate(command)
        status = "passed" if exit_code == 0 else "failed"
        if args.evidence:
            evidence.extend(require_nonempty(item, "gate evidence") for item in args.evidence)
    else:
        if args.status is None:
            raise SyncTrainError("record-gate requires --command-json or an explicit --status")
        status = args.status
        evidence = [require_nonempty(item, "gate evidence") for item in (args.evidence or [])]
        if not evidence:
            raise SyncTrainError("record-gate requires non-empty evidence")
        command = (
            parse_command_json(args.command_argv)
            if args.command_argv
            else ["external-evidence", gate_id]
        )
        exit_code = 0 if status == "passed" else 1
    gate = gates[index]
    gate["command"] = json.dumps(command, separators=(",", ":"))
    gate["status"] = status
    gate["evidence"] = evidence
    gate["exit_code"] = exit_code
    state["gates"] = gates
    state["last_gate_id"] = gate_id
    if status != "passed":
        state["status"] = "failed"
    write_json(state_path(train_dir), state)
    terminal_receipt: Path | None = None
    if status != "passed":
        terminal_receipt = write_terminal_receipt(
            train_dir,
            state,
            terminal_state="failed",
            reason=f"required gate {gate_id!r} failed",
            released_head_sha=state["product_head_sha"],
        )
    result = {
        "ok": status == "passed",
        "action": "record-gate",
        "gate": gate,
        "status": state["status"],
        "train_dir": str(train_dir),
        "terminal_receipt": str(terminal_receipt) if terminal_receipt else None,
    }
    if status != "passed":
        # Keep the failed gate in durable workflow state, but fail closed so a
        # caller cannot mistake recorded evidence for a successful train.
        raise SyncTrainError(f"required gate {gate_id!r} failed")
    return result


def abort(args: argparse.Namespace) -> dict[str, Any]:
    train_dir = train_directory(Path(args.train_dir))
    state = load_state(train_dir)
    if state["status"] == "aborted":
        return {"ok": True, "action": "abort", "status": "aborted", "train_dir": str(train_dir)}
    if state["status"] == "finalized":
        raise SyncTrainError("a finalized sync train cannot be aborted")
    repo = state_repo(args, state)
    product_sha = assert_product_unchanged(repo, state)
    sync_sha = resolve_direct_ref(repo, state["sync_ref"], "sync branch", missing_ok=True)
    if sync_sha is not None and sync_sha != state["sync_base_sha"]:
        raise SyncTrainError("sync branch moved; refusing to discard a raced train")

    if current_branch(repo) == state["sync_ref"]:
        paths = status_records(repo)
        if any(path.startswith("?? ") for path in paths):
            raise SyncTrainError("abort will not discard untracked files in the sync worktree")
        git(repo, ["reset", "--hard", product_sha])
        if current_branch(repo) != state["sync_ref"]:
            raise SyncTrainError("sync worktree detached unexpectedly during abort")
        git(repo, ["switch", state["product_branch"].removeprefix("refs/heads/")])

    product_metadata = metadata_from_commit(repo, product_sha, state["floor_metadata"])
    expected_digest = state.get("floor_metadata_sha256")
    if expected_digest != hashlib.sha256(product_metadata).hexdigest():
        raise SyncTrainError("product floor metadata changed; refusing to call the train aborted")
    if current_branch(repo) == state["product_branch"]:
        try:
            current_metadata = (repo / state["floor_metadata"]).read_bytes()
        except OSError as error:
            raise SyncTrainError(f"could not read product floor metadata: {error}") from error
        if current_metadata != product_metadata:
            raise SyncTrainError("abort would leave floor metadata bytes changed")

    if sync_sha is not None:
        git(
            repo,
            ["update-ref", "-d", state["sync_ref"], state["sync_base_sha"]],
        )
    state["status"] = "aborted"
    state["invalidated"] = True
    state["merge_in_progress"] = False
    write_json(state_path(train_dir), state)
    terminal_receipt = write_terminal_receipt(
        train_dir,
        state,
        terminal_state="aborted",
        reason="sync train aborted before isolated publication",
        released_head_sha=product_sha,
    )
    return {
        "ok": True,
        "action": "abort",
        "status": "aborted",
        "product_branch": state["product_branch"],
        "product_head_sha": product_sha,
        "sync_ref_removed": sync_sha is not None,
        "floor_metadata_sha256": expected_digest,
        "train_dir": str(train_dir),
        "terminal_receipt": str(terminal_receipt),
    }


def finalize(args: argparse.Namespace) -> dict[str, Any]:
    train_dir = train_directory(Path(args.train_dir))
    state = load_state(train_dir)
    if state["status"] not in {"prepared", "conflicted"}:
        raise SyncTrainError(f"cannot finalize a {state['status']} train")
    repo = state_repo(args, state)
    validate_state_policy(repo, state)
    product_sha = assert_product_unchanged(repo, state)
    if current_branch(repo) != state["sync_ref"]:
        raise SyncTrainError("finalize requires the isolated sync branch to be checked out")
    sync_sha = assert_sync_head(repo, state)
    source_sha = resolve_direct_ref(repo, state["source_ref"], "upstream source ref")
    if source_sha != state["source_sha"]:
        raise SyncTrainError("upstream source ref moved since it was pinned")
    merge_heads_now = merge_heads(repo)
    if merge_heads_now and state["source_sha"] not in merge_heads_now:
        raise SyncTrainError("merge state does not name the pinned upstream source")
    unresolved = unmerged_paths(repo)
    if unresolved:
        raise SyncTrainError("unresolved Git conflicts remain: " + ", ".join(unresolved[:16]))
    conflicts = validate_conflicts(state, repo)
    validate_gates(state, require_passed=True)
    ensure_no_untracked(repo)

    metadata_path = repo / state["floor_metadata"]
    try:
        current_metadata = metadata_path.read_bytes()
    except OSError as error:
        raise SyncTrainError(f"could not read floor metadata in sync worktree: {error}") from error
    floor_after = update_floor_bytes(
        current_metadata,
        state["floor_sha"],
        state["source_sha"],
        "floor metadata",
    )
    receipt_path = repo / state["receipt_path"]
    if receipt_path == metadata_path:
        raise SyncTrainError("floor metadata and convergence receipt must be different files")
    receipt_path.parent.mkdir(parents=True, exist_ok=True)

    parents = [sync_sha]
    if merge_heads_now:
        if state["source_sha"] not in merge_heads_now:
            raise SyncTrainError("pending merge source differs from pinned source")
        parents.append(state["source_sha"])
    elif state["source_sha"] != sync_sha:
        # If a caller applied the source in an earlier commit, preserving the
        # source as a parent is only safe when Git can prove that relationship.
        is_ancestor(repo, state["source_sha"], sync_sha, "sync branch/source relationship")
    # Build the publication tree in a private index.  Neither the live index
    # nor floor/receipt files are changed until the ref CAS succeeds.
    index_file = temporary_index(repo, train_dir)
    try:
        git(repo, ["add", "--all", "--", "."], index_file=index_file)
        if unmerged_paths(repo, index_file=index_file):
            raise SyncTrainError("unresolved Git conflicts remain after staging")
        stage_blob(repo, index_file, state["floor_metadata"], floor_after)
        receipt = make_receipt(
            state,
            completed_at=utc_now(),
            terminal_state="succeeded",
            terminal_reason="all required ordered gates passed and the isolated sync ref was published",
            cas_attempted=True,
            cas_result="matched_and_published",
            released_head_sha=product_sha,
            released_update_mode="unchanged",
            released_old_sha=product_sha,
            released_new_sha=product_sha,
        )
        receipt_data = json_bytes(receipt)
        stage_blob(repo, index_file, state["receipt_path"], receipt_data)
        tree_sha = require_sha(
            git_text(repo, ["write-tree"], index_file=index_file), "sync tree SHA"
        )
        commit_message = args.message or f"converge upstream {state['source_sha'][:12]}"
        commit_message = require_nonempty(commit_message, "commit message")
        commit_args: list[str] = ["commit-tree", tree_sha]
        for parent in parents:
            commit_args.extend(["-p", parent])
        commit_sha = require_sha(
            git_text(repo, commit_args, input_data=(commit_message + "\n").encode("utf-8")),
            "sync commit SHA",
        )

        # Verify both moving inputs and update only the isolated branch in one
        # compare-and-swap transaction.  A race leaves the product branch and
        # its floor untouched; the generated commit remains an unreachable
        # retry aid.
        transaction = (
            "start\n"
            f"verify {state['product_branch']} {product_sha}\n"
            f"verify {state['source_ref']} {state['source_sha']}\n"
            f"update {state['sync_ref']} {commit_sha} {sync_sha}\n"
            "prepare\n"
            "commit\n"
        ).encode("utf-8")
        try:
            git(repo, ["update-ref", "--stdin"], input_data=transaction)
        except SyncTrainError as error:
            state["last_failure"] = {
                "kind": "compare_and_swap",
                "message": bounded(str(error)),
            }
            write_json(state_path(train_dir), state)
            raise
    finally:
        index_file.unlink(missing_ok=True)

    git(repo, ["reset", "--hard", commit_sha])
    state["status"] = "finalized"
    state["merge_in_progress"] = False
    state["final_commit_sha"] = commit_sha
    state["final_tree_sha"] = tree_sha
    state["conflicts"] = sorted_conflicts(conflicts)
    state["completed_at"] = receipt["completed_at"]
    write_json(state_path(train_dir), state)
    return {
        "ok": True,
        "action": "finalize",
        "status": "finalized",
        "product_branch": state["product_branch"],
        "product_head_sha": product_sha,
        "sync_ref": state["sync_ref"],
        "sync_head_sha": commit_sha,
        "source_sha": state["source_sha"],
        "floor_before_sha": state["floor_sha"],
        "floor_after_sha": state["source_sha"],
        "receipt_path": state["receipt_path"],
        "tree_sha": tree_sha,
        "train_dir": str(train_dir),
    }


def inspect(args: argparse.Namespace) -> dict[str, Any]:
    train_dir = train_directory(Path(args.train_dir))
    state = load_state(train_dir)
    repo = state_repo(args, state)
    observed_product = resolve_direct_ref(repo, state["product_branch"], "product branch", missing_ok=True)
    observed_sync = resolve_direct_ref(repo, state["sync_ref"], "sync branch", missing_ok=True)
    return {
        "ok": observed_product == state["product_head_sha"]
        and (state["status"] in {"aborted", "finalized"} or observed_sync is not None),
        "action": "inspect",
        "state": state,
        "observed": {
            "product_head_sha": observed_product,
            "sync_head_sha": observed_sync,
            "current_branch": current_branch(repo),
            "unresolved_paths": unmerged_paths(repo),
        },
        "train_dir": str(train_dir),
    }


def add_repo_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repo", type=Path, default=argparse.SUPPRESS)


def add_train_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--train-dir", type=Path, required=True)


def parser_for() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=None)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="create and merge into an isolated sync branch")
    add_repo_argument(prepare_parser)
    add_train_argument(prepare_parser)
    prepare_parser.add_argument("--product-branch")
    prepare_parser.add_argument("--product-head")
    prepare_parser.add_argument("--source-ref", "--upstream-ref", dest="source_ref", required=True)
    prepare_parser.add_argument("--floor-metadata", "--metadata", dest="floor_metadata")
    prepare_parser.add_argument("--receipt-path", "--receipt", dest="receipt_path")
    prepare_parser.add_argument("--sync-prefix")
    prepare_parser.add_argument("--policy", default=DEFAULT_POLICY)
    prepare_parser.add_argument("--bead-id", default=TRAIN_BEAD_ID)

    conflict_parser = subparsers.add_parser("record-conflict", help="record an owned conflict resolution")
    add_repo_argument(conflict_parser)
    add_train_argument(conflict_parser)
    conflict_parser.add_argument("--path", required=True)
    conflict_parser.add_argument("--source-path")
    conflict_parser.add_argument("--owner", required=True)
    conflict_parser.add_argument("--resolution", required=True)
    conflict_parser.add_argument("--rationale", required=True)

    gate_parser = subparsers.add_parser("record-gate", help="execute or record one ordered required gate")
    add_repo_argument(gate_parser)
    add_train_argument(gate_parser)
    gate_parser.add_argument("--id", required=True)
    gate_parser.add_argument("--command-json", help="JSON argv array to execute without a shell")
    gate_parser.add_argument(
        "--command",
        dest="command_argv",
        help="JSON argv array for externally executed evidence",
    )
    gate_parser.add_argument(
        "--status",
        choices=("passed", "failed", "skipped", "not_run"),
    )
    gate_parser.add_argument("--evidence", action="append")

    abort_parser = subparsers.add_parser("abort", help="invalidate and remove an unfinalized train")
    add_repo_argument(abort_parser)
    add_train_argument(abort_parser)

    finalize_parser = subparsers.add_parser("finalize", help="atomically publish code, floor, and receipt")
    add_repo_argument(finalize_parser)
    add_train_argument(finalize_parser)
    finalize_parser.add_argument("--message")

    inspect_parser = subparsers.add_parser("inspect", help="inspect state and observed refs")
    add_repo_argument(inspect_parser)
    add_train_argument(inspect_parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = parser_for()
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = prepare(args)
        elif args.command == "record-conflict":
            result = record_conflict(args)
        elif args.command == "record-gate":
            result = record_gate(args)
        elif args.command == "abort":
            result = abort(args)
        elif args.command == "finalize":
            result = finalize(args)
        elif args.command == "inspect":
            result = inspect(args)
        else:  # pragma: no cover - argparse enforces the subcommand set.
            raise SyncTrainError(f"unknown command: {args.command}")
    except SyncTrainError as error:
        print(json.dumps({"ok": False, "error": bounded(str(error))}, indent=2, sort_keys=True))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
