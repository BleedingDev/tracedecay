#!/usr/bin/env python3
"""Run a product-owned, isolated upstream convergence train.

The train is deliberately ref-oriented.  ``prepare`` creates a new branch at
the configured product branch and starts a non-committing merge of one exact
upstream commit.  Conflict decisions live in ``state.json`` in the caller's
train directory.  ``advance-floor`` rewrites the canonical metadata and every
declared floor pin in the sync worktree and records the resulting candidate
tree SHA; every ``record-gate`` is bound to that exact tree; ``publish``
commits the gated tree plus the convergence receipt and publishes that one
commit with a Git compare-and-swap transaction.  The product branch is only
ever verified; it is never an update target.

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
import shlex
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
BEAD_ID_RE = re.compile(r"^tdmem-[0-9]{4}$")
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
PIN_KINDS = frozenset({"json_pointer", "anchored_line", "derived_metadata_receipt"})
JSON_PIN_KINDS = frozenset({"json_pointer", "derived_metadata_receipt"})
FLOOR_PLACEHOLDER = "{floor}"
MAX_FLOOR_PINS = 64
MAX_PIN_OCCURRENCES = 64
SHA_TOKEN_RE = re.compile(r"[0-9a-f]{40}")
GATE_TIMEOUT_SECONDS = 300
GATE_STATUSES = frozenset({"passed", "failed", "skipped", "not_run", "in_progress"})
GATE_COMMAND_SOURCES = frozenset({"executed", "external_command", "ci_run"})
MAX_LANE_COMMANDS = 64
WORKFLOW_JOB_RE = re.compile(r"^[a-z][a-z0-9_]*$")
CI_RUN_URL_RE = re.compile(
    r"^https://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/actions/runs/([0-9]{1,20})(?:/[A-Za-z0-9_./-]*)?$"
)
# The exact shell prelude the CI lanes run their commands under.
LANE_SHELL_PRELUDE = ("bash", "-euo", "pipefail", "-c")
TRAIN_STATUSES = frozenset(
    {"prepared", "conflicted", "advanced", "failed", "finalized", "aborted", "rolled_back"}
)
TERMINAL_STATUSES = frozenset({"aborted", "finalized", "rolled_back"})
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


def policy_train_bead_id(policy: dict[str, Any]) -> str:
    """Return the policy's single authority for advancing the pinned floor."""

    floor = policy.get("floor")
    workflow = policy.get("workflow")
    floor_authority = require_nonempty(
        floor.get("advancement_authority") if isinstance(floor, dict) else None,
        "sync policy floor advancement authority",
    )
    workflow_authority = require_nonempty(
        workflow.get("first_floor_advancement_bead")
        if isinstance(workflow, dict)
        else None,
        "sync policy first floor advancement bead",
    )
    if not BEAD_ID_RE.fullmatch(floor_authority):
        raise SyncTrainError(
            "sync policy floor advancement authority must be a tdmem bead id"
        )
    if not BEAD_ID_RE.fullmatch(workflow_authority):
        raise SyncTrainError(
            "sync policy first floor advancement bead must be a tdmem bead id"
        )
    if floor_authority != workflow_authority:
        raise SyncTrainError(
            "sync policy floor advancement authority differs from workflow authority"
        )
    return floor_authority


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
            "tree_sha": None,
            "commands": [],
            "coverage": None,
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


def load_policy(
    repo: Path, policy_argument: str | Path, *, source_commit: str | None = None
) -> tuple[dict[str, Any], str]:
    """Load the sync policy from the worktree or, once a train has been
    published, from the released product head so the advanced sync tree
    cannot redefine the authority that governs its own withdrawal."""

    policy_path = repo_relative(repo, policy_argument, "sync policy")
    if source_commit is None:
        policy = load_json(repo / policy_path, "sync policy")
    else:
        policy_data = blob_bytes(repo, source_commit, policy_path)
        if policy_data is None:
            raise SyncTrainError(f"sync policy {policy_path!r} is absent from the product head")
        try:
            policy = json.loads(policy_data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise SyncTrainError(f"sync policy is not valid UTF-8 JSON: {error}") from error
        if type(policy) is not dict:
            raise SyncTrainError("sync policy must be a JSON object")
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
    policy_gate_lanes(policy)
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
    policy_train_bead_id(policy)
    policy_floor_pins(policy, policy_path)
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
    """Copy the live index so tree hashing and publish never touch the live index."""

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


def load_state(train_dir: Path, *, expected_bead_id: str | None = None) -> dict[str, Any]:
    path = state_path(train_dir)
    state = load_json(path, "sync-train state")
    if state.get("schema_version") != SCHEMA_VERSION or state.get("kind") != STATE_KIND:
        raise SyncTrainError("sync-train state schema or kind is unsupported")
    status = state.get("status")
    if status not in TRAIN_STATUSES:
        raise SyncTrainError("sync-train state has an invalid status")
    pins = state.get("floor_pins", [])
    if not isinstance(pins, list) or len(pins) > MAX_FLOOR_PINS or any(
        not isinstance(pin, dict)
        or not isinstance(pin.get("path"), str)
        or pin.get("kind") not in PIN_KINDS
        or not isinstance(pin.get("occurrences"), int)
        or not isinstance(pin.get("each_occurrences"), int)
        for pin in pins
    ):
        raise SyncTrainError("sync-train state.floor_pins must be bounded pin records")
    if status in {"advanced", "finalized", "rolled_back"}:
        require_sha(state.get("candidate_tree_sha"), "sync-train state.candidate_tree_sha")
        require_sha(state.get("candidate_commit_sha"), "sync-train state.candidate_commit_sha")
        parents = state.get("candidate_parents")
        if not isinstance(parents, list) or not parents or len(parents) > 2:
            raise SyncTrainError("sync-train state.candidate_parents must name one or two parents")
        for parent in parents:
            require_sha(parent, "sync-train state.candidate_parents entry")
    archival = state.get("archival_provenance", [])
    if not isinstance(archival, list) or len(archival) > MAX_FLOOR_PINS or any(
        not isinstance(entry, dict)
        or not isinstance(entry.get("path"), str)
        or not isinstance(entry.get("reason"), str)
        for entry in archival
    ):
        raise SyncTrainError("sync-train state.archival_provenance must be bounded records")
    for key in ("product_head_sha", "source_sha", "sync_base_sha"):
        require_sha(state.get(key), f"sync-train state.{key}")
    require_nonempty(state.get("product_branch"), "sync-train state.product_branch")
    require_nonempty(state.get("source_ref"), "sync-train state.source_ref")
    require_nonempty(state.get("sync_ref"), "sync-train state.sync_ref")
    require_nonempty(state.get("floor_metadata"), "sync-train state.floor_metadata")
    require_sha(state.get("floor_sha"), "sync-train state.floor_sha")
    bead_id = require_nonempty(state.get("bead_id"), "sync-train state.bead_id")
    if not BEAD_ID_RE.fullmatch(bead_id):
        raise SyncTrainError("sync-train state.bead_id must be a tdmem bead id")
    if expected_bead_id is not None and bead_id != expected_bead_id:
        raise SyncTrainError(
            f"sync-train state.bead_id must be {expected_bead_id} from sync policy"
        )
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
        if gate.get("status") not in GATE_STATUSES:
            raise SyncTrainError(f"gate {gate['id']} has an invalid status")
        require_nonempty(gate.get("command"), f"gate {gate['id']} command")
        validate_gate_command_records(gate)
        evidence = gate.get("evidence")
        if not isinstance(evidence, list) or any(
            not isinstance(item, str) or not item.strip() for item in evidence
        ):
            raise SyncTrainError(f"gate {gate['id']} evidence must be non-empty strings")
        if gate.get("tree_sha") is not None:
            require_sha(gate.get("tree_sha"), f"gate {gate['id']} tree_sha")
        elif gate.get("status") == "passed":
            raise SyncTrainError(f"gate {gate['id']} passed without a bound tree SHA")
    return state


def validate_gate_command_records(gate: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate the per-command records that carry a gate's proof."""

    gate_id = gate.get("id")
    records = gate.get("commands", [])
    if not isinstance(records, list) or len(records) > MAX_LANE_COMMANDS:
        raise SyncTrainError(f"gate {gate_id} commands must be a bounded array")
    seen: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            raise SyncTrainError(f"gate {gate_id} command records must be objects")
        command = normalize_command(record.get("command"), f"gate {gate_id} command record")
        if record.get("source") not in GATE_COMMAND_SOURCES:
            raise SyncTrainError(f"gate {gate_id} command {command!r} has an invalid source")
        if record.get("status") not in {"passed", "failed"}:
            raise SyncTrainError(f"gate {gate_id} command {command!r} has an invalid status")
        if record.get("status") == "passed" and command in seen:
            raise SyncTrainError(f"gate {gate_id} command {command!r} passed twice")
        if record.get("status") == "passed":
            seen.add(command)
        require_sha(record.get("tree_sha"), f"gate {gate_id} command {command!r} tree_sha")
        exit_code = record.get("exit_code")
        if exit_code is not None and (type(exit_code) is not int or exit_code < 0):
            raise SyncTrainError(f"gate {gate_id} command {command!r} exit_code is invalid")
        evidence = record.get("evidence")
        if not isinstance(evidence, list) or not evidence or any(
            not isinstance(item, str) or not item.strip() for item in evidence
        ):
            raise SyncTrainError(f"gate {gate_id} command {command!r} evidence must be non-empty")
        require_nonempty(record.get("recorded_at"), f"gate {gate_id} command {command!r} recorded_at")
    coverage = gate.get("coverage")
    if coverage is not None:
        if (
            not isinstance(coverage, dict)
            or type(coverage.get("declared")) is not int
            or type(coverage.get("passed")) is not int
            or not isinstance(coverage.get("missing"), list)
        ):
            raise SyncTrainError(f"gate {gate_id} coverage record is invalid")
        if gate.get("status") == "passed" and coverage["missing"]:
            raise SyncTrainError(
                f"gate {gate_id} is passed while declared commands are missing: "
                + ", ".join(str(item) for item in coverage["missing"][:8])
            )
    elif gate.get("status") == "passed":
        raise SyncTrainError(f"gate {gate_id} passed without a lane coverage record")
    return records


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


def canonical_json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2) + "\n").encode("utf-8")


def canonical_json_value(data: bytes, label: str) -> Any:
    """Parse ``data`` (key order preserved) and prove it is canonical JSON.

    Structured pins are rewritten by re-serializing the parsed document, so a
    file that is not already in the serializer's two-space format would be
    reformatted wholesale and the floor move would hide inside an unrelated
    diff.  Refuse such a file instead of reformatting it.
    """

    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SyncTrainError(f"{label} is not valid UTF-8 JSON: {error}") from error
    if canonical_json_bytes(value) != data:
        raise SyncTrainError(
            f"{label} is not canonical two-space JSON; refusing to reformat a floor pin"
        )
    return value


def advance_floor_value(
    data: bytes, old_floor: str, new_floor: str, label: str
) -> dict[str, Any]:
    """Parse ``data`` (key order preserved) and move every floor SHA it pins."""

    value = canonical_json_value(data, label)
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
    return value


def json_pointer_tokens(pointer: object, label: str) -> list[str]:
    if (
        not isinstance(pointer, str)
        or not pointer.startswith("/")
        or len(pointer) > MAX_FIELD_CHARS
    ):
        raise SyncTrainError(f"{label} must be an absolute JSON pointer")
    return [token.replace("~1", "/").replace("~0", "~") for token in pointer[1:].split("/")]


def json_pointer_child(pointer: str, token: str) -> str:
    return pointer + "/" + token.replace("~", "~0").replace("/", "~1")


def json_pointer_step(container: Any, token: str, pointer: str, label: str) -> Any:
    if isinstance(container, dict):
        if token not in container:
            raise SyncTrainError(f"{label}: JSON pointer {pointer!r} names a missing key {token!r}")
        return container[token]
    if isinstance(container, list):
        if not token.isdecimal() or int(token) >= len(container):
            raise SyncTrainError(f"{label}: JSON pointer {pointer!r} has an invalid index {token!r}")
        return container[int(token)]
    raise SyncTrainError(f"{label}: JSON pointer {pointer!r} descends into a scalar at {token!r}")


def expand_json_pointer(
    value: Any, pointer: str, label: str
) -> list[tuple[Any, str | int, str]]:
    """Resolve ``pointer`` to ``(container, key, exact_pointer)`` triples.

    A ``*`` segment ranges over every key of an object or index of an array,
    so a policy can name ``/entries/*/last_verified_upstream_sha`` without
    hard-coding how many entries the map holds today.
    """

    tokens = json_pointer_tokens(pointer, label)
    results: list[tuple[Any, str | int, str]] = []

    def walk(container: Any, index: int, prefix: str) -> None:
        token = tokens[index]
        last = index == len(tokens) - 1
        if token == "*":
            if isinstance(container, dict):
                keys: list[str | int] = list(container.keys())
            elif isinstance(container, list):
                keys = list(range(len(container)))
            else:
                raise SyncTrainError(
                    f"{label}: JSON pointer {pointer!r} wildcards a scalar at {prefix!r}"
                )
            for key in keys:
                child = json_pointer_child(prefix, str(key))
                if last:
                    results.append((container, key, child))
                else:
                    walk(container[key], index + 1, child)
            return
        child_value = json_pointer_step(container, token, pointer, label)
        key = int(token) if isinstance(container, list) else token
        child = json_pointer_child(prefix, token)
        if last:
            results.append((container, key, child))
        else:
            walk(child_value, index + 1, child)

    walk(value, 0, "")
    return results


def resolve_json_pointer(value: Any, pointer: str, label: str) -> tuple[Any, str | int]:
    if "*" in json_pointer_tokens(pointer, label):
        raise SyncTrainError(f"{label}: JSON pointer {pointer!r} must not contain wildcards")
    (container, key, _), = expand_json_pointer(value, pointer, label)
    return container, key


def normalize_floor_pin(entry: object, metadata_path: str) -> dict[str, Any]:
    """Validate one declared floor pin into its structured targets."""

    if not isinstance(entry, dict):
        raise SyncTrainError("every sync policy floor pin must be an object")
    path = require_nonempty(entry.get("path"), "sync policy floor pin path")
    if path.startswith("/") or ".." in path.split("/") or path != path.strip():
        raise SyncTrainError(f"sync policy floor pin path is not repo-relative: {path!r}")
    if path == metadata_path:
        raise SyncTrainError("canonical floor metadata must not be declared as a pin")
    kind = entry.get("kind")
    if kind not in PIN_KINDS:
        raise SyncTrainError(f"sync policy floor pin {path!r} has unknown kind {kind!r}")
    occurrences = entry.get("occurrences")
    if (
        isinstance(occurrences, bool)
        or not isinstance(occurrences, int)
        or occurrences < 1
        or occurrences > MAX_PIN_OCCURRENCES
    ):
        raise SyncTrainError(
            f"sync policy floor pin {path!r} must declare its exact floor occurrences"
        )
    pin: dict[str, Any] = {"path": path, "kind": kind, "occurrences": occurrences}
    allowed_keys = {"path", "kind", "occurrences"}
    label = f"sync policy floor pin {path!r}"
    if kind in JSON_PIN_KINDS:
        pointers = entry.get("pointers")
        if (
            not isinstance(pointers, list)
            or not pointers
            or len(pointers) > MAX_PIN_OCCURRENCES
            or len(set(map(str, pointers))) != len(pointers)
        ):
            raise SyncTrainError(f"{label} must declare unique advanced JSON pointers")
        for pointer in pointers:
            if "*" in json_pointer_tokens(pointer, f"{label} pointer"):
                raise SyncTrainError(f"{label} advanced pointers must not contain wildcards")
        if len(pointers) != occurrences:
            raise SyncTrainError(
                f"{label} occurrences must equal the number of advanced pointers"
            )
        pin["pointers"] = list(pointers)
        allowed_keys.add("pointers")
        if kind == "derived_metadata_receipt":
            pin["metadata_pointer"] = entry.get("metadata_pointer")
            pin["blob_pointer"] = entry.get("blob_pointer")
            for key in ("metadata_pointer", "blob_pointer"):
                if "*" in json_pointer_tokens(pin[key], f"{label} {key}"):
                    raise SyncTrainError(f"{label} {key} must not contain wildcards")
            allowed_keys.update({"metadata_pointer", "blob_pointer"})
            pin["each_pointers"] = []
            pin["each_reason"] = None
        else:
            each = entry.get("each_pointers", [])
            if not isinstance(each, list) or len(each) > MAX_PIN_OCCURRENCES or any(
                not isinstance(item, str) for item in each
            ):
                raise SyncTrainError(f"{label} each_pointers must be a bounded pointer list")
            for pointer in each:
                if "*" not in json_pointer_tokens(pointer, f"{label} each pointer"):
                    raise SyncTrainError(f"{label} each_pointers must contain a * segment")
            if len(set(each)) != len(each):
                raise SyncTrainError(f"{label} each_pointers must be unique")
            reason = entry.get("each_reason")
            if each:
                reason = require_nonempty(reason, f"{label} each_reason")
            elif reason is not None:
                raise SyncTrainError(f"{label} each_reason requires each_pointers")
            pin["each_pointers"] = list(each)
            pin["each_reason"] = reason
            allowed_keys.update({"each_pointers", "each_reason"})
    else:
        # The anchored line is matched byte-exactly, including indentation,
        # so it is deliberately not whitespace-normalized.
        line = entry.get("line")
        if not isinstance(line, str) or not line.strip() or len(line) > MAX_FIELD_CHARS:
            raise SyncTrainError(f"{label} line must be a non-empty string")
        if "\n" in line or "\r" in line or line.count(FLOOR_PLACEHOLDER) != 1:
            raise SyncTrainError(
                f"{label} line must be one line containing {FLOOR_PLACEHOLDER} exactly once"
            )
        pin["line"] = line
        allowed_keys.add("line")
    unknown = sorted(set(entry) - allowed_keys)
    if unknown:
        raise SyncTrainError(f"{label} has undeclared fields: {', '.join(unknown)}")
    return pin


def policy_floor_pins(
    policy: dict[str, Any], policy_path: str
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Return the declared structured floor pins and archival provenance.

    Every file that hard-pins the accepted floor SHA must be declared here so
    that ``advance-floor`` moves all of them inside the one candidate tree.
    Each pin names its exact targets (JSON pointers or one anchored line) and
    its exact occurrence count; any other occurrence of the floor in a pin,
    or any undeclared file carrying the floor, fails closed.  The canonical
    metadata file is handled separately and must not be listed.  Archival
    provenance records keep the SHA they were produced against and are only
    reported.
    """

    floor = policy["floor"]
    metadata_path = floor["metadata"]
    raw_pins = floor.get("pins")
    if not isinstance(raw_pins, list) or not raw_pins or len(raw_pins) > MAX_FLOOR_PINS:
        raise SyncTrainError("sync policy floor.pins must be a non-empty bounded array")
    pins: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in raw_pins:
        pin = normalize_floor_pin(entry, metadata_path)
        if pin["path"] in seen:
            raise SyncTrainError(f"duplicate sync policy floor pin {pin['path']!r}")
        seen.add(pin["path"])
        pins.append(pin)
    if policy_path not in seen:
        raise SyncTrainError("sync policy floor.pins must include the sync policy itself")
    raw_archival = floor.get("archival_provenance", [])
    if not isinstance(raw_archival, list) or len(raw_archival) > MAX_FLOOR_PINS:
        raise SyncTrainError("sync policy floor.archival_provenance must be a bounded array")
    archival: list[dict[str, str]] = []
    for entry in raw_archival:
        if not isinstance(entry, dict):
            raise SyncTrainError("every archival provenance record must be an object")
        path = require_nonempty(entry.get("path"), "archival provenance path")
        reason = require_nonempty(entry.get("reason"), f"archival provenance {path} reason")
        if path in seen or path == metadata_path:
            raise SyncTrainError(
                f"archival provenance {path!r} conflicts with a floor pin or metadata"
            )
        seen.add(path)
        archival.append({"path": path, "reason": reason})
    policy_historical_prefixes(policy)
    return pins, archival


def policy_historical_prefixes(policy: dict[str, Any]) -> list[str]:
    """Return directory prefixes whose files are historical records.

    Beads receipts and operation logs quote the floor they were produced
    under.  They are neither pins nor archival provenance, so the sweep that
    enforces the pins contract classifies them by a policy-declared prefix
    instead of silently ignoring them.
    """

    raw = policy["floor"].get("historical_record_prefixes", [])
    if not isinstance(raw, list) or len(raw) > MAX_FLOOR_PINS:
        raise SyncTrainError("sync policy floor.historical_record_prefixes must be a bounded array")
    prefixes: list[str] = []
    for entry in raw:
        prefix = require_nonempty(entry, "sync policy historical record prefix")
        if (
            not prefix.endswith("/")
            or prefix.startswith("/")
            or ".." in prefix.split("/")
            or prefix in prefixes
        ):
            raise SyncTrainError(
                f"sync policy historical record prefix must be a unique repo-relative directory: {prefix!r}"
            )
        prefixes.append(prefix)
    return prefixes


def normalize_command(value: object, label: str) -> str:
    """One shell line as the CI lane runs it, with whitespace runs collapsed."""

    text = require_nonempty(value, label)
    if "\n" in text or "\0" in text:
        raise SyncTrainError(f"{label} must be a single shell line")
    return " ".join(text.split())


def policy_gate_lanes(policy: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return the per-gate CI lane binding: workflow file, job id, and the
    exact command set that job runs.

    A gate id is only evidence if the commands its CI lane runs are known,
    so the policy must bind every required gate to one workflow job and the
    exact commands of that job.  ``record-gate`` refuses a command that is
    not declared here, and checks the declared set against the workflow job
    in the candidate tree, so neither side can drift silently.
    """

    gates = policy.get("gates")
    lanes = gates.get("lanes") if isinstance(gates, dict) else None
    if not isinstance(lanes, dict) or sorted(lanes.keys()) != sorted(GATE_ORDER):
        raise SyncTrainError(
            "sync policy gates.lanes must bind exactly the required gates and nothing else"
        )
    result: dict[str, dict[str, Any]] = {}
    for gate_id in GATE_ORDER:
        raw = lanes[gate_id]
        label = f"sync policy gates.lanes.{gate_id}"
        if not isinstance(raw, dict):
            raise SyncTrainError(f"{label} must be an object")
        workflow = require_nonempty(raw.get("workflow"), f"{label} workflow")
        if (
            workflow.startswith("/")
            or ".." in workflow.split("/")
            or "\\" in workflow
            or not workflow.endswith((".yml", ".yaml"))
        ):
            raise SyncTrainError(f"{label} workflow must be a repo-relative workflow file")
        job = require_nonempty(raw.get("job"), f"{label} job")
        if not WORKFLOW_JOB_RE.match(job):
            raise SyncTrainError(f"{label} job must be a workflow job id")
        commands = raw.get("commands")
        if not isinstance(commands, list) or not commands or len(commands) > MAX_LANE_COMMANDS:
            raise SyncTrainError(f"{label} commands must be a non-empty bounded array")
        normalized: list[str] = []
        for index, command in enumerate(commands):
            value = normalize_command(command, f"{label} commands[{index}]")
            try:
                shlex.split(value)
            except ValueError as error:
                raise SyncTrainError(f"{label} commands[{index}] is not a shell line: {error}") from error
            if value in normalized:
                raise SyncTrainError(f"{label} declares {value!r} twice")
            normalized.append(value)
        result[gate_id] = {"workflow": workflow, "job": job, "commands": normalized}
    return result


def workflow_job_lines(data: bytes, job: str, label: str) -> set[str]:
    """Return the stripped lines of one top-level workflow job block."""

    lines = decode(data).splitlines()
    try:
        start = lines.index(f"  {job}:") + 1
    except ValueError as error:
        raise SyncTrainError(f"{label}: workflow job {job!r} is not defined") from error
    block: set[str] = set()
    for line in lines[start:]:
        stripped = line.strip()
        if stripped and len(line) - len(line.lstrip()) <= 2:
            break
        if stripped:
            block.add(" ".join(stripped.split()))
    return block


def verify_lane_in_workflow(
    repo: Path, commit: str, gate_id: str, lane: dict[str, Any]
) -> None:
    """Prove every command the policy declares for ``gate_id`` is a line of
    the bound workflow job at ``commit``; a script listed in the job's
    ``for`` loop counts when the loop runs ``python3 "$test_file"``."""

    label = f"gate {gate_id!r} lane {lane['workflow']}#{lane['job']}"
    data = blob_bytes(repo, commit, lane["workflow"])
    if data is None:
        raise SyncTrainError(f"{label}: workflow file is absent from the candidate commit")
    block = workflow_job_lines(data, lane["job"], label)
    loop_runs_scripts = 'python3 "$test_file"' in block
    for command in lane["commands"]:
        if command in block:
            continue
        argv = shlex.split(command)
        if (
            loop_runs_scripts
            and len(argv) == 2
            and argv[0] == "python3"
            and (argv[1] in block or f"{argv[1]} \\" in block)
        ):
            continue
        raise SyncTrainError(
            f"{label}: sync policy declares {command!r} but the workflow job at the candidate "
            "commit does not run it; reconcile gates.lanes with the workflow before recording"
        )


def match_declared_command(argv: Sequence[str], declared: Sequence[str]) -> str | None:
    """Return the declared lane command ``argv`` runs, or ``None``.

    ``argv`` matches a declared shell line when it is that line's argv or the
    line wrapped in the exact CI shell prelude (``bash -euo pipefail -c``).
    """

    argv_list = list(argv)
    for command in declared:
        try:
            expected = shlex.split(command)
        except ValueError:
            continue
        if argv_list == expected or argv_list == [*LANE_SHELL_PRELUDE, command]:
            return command
    return None


def stamped_verification_commands(
    repo: Path, tree_ish: str, pins: Sequence[dict[str, Any]]
) -> dict[str, list[str]]:
    """Map every ``verification``/``tests`` command declared by a wildcard-
    stamped map object (an area or entry whose ``last_verified_upstream_sha``
    the train advances) to the targets that require it.

    Each stamp is a claim that the target was verified at the new floor; the
    commands here are what has to run against the candidate tree before the
    claim may be published.
    """

    required: dict[str, list[str]] = {}
    for pin in pins:
        each = pin.get("each_pointers") or []
        if not each:
            continue
        label = f"floor pin {pin['path']!r}"
        data = blob_bytes(repo, tree_ish, pin["path"])
        if data is None:
            raise SyncTrainError(f"{label} is absent from the candidate tree")
        value = canonical_json_value(data, label)
        for pointer in each:
            for container, _key, exact in expand_json_pointer(value, pointer, label):
                if not isinstance(container, dict):
                    raise SyncTrainError(f"{label}: stamped target at {exact!r} is not an object")
                target = exact.rsplit("/", 1)[0]
                for field in ("verification", "tests"):
                    commands = container.get(field, [])
                    if not isinstance(commands, list) or any(
                        not isinstance(command, str) for command in commands
                    ):
                        raise SyncTrainError(
                            f"{label}: stamped target {target!r} {field} must be a list of commands"
                        )
                    for command in commands:
                        key = normalize_command(command, f"{label} {target}/{field}")
                        required.setdefault(key, []).append(f"{pin['path']}#{target}/{field}")
    return required


def verification_coverage(
    required: dict[str, list[str]],
    covered: dict[str, list[str]],
    *,
    lane_commands: dict[str, dict[str, int]],
) -> dict[str, Any]:
    uncovered = sorted(command for command in required if command not in covered)
    targets = {target for names in required.values() for target in names}
    return {
        "stamped_targets": len(targets),
        "required_commands": len(required),
        "covered_commands": len(required) - len(uncovered),
        "uncovered_commands": uncovered,
        "lane_commands": lane_commands,
    }


def sha_occurrences(data: bytes, sha: str) -> int:
    return data.count(sha.encode("ascii"))


def string_floor_value(value: object, where: str, label: str) -> str:
    """Return the single 40-hex SHA embedded in a pinned string value."""

    if not isinstance(value, str):
        raise SyncTrainError(f"{label}: {where} is not a string")
    found = SHA_TOKEN_RE.findall(value)
    if len(found) != 1:
        raise SyncTrainError(f"{label}: {where} must embed exactly one 40-character SHA")
    return found[0]


def anchored_line_pattern(pin: dict[str, Any]) -> re.Pattern[bytes]:
    before, _, after = pin["line"].partition(FLOOR_PLACEHOLDER)
    return re.compile(
        b"^" + re.escape(before.encode("utf-8")) + b"([0-9a-f]{40})" + re.escape(after.encode("utf-8")) + b"$",
        re.MULTILINE,
    )


def inspect_pin_bytes(
    pin: dict[str, Any], data: bytes, label: str
) -> tuple[list[str], list[str], Any]:
    """Return ``(fixed_values, each_values, parsed)`` for one pin.

    ``fixed_values`` come from the exactly counted pointers or anchored
    lines; ``each_values`` come from every match of the wildcard pointers
    (per-entry verification stamps whose number follows the document).
    """

    path = pin["path"]
    if pin["kind"] in JSON_PIN_KINDS:
        value = canonical_json_value(data, f"floor pin {path!r} at the {label}")
        fixed = [
            string_floor_value(
                container[key], f"pointer {pointer!r}", f"floor pin {path!r} at the {label}"
            )
            for pointer in pin["pointers"]
            for container, key in (resolve_json_pointer(value, pointer, f"floor pin {path!r}"),)
        ]
        each: list[str] = []
        for pattern in pin.get("each_pointers", []):
            matches = expand_json_pointer(value, pattern, f"floor pin {path!r}")
            if not matches:
                raise SyncTrainError(
                    f"floor pin {path!r} at the {label}: each pointer {pattern!r} matches nothing"
                )
            for container, key, exact in matches:
                each.append(
                    string_floor_value(
                        container[key], f"pointer {exact!r}", f"floor pin {path!r} at the {label}"
                    )
                )
        return fixed, each, value
    matches = anchored_line_pattern(pin).findall(data)
    return [match.decode("ascii") for match in matches], [], None


def verify_pin_bytes(
    pin: dict[str, Any], data: bytes, floor: str, label: str
) -> dict[str, Any]:
    """Prove ``data`` pins ``floor`` at exactly the declared targets.

    The floor must occur in the bytes exactly as many times as the
    structured targets (fixed and wildcard) explain, so an undeclared
    literal cannot hide in a declared file.
    """

    path = pin["path"]
    fixed, each, _ = inspect_pin_bytes(pin, data, label)
    if len(fixed) != pin["occurrences"]:
        raise SyncTrainError(
            f"floor pin {path!r} at the {label} does not carry floor {floor} at its "
            f"{pin['occurrences']} declared targets (found {len(fixed)})"
        )
    targets = fixed + each
    if any(value != floor for value in targets):
        raise SyncTrainError(
            f"floor pin {path!r} at the {label} does not carry floor {floor}"
        )
    actual = sha_occurrences(data, floor)
    if actual != len(targets):
        raise SyncTrainError(
            f"floor pin {path!r} at the {label} contains {floor} {actual} times but its "
            f"declared targets explain {len(targets)}"
        )
    return {
        "path": path,
        "kind": pin["kind"],
        "occurrences": len(targets),
        "each_occurrences": len(each),
    }


def derived_receipt_fields(
    pin: dict[str, Any], value: Any, path: str
) -> tuple[str, str]:
    container, key = resolve_json_pointer(value, pin["metadata_pointer"], f"floor pin {path!r}")
    canonical = require_nonempty(container[key], f"{path} canonical metadata pointer")
    container, key = resolve_json_pointer(value, pin["blob_pointer"], f"floor pin {path!r}")
    blob = require_sha(container[key], f"{path} canonical metadata blob pointer")
    return canonical, blob


def verify_pins_at_commit(
    repo: Path,
    commit: str,
    pins: Iterable[dict[str, Any]],
    floor_sha: str,
    metadata_path: str,
    label: str,
) -> list[dict[str, Any]]:
    """Prove every declared pin at ``commit`` (or tree) carries ``floor_sha``."""

    records: list[dict[str, Any]] = []
    for pin in pins:
        path = pin["path"]
        data = blob_bytes(repo, commit, path)
        if data is None:
            raise SyncTrainError(f"floor pin {path!r} is absent from the {label}")
        record = verify_pin_bytes(pin, data, floor_sha, label)
        if pin["kind"] == "derived_metadata_receipt":
            _, _, value = inspect_pin_bytes(pin, data, label)
            canonical, blob = derived_receipt_fields(pin, value, path)
            if canonical != metadata_path:
                raise SyncTrainError(
                    f"floor pin {path!r} derives from {canonical!r}, not the canonical metadata"
                )
            expected_blob = blob_sha(repo, commit, metadata_path)
            if blob != expected_blob:
                raise SyncTrainError(
                    f"floor pin {path!r} canonical_metadata_blob_sha does not match the {label}"
                )
        records.append(record)
    return records


def archival_records_at_commit(
    repo: Path, commit: str, archival: Iterable[dict[str, str]], label: str
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for entry in archival:
        path = entry["path"]
        blob = blob_sha(repo, commit, path)
        if blob is None:
            raise SyncTrainError(f"archival provenance {path!r} is absent from the {label}")
        records.append({"path": path, "reason": entry["reason"], "blob_sha": blob})
    return records


def advance_pin_bytes(
    repo: Path,
    pin: dict[str, Any],
    data: bytes,
    old_floor: str,
    new_floor: str,
    *,
    metadata_path: str,
    metadata_after: bytes,
    label: str,
) -> tuple[bytes, dict[str, Any]]:
    """Rewrite one pin's declared targets from ``old_floor`` to ``new_floor``.

    Only the structured targets move: JSON pointers are replaced inside the
    parsed document (key order preserved, canonical serialization proven
    beforehand) and an anchored line is replaced as a whole line.  Wildcard
    per-entry stamps are advanced together with the fixed targets; their
    validity at the candidate is what the tree-bound gates prove before
    ``publish``.  A pin whose targets already carry ``new_floor`` is
    accepted unchanged so an interrupted advance can be re-run.
    """

    path = pin["path"]
    fixed, each, _ = inspect_pin_bytes(pin, data, label)
    targets = fixed + each
    if targets and all(value == new_floor for value in targets):
        record = verify_pin_bytes(pin, data, new_floor, label)
        if pin["kind"] == "derived_metadata_receipt":
            _, _, value = inspect_pin_bytes(pin, data, label)
            _, blob = derived_receipt_fields(pin, value, path)
            if blob != hash_blob(repo, metadata_after):
                raise SyncTrainError(
                    f"floor pin {path!r} already names the candidate but not the advanced metadata blob"
                )
        return data, record
    verify_pin_bytes(pin, data, old_floor, label)
    if pin["kind"] in JSON_PIN_KINDS:
        _, _, value = inspect_pin_bytes(pin, data, label)
        for pointer in pin["pointers"]:
            container, key = resolve_json_pointer(value, pointer, f"floor pin {path!r}")
            container[key] = container[key].replace(old_floor, new_floor)
        for pattern in pin.get("each_pointers", []):
            for container, key, _ in expand_json_pointer(value, pattern, f"floor pin {path!r}"):
                container[key] = container[key].replace(old_floor, new_floor)
        if pin["kind"] == "derived_metadata_receipt":
            canonical, _ = derived_receipt_fields(pin, value, path)
            if canonical != metadata_path:
                raise SyncTrainError(
                    f"floor pin {path!r} derives from {canonical!r}, not the canonical metadata"
                )
            container, key = resolve_json_pointer(value, pin["blob_pointer"], f"floor pin {path!r}")
            container[key] = hash_blob(repo, metadata_after)
        advanced = canonical_json_bytes(value)
    else:
        old_line = pin["line"].replace(FLOOR_PLACEHOLDER, old_floor).encode("utf-8")
        new_line = pin["line"].replace(FLOOR_PLACEHOLDER, new_floor).encode("utf-8")
        advanced = b"\n".join(
            new_line if line == old_line else line for line in data.split(b"\n")
        )
    record = verify_pin_bytes(pin, advanced, new_floor, label)
    if sha_occurrences(advanced, old_floor):
        raise SyncTrainError(
            f"floor pin {path!r} would still carry the previous floor after advancing its declared targets"
        )
    return advanced, record


def advance_metadata_bytes(
    data: bytes,
    old_floor: str,
    new_floor: str,
    *,
    selected_at: str,
    selection_basis: str,
) -> bytes:
    """Advance the canonical metadata floor and refresh its selection record."""

    value = advance_floor_value(data, old_floor, new_floor, "floor metadata")
    pinned = value.get("pinned_floor")
    if isinstance(pinned, dict):
        pinned["selected_at"] = selected_at
        pinned["selection_basis"] = selection_basis
    # Keep the author's key order so the train diff shows only the floor move.
    return canonical_json_bytes(value)


def floor_reference_paths(repo: Path, tree_ish: str, sha: str) -> list[str]:
    """Return every tracked path in ``tree_ish`` whose bytes contain ``sha``."""

    result = git(
        repo,
        ["grep", "-l", "-z", "-F", "-e", sha, tree_ish],
        allowed_statuses=frozenset({0, 1}),
    )
    paths: list[str] = []
    for record in result.stdout.split(b"\0"):
        if not record:
            continue
        prefix, separator, path = decode(record).partition(":")
        if not separator or prefix != tree_ish or not path:
            raise SyncTrainError("git grep returned an unexpected tree record")
        paths.append(path)
    return sorted(set(paths))


def verify_floor_references(
    repo: Path,
    tree_ish: str,
    sha: str,
    *,
    allowed: dict[str, str],
    prefixes: Sequence[str],
    label: str,
) -> dict[str, list[str]]:
    """Enforce the pins contract: every file carrying ``sha`` is classified.

    A hit must be the canonical metadata, a declared pin, declared archival
    provenance, the train's own receipt, or live under a policy-declared
    historical-record prefix.  Anything else is an undeclared pin and fails
    closed before any ref moves.
    """

    classified: dict[str, list[str]] = {}
    unclassified: list[str] = []
    for path in floor_reference_paths(repo, tree_ish, sha):
        kind = allowed.get(path)
        if kind is None and any(path.startswith(prefix) for prefix in prefixes):
            kind = "historical_record"
        if kind is None:
            unclassified.append(path)
        else:
            classified.setdefault(kind, []).append(path)
    if unclassified:
        raise SyncTrainError(
            f"floor {sha} is hard-pinned by undeclared paths at the {label}: "
            + ", ".join(unclassified[:16])
            + "; declare each as a floor pin, archival provenance, or historical record prefix in sync policy"
        )
    return classified


def worktree_tree_sha(repo: Path, train_dir: Path) -> str:
    """Hash the exact working tree (tracked, modified, and untracked files).

    Gates run against the checkout, so the tree they observe is the tree
    that must be published.  A private index copy keeps the live index and
    any in-progress merge state untouched.
    """

    index_file = temporary_index(repo, train_dir)
    try:
        git(repo, ["add", "--all", "--", "."], index_file=index_file)
        if unmerged_paths(repo, index_file=index_file):
            raise SyncTrainError("unresolved Git conflicts remain in the working tree")
        return require_sha(
            git_text(repo, ["write-tree"], index_file=index_file), "working tree SHA"
        )
    finally:
        index_file.unlink(missing_ok=True)


def verify_candidate_tree(
    repo: Path,
    tree_sha: str,
    state: dict[str, Any],
    declared_pins: Sequence[dict[str, Any]],
    declared_archival: Sequence[dict[str, str]],
    prefixes: Sequence[str],
    *,
    label: str,
    receipt_present: bool,
) -> list[dict[str, Any]]:
    """Prove from a written tree that the floor moved exactly as declared."""

    old_floor = state["floor_sha"]
    new_floor = state["source_sha"]
    metadata_path = state["floor_metadata"]
    receipt_path = state["receipt_path"]
    metadata = blob_bytes(repo, tree_sha, metadata_path)
    if metadata is None or floor_sha_from_bytes(metadata, f"floor metadata at the {label}") != new_floor:
        raise SyncTrainError(f"canonical floor metadata at the {label} does not pin the candidate")
    records = verify_pins_at_commit(
        repo, tree_sha, declared_pins, new_floor, metadata_path, label
    )
    archival = archival_records_at_commit(repo, tree_sha, declared_archival, label)
    if [(entry["path"], entry["blob_sha"]) for entry in archival] != [
        (entry["path"], entry["blob_sha"]) for entry in state.get("archival_provenance", [])
    ]:
        raise SyncTrainError(f"archival provenance changed at the {label}; a train never rewrites it")
    receipt_in_tree = blob_bytes(repo, tree_sha, receipt_path) is not None
    if receipt_in_tree != receipt_present:
        raise SyncTrainError(
            f"convergence receipt {receipt_path!r} is {'present' if receipt_in_tree else 'absent'} at the {label}"
        )
    allowed_new = {metadata_path: "canonical_metadata"}
    allowed_new.update({pin["path"]: "floor_pin" for pin in declared_pins})
    allowed_old = {entry["path"]: "archival_provenance" for entry in declared_archival}
    if receipt_present:
        allowed_new[receipt_path] = "convergence_receipt"
        allowed_old[receipt_path] = "convergence_receipt"
    verify_floor_references(
        repo, tree_sha, new_floor, allowed=allowed_new, prefixes=prefixes, label=label
    )
    verify_floor_references(
        repo, tree_sha, old_floor, allowed=allowed_old, prefixes=prefixes, label=label
    )
    return records

def blob_bytes(repo: Path, commit: str, path: str) -> bytes | None:
    existence = git(
        repo,
        ["cat-file", "-e", f"{commit}:{path}"],
        # Git versions differ on the status used for an absent path.  Some
        # return 1, while newer versions return 128 with a path-resolution
        # diagnostic.  Keep both statuses observable until the tree lookup
        # below classifies the result semantically.
        allowed_statuses=frozenset({0, 1, 128}),
    )
    if existence.returncode != 0:
        # A missing path is an ordinary provenance state, but a missing or
        # corrupt commit/tree must remain a hard Git error.  ls-tree gives us
        # that distinction without parsing Git's version-specific diagnostic
        # text: an absent path has no entry, whereas an invalid/corrupt tree
        # makes the command fail and therefore propagates through git().
        tree_entry = git(
            repo,
            ["ls-tree", "-z", commit, "--", path],
        ).stdout
        if not tree_entry:
            return None
        detail = bounded(decode(existence.stderr) or decode(existence.stdout) or "no diagnostic")
        raise SyncTrainError(
            f"git cat-file -e {commit}:{path} exited {existence.returncode}: {detail}"
        )
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


def branch_checked_out_elsewhere(repo: Path, branch_ref: str) -> bool:
    """Return whether another linked worktree currently owns ``branch_ref``."""

    worktrees = git_text(repo, ["worktree", "list", "--porcelain"])
    return any(line == f"branch {branch_ref}" for line in worktrees.splitlines())


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


def expected_sync_head(state: dict[str, Any]) -> str:
    """The only commit the isolated sync ref may point at in this state."""

    candidate = state.get("candidate_commit_sha")
    if state["status"] == "advanced" or (state["status"] == "failed" and candidate):
        return require_sha(candidate, "advanced candidate commit SHA")
    return state["sync_base_sha"]


def assert_sync_head(repo: Path, state: dict[str, Any]) -> str:
    sync_sha = resolve_direct_ref(repo, state["sync_ref"], "sync branch")
    if sync_sha is None:
        raise SyncTrainError("sync branch no longer exists")
    if sync_sha != expected_sync_head(state):
        raise SyncTrainError("sync branch moved since the train was prepared or advanced")
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
    sync_ref_retained: bool = False,
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
    bound = terminal_state in {"succeeded", "rolled_back"}
    gates = validate_gates(
        state,
        require_passed=bound,
        tree_sha=require_sha(state.get("candidate_tree_sha"), "gated candidate tree SHA")
        if bound
        else None,
    )
    if terminal_state == "succeeded":
        outcome = "published"
        advancement = "advanced"
    elif terminal_state == "rolled_back":
        outcome = "withdrawn"
        advancement = "withdrawn"
    else:
        outcome = "not_published"
        advancement = "not_advanced"
    receipt_sync_ref = (
        state["sync_ref"]
        if terminal_state == "succeeded" or (terminal_state == "rolled_back" and sync_ref_retained)
        else None
    )
    # A commit cannot embed its own SHA, so a published receipt leaves the
    # sync head null.  A rollback receipt lives outside Git and may name
    # the withdrawn commit exactly.
    receipt_sync_head = (
        state.get("final_commit_sha") if terminal_state == "rolled_back" else None
    )
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
        "floor_advancement": {
            "outcome": advancement,
            "previous_floor_sha": state["floor_sha"],
            "candidate_floor_sha": state["source_sha"],
            "canonical_metadata": state["floor_metadata"],
            "gated_tree_sha": state.get("candidate_tree_sha"),
            "verification_coverage": state.get("verification_coverage"),
            "pins": [
                {
                    "path": pin["path"],
                    "kind": pin["kind"],
                    "occurrences": pin["occurrences"],
                    "each_occurrences": pin["each_occurrences"],
                }
                for pin in state.get("floor_pins", [])
            ],
            "archival_provenance": [
                {"path": entry["path"], "reason": entry["reason"], "blob_sha": entry["blob_sha"]}
                for entry in state.get("archival_provenance", [])
            ],
        },
        "terminal": {"state": terminal_state, "reason": terminal_reason},
        "finalization": {
            "outcome": outcome,
            "sync_ref": receipt_sync_ref,
            "sync_head_sha": receipt_sync_head,
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
    cas_attempted: bool = False,
    cas_result: str = "not_attempted",
    sync_ref_retained: bool = False,
) -> Path:
    """Persist terminal failure/abort/rollback evidence outside canonical Git state."""

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
        cas_attempted=cas_attempted,
        cas_result=cas_result,
        released_head_sha=released_head_sha,
        released_update_mode="unchanged",
        released_old_sha=released_head_sha,
        released_new_sha=released_head_sha,
        sync_ref_retained=sync_ref_retained,
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


def conflict_values(
    entry: dict[str, Any],
    path: str,
    state: dict[str, Any],
    *,
    require_resolution_fields: bool,
) -> tuple[str | None, str | None, str | None]:
    if not isinstance(entry, dict):
        raise SyncTrainError(f"conflict record for {path!r} must be an object")
    owner_value = entry.get("owner")
    resolution_value = entry.get("resolution")
    rationale_value = entry.get("rationale")
    if not require_resolution_fields and all(
        value is None for value in (owner_value, resolution_value, rationale_value)
    ):
        return None, None, None
    if any(value is None for value in (owner_value, resolution_value, rationale_value)):
        raise SyncTrainError(
            f"conflict {path} owner, resolution, and rationale must be provided together"
        )
    owner = validate_conflict_owner(owner_value, state, f"conflict {path} owner")
    resolution = validate_resolution(resolution_value, f"conflict {path} resolution")
    rationale = require_nonempty(rationale_value, f"conflict {path} rationale")
    return owner, resolution, rationale


def validate_conflicts(
    state: dict[str, Any],
    repo: Path | None = None,
    *,
    require_resolution_fields: bool = True,
) -> list[dict[str, Any]]:
    """Validate conflict provenance and optionally require recorded resolutions.

    Every prepared conflict is represented in state from the start, so an
    operator can record those entries one at a time. The optional unresolved
    fields are accepted only as the untouched all-``None`` triplet; every
    structural, path, source, and Git blob check remains active. Gates,
    finalization, and receipts use the strict default.
    """

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
        conflict_values(
            entry,
            path,
            state,
            require_resolution_fields=require_resolution_fields,
        )
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


def validate_gates(
    state: dict[str, Any], *, require_passed: bool, tree_sha: str | None = None
) -> list[dict[str, Any]]:
    """Validate gate order and, when ``tree_sha`` is given, tree binding.

    A passed gate is evidence about exactly one tree; publication requires
    every gate to have been recorded against the candidate tree it publishes.
    """

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
        if tree_sha is not None and gate.get("tree_sha") != tree_sha:
            raise SyncTrainError(
                f"required gate {GATE_ORDER[index]!r} was recorded against tree "
                f"{gate.get('tree_sha')!r}, not the candidate tree {tree_sha}"
            )
        if require_passed:
            coverage = gate.get("coverage")
            if not isinstance(coverage, dict) or coverage.get("missing"):
                raise SyncTrainError(
                    f"required gate {GATE_ORDER[index]!r} passed without every declared lane command"
                )
            if tree_sha is not None and any(
                record.get("tree_sha") != tree_sha
                for record in validate_gate_command_records(gate)
            ):
                raise SyncTrainError(
                    f"required gate {GATE_ORDER[index]!r} carries a command recorded against another tree"
                )
    return gates


def passed_gate_commands(state: dict[str, Any], tree_sha: str) -> dict[str, list[str]]:
    """Union of commands that passed against ``tree_sha``, keyed to their gates."""

    covered: dict[str, list[str]] = {}
    for gate in state.get("gates", []):
        for record in validate_gate_command_records(gate):
            if record["status"] == "passed" and record["tree_sha"] == tree_sha:
                covered.setdefault(record["command"], []).append(gate["id"])
    return covered


def prepare(args: argparse.Namespace) -> dict[str, Any]:
    repo = resolve_repo(Path(args.repo or "."))
    train_dir = train_directory(Path(args.train_dir))
    policy, policy_path = load_policy(repo, args.policy)
    policy_bead_id = policy_train_bead_id(policy)
    existing = state_path(train_dir)
    if existing.exists():
        previous = load_state(train_dir, expected_bead_id=policy_bead_id)
        if previous.get("status") not in TERMINAL_STATUSES:
            raise SyncTrainError("train directory already contains an active sync train")

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
    declared_pins, declared_archival = policy_floor_pins(policy, policy_path)
    floor_pins = verify_pins_at_commit(
        repo, product_sha, declared_pins, floor_sha, floor_path, "product head"
    )
    archival_provenance = archival_records_at_commit(
        repo, product_sha, declared_archival, "product head"
    )
    # Enforce the pins contract: every tracked file at the product head that
    # carries the floor is classified, so a newly added pin cannot be left
    # behind silently at the old floor.
    allowed_references = {floor_path: "canonical_metadata"}
    allowed_references.update({pin["path"]: "floor_pin" for pin in declared_pins})
    allowed_references.update({entry["path"]: "archival_provenance" for entry in declared_archival})
    floor_references = verify_floor_references(
        repo,
        product_sha,
        floor_sha,
        allowed=allowed_references,
        prefixes=policy_historical_prefixes(policy),
        label="product head",
    )

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
    bead_id = (
        policy_bead_id
        if args.bead_id is None
        else require_nonempty(args.bead_id, "bead id")
    )
    if bead_id != policy_bead_id:
        raise SyncTrainError(f"bead id must be {policy_bead_id} from sync policy")

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
        "floor_pins": floor_pins,
        "archival_provenance": archival_provenance,
        "floor_references": floor_references,
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
        "floor_pins": [pin["path"] for pin in floor_pins],
        "floor_references": floor_references,
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
    # Once the floor is advanced the sync checkout carries the advanced
    # policy; the released product head is the only authority for the train.
    policy, _ = load_policy(
        repo, state["policy_path"], source_commit=state["product_head_sha"]
    )
    policy_bead_id = policy_train_bead_id(policy)
    if state["bead_id"] != policy_bead_id:
        raise SyncTrainError("sync-train state bead id differs from sync policy authority")
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
    declared_pins, declared_archival = policy_floor_pins(policy, state["policy_path"])
    if [(pin["path"], pin["kind"]) for pin in declared_pins] != [
        (pin["path"], pin["kind"]) for pin in state.get("floor_pins", [])
    ]:
        raise SyncTrainError("sync policy floor pins changed during the train")
    if [entry["path"] for entry in declared_archival] != [
        entry["path"] for entry in state.get("archival_provenance", [])
    ]:
        raise SyncTrainError("sync policy archival provenance changed during the train")
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
    # Re-validate every entry after updating the selected one. Structural and
    # provenance checks stay fail-closed while the other prepared entries may
    # remain untouched until their own record-conflict invocation.
    state["conflicts"] = sorted_conflicts(conflicts)
    validate_conflicts(state, repo, require_resolution_fields=False)
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


def execute_gate(command: list[str], cwd: Path) -> tuple[int, list[str]]:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=GATE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return 1, [bounded(f"gate command could not run: {type(error).__name__}")]
    output = bounded(decode(result.stdout) or decode(result.stderr) or f"exit code {result.returncode}")
    return result.returncode, [output]


def record_gate(args: argparse.Namespace) -> dict[str, Any]:
    """Run or record one ordered gate against the exact advanced candidate tree.

    A gate is evidence about one tree.  ``advance-floor`` records the
    candidate tree SHA (code, resolved conflicts, advanced metadata and
    pins); the working tree must hash to that SHA before and after the gate
    runs, and the gate record is bound to it so ``publish`` can refuse a
    tree that changed after the gate passed.
    """

    train_dir = train_directory(Path(args.train_dir))
    state = load_state(train_dir)
    if state["status"] != "advanced":
        raise SyncTrainError(
            f"cannot record a gate in a {state['status']} train; run advance-floor so gates "
            "can run against the exact candidate tree"
        )
    repo = state_repo(args, state)
    policy = validate_state_policy(repo, state)
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
    assert_sync_head(repo, state)
    candidate_tree = assert_candidate_checkout(repo, train_dir, state)
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
    gate = gates[index]
    if gate["status"] == "failed":
        raise SyncTrainError(f"gate {gate_id!r} already failed; the train is terminal")
    lane = policy_gate_lanes(policy)[gate_id]
    verify_lane_in_workflow(repo, state["candidate_commit_sha"], gate_id, lane)
    records = validate_gate_command_records(gate)
    passed_commands = {record["command"] for record in records if record["status"] == "passed"}
    recorded_at = utc_now()
    lane_label = f"{lane['workflow']}#{lane['job']}"

    def declared_for(argv: list[str]) -> str:
        declared = match_declared_command(argv, lane["commands"])
        if declared is None:
            raise SyncTrainError(
                f"gate {gate_id!r} command {shlex.join(argv)!r} is not one of the "
                f"{len(lane['commands'])} commands sync policy binds to {lane_label}: "
                + bounded("; ".join(lane["commands"]))
            )
        if declared in passed_commands:
            raise SyncTrainError(
                f"gate {gate_id!r} command {declared!r} already passed against this candidate tree"
            )
        return declared

    new_records: list[dict[str, Any]] = []
    if args.command_json is not None:
        if args.tree_sha is not None or args.status is not None or args.ci_run is not None:
            raise SyncTrainError(
                "--tree-sha, --status and --ci-run are only for externally executed evidence"
            )
        argv = parse_command_json(args.command_json)
        declared = declared_for(argv)
        exit_code, evidence = execute_gate(argv, repo)
        status = "passed" if exit_code == 0 else "failed"
        if args.evidence:
            evidence.extend(require_nonempty(item, "gate evidence") for item in args.evidence)
        after_tree = worktree_tree_sha(repo, train_dir)
        if after_tree != candidate_tree:
            status = "failed"
            exit_code = exit_code or 1
            evidence.append(
                f"gate command changed the candidate tree from {candidate_tree} to {after_tree}"
            )
        new_records.append(
            {
                "command": declared,
                "source": "executed",
                "status": status,
                "exit_code": exit_code,
                "evidence": evidence,
                "tree_sha": candidate_tree,
                "recorded_at": recorded_at,
            }
        )
    else:
        if args.status is None:
            raise SyncTrainError("record-gate requires --command-json or an explicit --status")
        recorded_tree = require_sha(args.tree_sha, "externally gated tree SHA (--tree-sha)")
        if recorded_tree != candidate_tree:
            raise SyncTrainError(
                f"external evidence was produced against tree {recorded_tree}, not the "
                f"advanced candidate tree {candidate_tree}"
            )
        status = "passed" if args.status == "passed" else "failed"
        evidence = [require_nonempty(item, "gate evidence") for item in (args.evidence or [])]
        if not evidence:
            raise SyncTrainError("record-gate requires non-empty evidence")
        exit_code = 0 if status == "passed" else 1
        if args.ci_run is not None and args.command_argv is not None:
            raise SyncTrainError("external evidence names either --command or --ci-run, not both")
        if args.ci_run is not None:
            # A CI run of the bound lane is evidence for every declared
            # command of that lane, but only when it ran the candidate commit.
            match = CI_RUN_URL_RE.match(require_nonempty(args.ci_run, "CI run URL (--ci-run)"))
            if match is None or match.group(1) != state["product_repository"]:
                raise SyncTrainError(
                    "--ci-run must be a GitHub Actions run URL of the product repository "
                    f"{state['product_repository']}"
                )
            ci_head = require_sha(args.ci_head_sha, "CI run head SHA (--ci-head-sha)")
            if ci_head != state["candidate_commit_sha"]:
                raise SyncTrainError(
                    f"CI run head {ci_head} is not the advanced candidate commit "
                    f"{state['candidate_commit_sha']}"
                )
            ci_evidence = [f"ci_run={match.group(0)} head={ci_head} lane={lane_label}", *evidence]
            for command in lane["commands"]:
                if command in passed_commands:
                    continue
                new_records.append(
                    {
                        "command": command,
                        "source": "ci_run",
                        "status": status,
                        "exit_code": exit_code,
                        "evidence": ci_evidence,
                        "tree_sha": candidate_tree,
                        "recorded_at": recorded_at,
                    }
                )
            if not new_records:
                raise SyncTrainError(
                    f"gate {gate_id!r}: every declared command of {lane_label} already passed"
                )
        elif args.command_argv is not None:
            declared = declared_for(parse_command_json(args.command_argv))
            new_records.append(
                {
                    "command": declared,
                    "source": "external_command",
                    "status": status,
                    "exit_code": exit_code,
                    "evidence": evidence,
                    "tree_sha": candidate_tree,
                    "recorded_at": recorded_at,
                }
            )
        else:
            raise SyncTrainError(
                "external gate evidence must name the declared lane command it ran (--command) "
                "or the CI run of the bound lane (--ci-run with --ci-head-sha); free-text "
                "evidence is not proof"
            )
    records.extend(new_records)
    covered = {record["command"] for record in records if record["status"] == "passed"}
    missing = [command for command in lane["commands"] if command not in covered]
    if any(record["status"] != "passed" for record in records):
        status = "failed"
    elif missing:
        status = "in_progress"
    else:
        status = "passed"
    exit_code = next(
        (record["exit_code"] or 1 for record in records if record["status"] != "passed"), 0
    )
    gate["command"] = lane_label
    gate["status"] = status
    gate["evidence"] = [
        f"{record['status']} [{record['source']}] {record['command']}: {item}"
        for record in records
        for item in record["evidence"]
    ]
    gate["exit_code"] = exit_code
    gate["tree_sha"] = candidate_tree
    gate["commands"] = records
    gate["coverage"] = {
        "declared": len(lane["commands"]),
        "passed": len(lane["commands"]) - len(missing),
        "missing": missing,
    }
    state["gates"] = gates
    state["last_gate_id"] = gate_id
    if status == "failed":
        state["status"] = "failed"
    write_json(state_path(train_dir), state)
    terminal_receipt: Path | None = None
    if status == "failed":
        terminal_receipt = write_terminal_receipt(
            train_dir,
            state,
            terminal_state="failed",
            reason=f"required gate {gate_id!r} failed",
            released_head_sha=state["product_head_sha"],
        )
    result = {
        "ok": status != "failed",
        "action": "record-gate",
        "gate": gate,
        "status": state["status"],
        "lane": lane_label,
        "recorded_commands": [record["command"] for record in new_records],
        "missing_commands": missing,
        "candidate_tree_sha": candidate_tree,
        "train_dir": str(train_dir),
        "terminal_receipt": str(terminal_receipt) if terminal_receipt else None,
    }
    if status == "failed":
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
    validate_state_policy(repo, state)
    product_sha = assert_product_unchanged(repo, state)
    sync_sha = resolve_direct_ref(repo, state["sync_ref"], "sync branch", missing_ok=True)
    if sync_sha is not None and sync_sha not in {state["sync_base_sha"], state.get("candidate_commit_sha")}:
        raise SyncTrainError("sync branch moved; refusing to discard a raced train")

    checkout_mode: str | None = None
    checkout_head: str | None = None
    if current_branch(repo) == state["sync_ref"]:
        paths = status_records(repo)
        if any(path.startswith("?? ") for path in paths):
            raise SyncTrainError("abort will not discard untracked files in the sync worktree")
        git(repo, ["reset", "--hard", product_sha])
        if current_branch(repo) != state["sync_ref"]:
            raise SyncTrainError("sync worktree detached unexpectedly during abort")
        if branch_checked_out_elsewhere(repo, state["product_branch"]):
            # A linked worktree cannot attach to a branch already checked out
            # by a sibling worktree. Leave this train checkout detached at
            # the verified product commit instead.
            git(repo, ["switch", "--detach", product_sha])
            checkout_mode = "detached_product_sha"
        else:
            git(repo, ["switch", state["product_branch"].removeprefix("refs/heads/")])
            checkout_mode = "product_branch"
        checkout_branch = current_branch(repo)
        checkout_head = resolve_commit(repo, "HEAD", "aborted sync worktree HEAD")
        if checkout_head != product_sha:
            raise SyncTrainError("abort did not restore the product commit in the sync worktree")
        if checkout_mode == "detached_product_sha":
            if checkout_branch is not None:
                raise SyncTrainError("abort did not leave the linked train worktree detached")
        elif checkout_branch != state["product_branch"]:
            raise SyncTrainError("abort did not restore the product branch in the sync worktree")
        if status_bytes(repo):
            raise SyncTrainError("abort left the sync worktree dirty")

    product_metadata = metadata_from_commit(repo, product_sha, state["floor_metadata"])
    expected_digest = state.get("floor_metadata_sha256")
    if expected_digest != hashlib.sha256(product_metadata).hexdigest():
        raise SyncTrainError("product floor metadata changed; refusing to call the train aborted")
    current = current_branch(repo)
    if current == state["product_branch"] or checkout_head == product_sha:
        try:
            current_metadata = (repo / state["floor_metadata"]).read_bytes()
        except OSError as error:
            raise SyncTrainError(f"could not read product floor metadata: {error}") from error
        if current_metadata != product_metadata:
            raise SyncTrainError("abort would leave floor metadata bytes changed")

    if sync_sha is not None:
        # The hard reset above may have moved the checked-out sync branch back
        # to the product head; delete whatever value it holds now, provided it
        # is still one of the train's own commits.
        current_sync = resolve_direct_ref(repo, state["sync_ref"], "sync branch", missing_ok=True)
        if current_sync is None or current_sync not in {
            state["sync_base_sha"],
            product_sha,
            state.get("candidate_commit_sha"),
        }:
            raise SyncTrainError("sync branch moved during abort; refusing to discard a raced train")
        git(
            repo,
            ["update-ref", "-d", state["sync_ref"], current_sync],
        )
        if resolve_direct_ref(repo, state["sync_ref"], "sync branch", missing_ok=True) is not None:
            raise SyncTrainError("abort did not remove the isolated sync branch")
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
        "checkout_mode": checkout_mode,
        "checkout_head_sha": resolve_commit(repo, "HEAD", "aborted train checkout HEAD"),
        "current_branch": current_branch(repo),
        "worktree_clean": not bool(status_bytes(repo)),
        "train_dir": str(train_dir),
        "terminal_receipt": str(terminal_receipt),
    }


def train_publication_preflight(
    args: argparse.Namespace, state: dict[str, Any]
) -> tuple[Path, dict[str, Any], str, str, list[str], list[dict[str, Any]]]:
    """Shared checks for the two mutation-ordered steps of publication."""

    repo = state_repo(args, state)
    policy = validate_state_policy(repo, state)
    product_sha = assert_product_unchanged(repo, state)
    if current_branch(repo) != state["sync_ref"]:
        raise SyncTrainError(f"{args.command} requires the isolated sync branch to be checked out")
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
    ensure_no_untracked(repo)
    return repo, policy, product_sha, sync_sha, merge_heads_now, conflicts


def assert_candidate_checkout(repo: Path, train_dir: Path, state: dict[str, Any]) -> str:
    """Prove the sync checkout is exactly the advanced candidate: HEAD is the
    candidate commit, its tree is the recorded candidate tree, and the working
    tree (including untracked files) hashes to that same tree."""

    candidate_tree = require_sha(state.get("candidate_tree_sha"), "advanced candidate tree SHA")
    candidate_commit = require_sha(state.get("candidate_commit_sha"), "advanced candidate commit SHA")
    if current_branch(repo) != state["sync_ref"]:
        raise SyncTrainError("the advanced candidate requires the isolated sync branch to be checked out")
    head = resolve_commit(repo, "HEAD", "sync worktree HEAD")
    if head != candidate_commit:
        raise SyncTrainError(
            f"sync worktree HEAD {head} is not the advanced candidate commit {candidate_commit}"
        )
    if git_text(repo, ["rev-parse", "HEAD^{tree}"]) != candidate_tree:
        raise SyncTrainError("candidate commit does not carry the recorded candidate tree")
    observed = worktree_tree_sha(repo, train_dir)
    if observed != candidate_tree:
        raise SyncTrainError(
            f"working tree {observed} differs from the advanced candidate tree "
            f"{candidate_tree}; gates must run against the exact candidate tree"
        )
    return candidate_tree


def advance_floor(args: argparse.Namespace) -> dict[str, Any]:
    """Write the candidate tree: resolved code plus advanced metadata and pins.

    This is the only step that rewrites floor pins, and it happens before any
    gate runs so every gate observes the tree that ``publish`` will commit.
    The candidate tree is committed on the isolated sync ref (compare-and-swap
    from the starting product head) and checked out, so ancestry-based gates
    see the candidate floor as ``HEAD``.  The released product ref never
    moves; ``abort`` discards the candidate commit.
    """

    train_dir = train_directory(Path(args.train_dir))
    state = load_state(train_dir)
    if state["status"] == "advanced":
        repo = state_repo(args, state)
        candidate_tree = require_sha(state.get("candidate_tree_sha"), "advanced candidate tree SHA")
        candidate_commit = require_sha(state.get("candidate_commit_sha"), "advanced candidate commit SHA")
        assert_candidate_checkout(repo, train_dir, state)
        return {
            "ok": True,
            "action": "advance-floor",
            "status": "advanced",
            "candidate_tree_sha": candidate_tree,
            "candidate_commit_sha": candidate_commit,
            "already_advanced": True,
            "train_dir": str(train_dir),
        }
    if state["status"] not in {"prepared", "conflicted"}:
        raise SyncTrainError(f"cannot advance the floor of a {state['status']} train")
    repo, policy, product_sha, sync_sha, merge_heads_now, conflicts = train_publication_preflight(args, state)
    declared_pins, declared_archival = policy_floor_pins(policy, state["policy_path"])
    prefixes = policy_historical_prefixes(policy)
    parents = [sync_sha]
    if merge_heads_now:
        parents.append(state["source_sha"])
    elif state["source_sha"] != sync_sha:
        # If a caller applied the source in an earlier commit, preserving the
        # source as a parent is only safe when Git can prove that relationship.
        is_ancestor(repo, state["source_sha"], sync_sha, "sync branch/source relationship")

    metadata_path = repo / state["floor_metadata"]
    try:
        current_metadata = metadata_path.read_bytes()
    except OSError as error:
        raise SyncTrainError(f"could not read floor metadata in sync worktree: {error}") from error
    advanced_at = utc_now()
    floor_after = advance_metadata_bytes(
        current_metadata,
        state["floor_sha"],
        state["source_sha"],
        selected_at=advanced_at,
        selection_basis=(
            f"sync train sync-train-{state['source_sha'][:12]} resolved "
            f"{state['source_ref']} to {state['source_sha']} ({state['bead_id']})"
        ),
    )
    if (repo / state["receipt_path"]).exists():
        raise SyncTrainError("convergence receipt path already exists in the sync worktree")
    # Compute every advanced file before writing any of them so a refused pin
    # leaves the worktree exactly as the merge produced it.
    advanced_files: list[tuple[str, bytes]] = [(state["floor_metadata"], floor_after)]
    records: list[dict[str, Any]] = []
    for pin in declared_pins:
        try:
            pin_data = (repo / pin["path"]).read_bytes()
        except OSError as error:
            raise SyncTrainError(
                f"floor pin {pin['path']!r} is unreadable in the sync worktree: {error}"
            ) from error
        pin_after, record = advance_pin_bytes(
            repo,
            pin,
            pin_data,
            state["floor_sha"],
            state["source_sha"],
            metadata_path=state["floor_metadata"],
            metadata_after=floor_after,
            label="sync worktree",
        )
        advanced_files.append((pin["path"], pin_after))
        records.append(record)
    for path, data in advanced_files:
        atomic_write(repo / path, data)
    git(repo, ["add", "--", *[path for path, _ in advanced_files]])
    candidate_tree = worktree_tree_sha(repo, train_dir)
    verified = verify_candidate_tree(
        repo,
        candidate_tree,
        state,
        declared_pins,
        declared_archival,
        prefixes,
        label="candidate tree",
        receipt_present=False,
    )
    # Every wildcard stamp the train just advanced is a verification claim;
    # refuse before any ref moves unless some declared gate lane runs each
    # command the stamped target names.
    lanes = policy_gate_lanes(policy)
    declared_commands: dict[str, list[str]] = {}
    for lane_id, lane in lanes.items():
        for command in lane["commands"]:
            declared_commands.setdefault(command, []).append(lane_id)
    required_commands = stamped_verification_commands(repo, candidate_tree, declared_pins)
    coverage = verification_coverage(
        required_commands,
        declared_commands,
        lane_commands={
            lane_id: {"declared": len(lane["commands"]), "passed": 0}
            for lane_id, lane in lanes.items()
        },
    )
    if coverage["uncovered_commands"]:
        raise SyncTrainError(
            f"{len(coverage['uncovered_commands'])} of {coverage['required_commands']} verification "
            "commands declared by stamped convergence targets are run by no gate lane in sync policy: "
            + bounded(
                "; ".join(
                    f"{command} <- {', '.join(required_commands[command][:3])}"
                    for command in coverage["uncovered_commands"][:8]
                )
            )
        )
    commit_args: list[str] = ["commit-tree", candidate_tree]
    for parent in parents:
        commit_args.extend(["-p", parent])
    candidate_message = f"candidate: converge upstream {state['source_sha'][:12]} (ungated)\n"
    candidate_commit = require_sha(
        git_text(repo, commit_args, input_data=candidate_message.encode("utf-8")),
        "candidate commit SHA",
    )
    # Move only the isolated sync ref, from the exact starting product head,
    # and only while the product branch and pinned source are unchanged.
    transaction = (
        "start\n"
        f"verify {state['product_branch']} {product_sha}\n"
        f"verify {state['source_ref']} {state['source_sha']}\n"
        f"update {state['sync_ref']} {candidate_commit} {sync_sha}\n"
        "prepare\n"
        "commit\n"
    ).encode("utf-8")
    git(repo, ["update-ref", "--stdin"], input_data=transaction)
    git(repo, ["reset", "--hard", candidate_commit])
    state["status"] = "advanced"
    state["candidate_tree_sha"] = candidate_tree
    state["candidate_commit_sha"] = candidate_commit
    state["candidate_parents"] = parents
    state["advanced_at"] = advanced_at
    state["floor_pins"] = verified
    state["verification_coverage"] = coverage
    state["conflicts"] = sorted_conflicts(conflicts)
    state["merge_in_progress"] = False
    write_json(state_path(train_dir), state)
    assert_candidate_checkout(repo, train_dir, state)
    return {
        "ok": True,
        "action": "advance-floor",
        "verification_coverage": coverage,
        "status": "advanced",
        "product_head_sha": product_sha,
        "sync_ref": state["sync_ref"],
        "candidate_tree_sha": candidate_tree,
        "candidate_commit_sha": candidate_commit,
        "candidate_parents": parents,
        "floor_before_sha": state["floor_sha"],
        "floor_after_sha": state["source_sha"],
        "advanced_paths": [path for path, _ in advanced_files],
        "floor_pins": verified,
        "already_advanced": False,
        "train_dir": str(train_dir),
    }


def publish(args: argparse.Namespace) -> dict[str, Any]:
    """Commit the gated candidate tree plus its receipt and CAS the sync ref.

    The published tree may differ from the gated candidate tree only by the
    convergence receipt, which cannot be gate input because it records the
    gates themselves.  ``git diff-tree`` between the two trees is the proof.
    The published commit reuses the candidate commit's parents (starting
    product head and pinned upstream source) so the sync ref carries exactly
    one train commit above the product head.
    """

    train_dir = train_directory(Path(args.train_dir))
    state = load_state(train_dir)
    if state["status"] != "advanced":
        raise SyncTrainError(
            f"cannot publish a {state['status']} train; advance-floor must produce the gated "
            "candidate tree first"
        )
    repo, policy, product_sha, sync_sha, _, conflicts = train_publication_preflight(args, state)
    candidate_tree = require_sha(state.get("candidate_tree_sha"), "advanced candidate tree SHA")
    candidate_commit = require_sha(state.get("candidate_commit_sha"), "advanced candidate commit SHA")
    if sync_sha != candidate_commit:
        raise SyncTrainError("sync branch is not at the advanced candidate commit")
    validate_gates(state, require_passed=True, tree_sha=candidate_tree)
    observed_tree = worktree_tree_sha(repo, train_dir)
    if observed_tree != candidate_tree:
        raise SyncTrainError(
            f"working tree {observed_tree} changed after the gates passed against candidate "
            f"tree {candidate_tree}; refusing to publish an ungated tree"
        )
    if resolve_commit(repo, "HEAD", "sync worktree HEAD") != candidate_commit:
        raise SyncTrainError("sync worktree HEAD is not the advanced candidate commit")
    parents = [require_sha(parent, "candidate parent") for parent in state["candidate_parents"]]
    recorded = git_text(repo, ["rev-list", "--parents", "-n", "1", candidate_commit]).split()
    if recorded[1:] != parents or parents[0] != state["sync_base_sha"]:
        raise SyncTrainError("candidate commit parents differ from the recorded train parents")
    declared_pins, declared_archival = policy_floor_pins(policy, state["policy_path"])
    prefixes = policy_historical_prefixes(policy)
    state["floor_pins"] = verify_candidate_tree(
        repo,
        candidate_tree,
        state,
        declared_pins,
        declared_archival,
        prefixes,
        label="candidate tree",
        receipt_present=False,
    )
    if state["receipt_path"] == state["floor_metadata"]:
        raise SyncTrainError("floor metadata and convergence receipt must be different files")
    # The stamps in the candidate tree are published only if every
    # verification command the stamped targets declare actually passed
    # against that exact tree.
    lanes = policy_gate_lanes(policy)
    covered = passed_gate_commands(state, candidate_tree)
    required_commands = stamped_verification_commands(repo, candidate_tree, declared_pins)
    coverage = verification_coverage(
        required_commands,
        covered,
        lane_commands={
            lane_id: {
                "declared": len(lane["commands"]),
                "passed": sum(1 for command in lane["commands"] if command in covered),
            }
            for lane_id, lane in lanes.items()
        },
    )
    if coverage["uncovered_commands"]:
        raise SyncTrainError(
            f"{len(coverage['uncovered_commands'])} of {coverage['required_commands']} verification "
            "commands declared by stamped convergence targets did not pass against the candidate tree: "
            + bounded("; ".join(coverage["uncovered_commands"][:8]))
        )
    state["verification_coverage"] = coverage
    completed_at = utc_now()
    receipt = make_receipt(
        state,
        completed_at=completed_at,
        terminal_state="succeeded",
        terminal_reason="all required ordered gates passed against the candidate tree and the isolated sync ref was published",
        cas_attempted=True,
        cas_result="matched_and_published",
        released_head_sha=product_sha,
        released_update_mode="unchanged",
        released_old_sha=product_sha,
        released_new_sha=product_sha,
    )
    receipt_data = json_bytes(receipt)
    # Build the publication tree in a private index seeded from the gated
    # candidate tree.  Neither the live index nor the ref changes until the
    # CAS succeeds.
    index_file = temporary_index(repo, train_dir)
    try:
        git(repo, ["read-tree", candidate_tree], index_file=index_file)
        stage_blob(repo, index_file, state["receipt_path"], receipt_data)
        tree_sha = require_sha(
            git_text(repo, ["write-tree"], index_file=index_file), "sync tree SHA"
        )
        delta = git(repo, ["diff-tree", "-r", "-z", "--name-status", candidate_tree, tree_sha]).stdout
        fields = [decode(field) for field in delta.split(b"\0") if field]
        if fields != ["A", state["receipt_path"]]:
            raise SyncTrainError(
                "published tree differs from the gated candidate tree by more than the receipt: "
                + bounded(" ".join(fields))
            )
        verify_candidate_tree(
            repo,
            tree_sha,
            state,
            declared_pins,
            declared_archival,
            prefixes,
            label="published tree",
            receipt_present=True,
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
        # Verify both moving inputs and replace the ungated candidate commit
        # with the published commit on the isolated branch in one
        # compare-and-swap transaction.  A race leaves the product branch and
        # its floor untouched; the generated commit remains an unreachable
        # retry aid, and the candidate commit stays reachable only from state.
        transaction = (
            "start\n"
            f"verify {state['product_branch']} {product_sha}\n"
            f"verify {state['source_ref']} {state['source_sha']}\n"
            f"update {state['sync_ref']} {commit_sha} {candidate_commit}\n"
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
    if git_text(repo, ["rev-parse", "HEAD^{tree}"]) != tree_sha:
        raise SyncTrainError("published commit does not carry the verified tree")
    state["status"] = "finalized"
    state["merge_in_progress"] = False
    state["final_commit_sha"] = commit_sha
    state["final_tree_sha"] = tree_sha
    state["conflicts"] = sorted_conflicts(conflicts)
    state["completed_at"] = receipt["completed_at"]
    write_json(state_path(train_dir), state)
    return {
        "ok": True,
        "action": "publish",
        "status": "finalized",
        "product_branch": state["product_branch"],
        "product_head_sha": product_sha,
        "sync_ref": state["sync_ref"],
        "sync_head_sha": commit_sha,
        "source_sha": state["source_sha"],
        "floor_before_sha": state["floor_sha"],
        "floor_after_sha": state["source_sha"],
        "receipt_path": state["receipt_path"],
        "candidate_tree_sha": candidate_tree,
        "tree_sha": tree_sha,
        "train_dir": str(train_dir),
    }

def rollback(args: argparse.Namespace) -> dict[str, Any]:
    """Withdraw a finalized but unpromoted train.

    Scope: this covers only a train whose commit still lives solely on the
    isolated sync ref.  The released product ref is never an update target
    of this workflow, so rollback never rewrites it: it proves the product
    head still carries the starting floor in every declared pin, withdraws
    the isolated sync ref with a compare-and-swap on the exact finalized
    commit (or retains it as review evidence), and records a ``rolled_back``
    terminal receipt.  A product ref that root already fast-forwarded to the
    train is a promotion; this workflow has no reverse train for it yet and
    refuses with a typed error rather than force-updating anything.
    """

    train_dir = train_directory(Path(args.train_dir))
    state = load_state(train_dir)
    if state["status"] == "rolled_back":
        return {
            "ok": True,
            "action": "rollback",
            "status": "rolled_back",
            "train_dir": str(train_dir),
        }
    if state["status"] != "finalized":
        raise SyncTrainError(
            f"cannot roll back a {state['status']} train; use abort for an unfinalized train"
        )
    repo = state_repo(args, state)
    policy = validate_state_policy(repo, state)
    final_sha = require_sha(state.get("final_commit_sha"), "finalized train commit SHA")
    product_sha = resolve_direct_ref(repo, state["product_branch"], "product branch")
    assert product_sha is not None
    if product_sha != state["product_head_sha"]:
        raise SyncTrainError(
            "released product ref moved past the recorded starting head; this workflow "
            "never force-updates a released ref, so reverse a promoted train with a new "
            "forward train instead of a rollback"
        )
    sync_sha = resolve_direct_ref(repo, state["sync_ref"], "sync branch", missing_ok=True)
    if sync_sha is not None and sync_sha != final_sha:
        raise SyncTrainError("sync branch moved since publish; refusing to withdraw a raced train")

    checkout_mode: str | None = None
    if current_branch(repo) == state["sync_ref"]:
        if status_bytes(repo):
            raise SyncTrainError("rollback will not discard changes in the sync worktree")
        if branch_checked_out_elsewhere(repo, state["product_branch"]):
            git(repo, ["switch", "--detach", product_sha])
            checkout_mode = "detached_product_sha"
        else:
            git(repo, ["switch", state["product_branch"].removeprefix("refs/heads/")])
            checkout_mode = "product_branch"
        if resolve_commit(repo, "HEAD", "rollback checkout HEAD") != product_sha:
            raise SyncTrainError("rollback did not restore the product commit in the sync worktree")
        if status_bytes(repo):
            raise SyncTrainError("rollback left the sync worktree dirty")

    product_metadata = metadata_from_commit(repo, product_sha, state["floor_metadata"])
    if state.get("floor_metadata_sha256") != hashlib.sha256(product_metadata).hexdigest():
        raise SyncTrainError("product floor metadata changed; refusing to call the train rolled back")
    if floor_sha_from_bytes(product_metadata, "product floor metadata") != state["floor_sha"]:
        raise SyncTrainError("product head does not carry the starting floor")
    declared_pins, _ = policy_floor_pins(policy, state["policy_path"])
    restored_pins = verify_pins_at_commit(
        repo,
        product_sha,
        declared_pins,
        state["floor_sha"],
        state["floor_metadata"],
        "product head",
    )

    retained = bool(args.retain_sync_ref)
    cas_result = "not_attempted"
    cas_attempted = False
    if sync_sha is not None and not retained:
        cas_attempted = True
        transaction = (
            "start\n"
            f"verify {state['product_branch']} {product_sha}\n"
            f"delete {state['sync_ref']} {final_sha}\n"
            "prepare\n"
            "commit\n"
        ).encode("utf-8")
        git(repo, ["update-ref", "--stdin"], input_data=transaction)
        if resolve_direct_ref(repo, state["sync_ref"], "sync branch", missing_ok=True) is not None:
            raise SyncTrainError("rollback did not withdraw the isolated sync branch")
        cas_result = "matched_and_withdrawn"
    sync_ref_retained = sync_sha is not None and retained

    state["status"] = "rolled_back"
    state["rolled_back_at"] = utc_now()
    state["sync_ref_retained"] = sync_ref_retained
    state["floor_pins"] = restored_pins
    write_json(state_path(train_dir), state)
    terminal_receipt = write_terminal_receipt(
        train_dir,
        state,
        terminal_state="rolled_back",
        reason=(
            "finalized train withdrawn; the released product ref and every declared "
            f"floor pin remain at {state['floor_sha']}"
        ),
        released_head_sha=product_sha,
        cas_attempted=cas_attempted,
        cas_result=cas_result,
        sync_ref_retained=sync_ref_retained,
    )
    return {
        "ok": True,
        "action": "rollback",
        "status": "rolled_back",
        "product_branch": state["product_branch"],
        "product_head_sha": product_sha,
        "restored_floor_sha": state["floor_sha"],
        "withdrawn_commit_sha": final_sha,
        "sync_ref": state["sync_ref"],
        "sync_ref_removed": cas_result == "matched_and_withdrawn",
        "sync_ref_retained": sync_ref_retained,
        "restored_pins": [pin["path"] for pin in restored_pins],
        "checkout_mode": checkout_mode,
        "checkout_head_sha": resolve_commit(repo, "HEAD", "rollback checkout HEAD"),
        "current_branch": current_branch(repo),
        "worktree_clean": not bool(status_bytes(repo)),
        "train_dir": str(train_dir),
        "terminal_receipt": str(terminal_receipt),
    }


def inspect(args: argparse.Namespace) -> dict[str, Any]:
    train_dir = train_directory(Path(args.train_dir))
    state = load_state(train_dir)
    repo = state_repo(args, state)
    validate_state_policy(repo, state)
    observed_product = resolve_direct_ref(repo, state["product_branch"], "product branch", missing_ok=True)
    observed_sync = resolve_direct_ref(repo, state["sync_ref"], "sync branch", missing_ok=True)
    return {
        "ok": observed_product == state["product_head_sha"]
        and (state["status"] in TERMINAL_STATUSES or observed_sync is not None),
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
    prepare_parser.add_argument("--bead-id")

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
    gate_parser.add_argument(
        "--tree-sha",
        help="candidate tree SHA the external evidence was produced against (required with --status)",
    )
    gate_parser.add_argument(
        "--ci-run",
        help="GitHub Actions run URL of the bound lane that ran the candidate commit (external evidence)",
    )
    gate_parser.add_argument(
        "--ci-head-sha",
        help="commit SHA the CI run checked out; must equal the advanced candidate commit",
    )

    abort_parser = subparsers.add_parser("abort", help="invalidate and remove an unfinalized train")
    add_repo_argument(abort_parser)
    add_train_argument(abort_parser)

    advance_parser = subparsers.add_parser(
        "advance-floor",
        help="write the candidate tree (resolved code, advanced metadata and pins) that gates run against",
    )
    add_repo_argument(advance_parser)
    add_train_argument(advance_parser)

    publish_parser = subparsers.add_parser(
        "publish", help="commit the gated candidate tree plus receipt and CAS the sync ref"
    )
    add_repo_argument(publish_parser)
    add_train_argument(publish_parser)
    publish_parser.add_argument("--message")

    rollback_parser = subparsers.add_parser(
        "rollback", help="withdraw a finalized train and prove the prior floor is restored"
    )
    add_repo_argument(rollback_parser)
    add_train_argument(rollback_parser)
    rollback_parser.add_argument(
        "--retain-sync-ref",
        action="store_true",
        help="keep the withdrawn sync ref as review evidence instead of deleting it",
    )

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
        elif args.command == "advance-floor":
            result = advance_floor(args)
        elif args.command == "publish":
            result = publish(args)
        elif args.command == "rollback":
            result = rollback(args)
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
