#!/usr/bin/env python3
"""Enforce the product memory dependency graph from Cargo metadata and source.

WHY THIS GATE HAS TWO LAYERS (2026-09-02)
-----------------------------------------

The invariant this gate defends is unchanged and is about *capability
reachability*, not crate-name spelling:

    The composition registry may construct the provider-neutral fabric and the
    Native adapter and may adapt provider-neutral and application-owned
    contract value types. It must never depend on, construct, or reach a
    concrete store, session store, code index, host, transport, another
    concrete provider, or the root crate. It must never start background work,
    which is what keeps the feature-on/provider-disabled host dormant.

Layer 1 (Cargo metadata) is default-deny on the dependency graph: exact names,
split by dependency kind, with an exact allowed feature set per edge.

Layer 2 (crate source) exists because layer 1 provably cannot finish the job.
Cargo unifies features per compiled crate instance. `tracedecay-application`
mounts an optional `gix` historical-blob reader behind its `native-git`
feature, and the production root (`crates/tracedecay/Cargo.toml`) enables
`tracedecay-application/native-git`. In that build there is exactly ONE
compiled `tracedecay-application`, with `native-git` on, and every consumer in
the graph links against it. Writing `default-features = false` on the
registry's own dependency entry therefore does NOT produce a capability-free
instance for the registry: `NativeHistoricalBlobReaderV1` is in scope for it
regardless of what the registry's manifest requests. A manifest-only gate
cannot see that, and a manifest-only gate cannot see it in principle. The only
honest enforcement is an exact source-import allowlist over the crate's `src/`,
which is what `source_contracts` provides.

The direct-edge feature pins are retained, but their documented job is now the
narrow one they can actually do: stop the *registry itself* from asking for a
capability feature (which would also switch it on for every other consumer),
and keep a future `default` feature set from silently widening the edge. They
are not, and are no longer claimed to be, protection against unification.

ENFORCEMENT INDEX
-----------------

* Dependency kind is carried through. A dev edge cannot ship a capability into
  the artifact, so it answers to its own exact `allowed_dev_dependencies` list.
  It is scoped, never exempted: an unlisted dev edge still fails, and a dev
  allowance never authorizes the same name as a production edge.
* `allowed_dependency_features` is an exact allowlist, not a denylist. Any
  feature enabled on a production edge that is not named there is refused, so
  an unreviewed capability feature (a future `native-sqlite-store`, or
  `chrono/clock`) cannot arrive unnoticed.
* `dependencies_requiring_default_features_off` keeps a future default feature
  set from widening an edge behind the allowlist's back.
* SOURCE CONTRACTS ARE NOT OPTIONAL AND CANNOT BE DELETED FROM THE POLICY.
  The checker derives when one is mandatory:
    - Any production edge to a workspace package that declares an OPTIONAL
      dependency of its own requires an exact source-import allowlist for that
      edge, because feature unification means the direct edge does not bound
      what that crate exports in the production build. This is the structural
      statement of the `tracedecay-application`/`gix` problem, derived from
      metadata rather than from a crate name a policy edit could remove.
    - Any production edge to a known executor crate (EXECUTOR_DEPENDENCIES,
      held in the checker, not the policy) requires the same, plus every
      admitted executor item path pinned to exact call sites.
* Executor call sites are pinned by enclosing function. The gate cannot prove
  reachability from the disabled composition path, so it enforces the bound it
  can: no NEW task-spawning or blocking-offload site may appear anywhere in the
  crate without review, and every pinned site must still exist.
* FORBIDDEN_SOURCE_SYMBOL_FLOOR is held in the checker. Any crate with a source
  contract is refused if its source names a runtime constructor, an OS thread,
  a process, the filesystem, a socket, an embedded store, an HTTP stack, a git
  object store, or the concrete `NativeHistoricalBlobReaderV1` reader. A policy
  edit can add to this floor and can never remove from it.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
REQUIRED_EXCEPTION_FIELDS = (
    "id",
    "rule_id",
    "from_package",
    "to_package",
    "adr",
    "rationale",
    "owner",
    "verification",
    "review_after",
)

KIND_NORMAL = "normal"
KIND_DEV = "dev"
KIND_BUILD = "build"
DEPENDENCY_KINDS = (KIND_NORMAL, KIND_DEV, KIND_BUILD)
# Normal and build edges are compiled into the shipped artifact; dev edges are
# only linked into the crate's own tests and benches.
PRODUCTION_KINDS = (KIND_NORMAL, KIND_BUILD)

# Crates that hand a caller an executor. Held here, not in the policy, so a
# policy edit cannot drop the requirement that their use be pinned to exact
# reviewed call sites.
EXECUTOR_DEPENDENCIES = frozenset(
    {
        "tokio",
        "tokio-uring",
        "async-std",
        "smol",
        "rayon",
        "futures-executor",
        "async-global-executor",
    }
)

# An admitted item path whose leaf matches this is an executor entry point and
# must be pinned to exact call sites by the source contract.
EXECUTOR_ITEM_RE = re.compile(
    r"(?:^|::)(?:spawn|spawn_blocking|spawn_local|spawn_pinned|block_on|block_in_place"
    r"|Runtime|Builder|Handle|JoinSet|LocalSet|scope)$"
)

# Capability symbols no source-contracted crate may name, whatever the Cargo
# graph says. A policy may add to this floor; it can never remove from it.
FORBIDDEN_SOURCE_SYMBOL_FLOOR: tuple[tuple[str, str], ...] = (
    (r"\bstd::fs\b", "filesystem access"),
    (r"\bstd::net\b", "network access"),
    (r"\bstd::process\b", "process construction"),
    (r"\bstd::os::unix::net\b", "unix socket access"),
    (r"\bstd::thread::(?:spawn|Builder)\b", "OS thread spawning"),
    (r"\bthread::spawn\b", "OS thread spawning"),
    (r"\btokio::runtime\b", "async runtime construction"),
    (r"\bruntime::(?:Runtime|Builder|Handle)\b", "async runtime construction"),
    (r"\bRuntime::new\b", "async runtime construction"),
    (r"\bBuilder::new_(?:multi_thread|current_thread)\b", "async runtime construction"),
    (r"\bblock_on\b", "nested executor entry"),
    (r"\bblock_in_place\b", "runtime-blocking escape hatch"),
    (r"\bspawn_local\b", "local task spawning"),
    (r"\bLocalSet\b", "local task set"),
    (r"\bJoinSet\b", "unbounded task set"),
    (r"\brusqlite\b", "embedded store handle"),
    (r"\bTcpStream\b|\bTcpListener\b|\bUnixStream\b|\bUnixListener\b", "socket transport"),
    (r"\bCommand::new\b", "process construction"),
    (r"\breqwest\b|\bhyper\b|\bureq\b", "HTTP stack"),
    (r"\bgix::|\bgit2::", "git object store"),
    (r"NativeHistoricalBlobReader", "concrete git historical-blob reader"),
)

IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
FN_RE = re.compile(
    r"^\s*(?:pub(?:\s*\([^)]*\))?\s+)?(?:default\s+)?(?:const\s+)?(?:async\s+)?"
    r"(?:unsafe\s+)?(?:extern\s+\"[^\"]*\"\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)"
)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return value


def dependency_entries(package: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize Cargo dependency metadata, keeping kind, features, optionality.

    Cargo reports `kind: null` for a normal dependency and `"dev"`/`"build"`
    otherwise. A bare string dependency (used by focused fixtures) is a normal
    edge with default features and no extra features.
    """
    label = package.get("name", "<unknown>")
    entries: list[dict[str, Any]] = []
    for dependency in package.get("dependencies", []):
        if isinstance(dependency, str):
            entries.append(
                {
                    "name": dependency,
                    "kind": KIND_NORMAL,
                    "features": [],
                    "uses_default_features": True,
                    "optional": False,
                }
            )
            continue
        if not isinstance(dependency, dict) or not isinstance(dependency.get("name"), str):
            raise ValueError(f"package {label} has malformed dependency metadata")
        raw_kind = dependency.get("kind")
        if raw_kind is None:
            kind = KIND_NORMAL
        elif isinstance(raw_kind, str) and raw_kind in DEPENDENCY_KINDS:
            kind = raw_kind
        else:
            raise ValueError(
                f"package {label} dependency {dependency['name']} has an unrecognized kind"
            )
        features = dependency.get("features", [])
        if not isinstance(features, list) or not all(
            isinstance(item, str) for item in features
        ):
            raise ValueError(
                f"package {label} dependency {dependency['name']} has malformed features"
            )
        uses_default_features = dependency.get("uses_default_features", True)
        if not isinstance(uses_default_features, bool):
            raise ValueError(
                f"package {label} dependency {dependency['name']} has malformed"
                " uses_default_features"
            )
        optional = dependency.get("optional", False)
        if not isinstance(optional, bool):
            raise ValueError(
                f"package {label} dependency {dependency['name']} has malformed optional"
            )
        entries.append(
            {
                "name": dependency["name"],
                "kind": kind,
                "features": features,
                "uses_default_features": uses_default_features,
                "optional": optional,
            }
        )
    return entries


def dependency_names(
    package: dict[str, Any], kinds: tuple[str, ...] = PRODUCTION_KINDS
) -> set[str]:
    """Names of the package's dependency edges of the requested kinds.

    Defaults to production kinds: dev edges are governed by their own exact
    allowlists so a test fixture edge is never mistaken for a shipped one.
    """
    return {entry["name"] for entry in dependency_entries(package) if entry["kind"] in kinds}


def package_has_optional_dependency(package: dict[str, Any]) -> bool:
    """True when this crate can export more than its bare contract surface.

    An optional dependency means some feature of this crate mounts extra code.
    Cargo unifies features across a build graph, so a consumer's own
    `default-features = false` does not bound what this crate exports in the
    production artifact. Any consumer edge to such a crate therefore needs an
    exact source-import allowlist, not just a manifest allowlist.
    """
    try:
        return any(entry["optional"] for entry in dependency_entries(package))
    except ValueError:
        return False


def matches_any(value: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)


def crate_root_ident(package_name: str) -> str:
    return package_name.replace("-", "_")


def strip_rust_comments(source: str) -> str:
    """Remove line and block comments, preserving offsets with spaces.

    Offsets are preserved so line numbers stay exact for call-site reporting.
    String literals are tracked so a `//` inside a string is not mistaken for
    a comment; a comment can never hide code, so removing them only removes
    prose that would otherwise be scanned as if it were an import.
    """
    out = list(source)
    index = 0
    length = len(source)
    while index < length:
        char = source[index]
        if char == '"':
            index += 1
            while index < length:
                if source[index] == "\\":
                    index += 2
                    continue
                if source[index] == '"':
                    index += 1
                    break
                index += 1
            continue
        if char == "'" and index + 2 < length and source[index + 2] == "'":
            index += 3
            continue
        if source.startswith("//", index):
            while index < length and source[index] != "\n":
                out[index] = " "
                index += 1
            continue
        if source.startswith("/*", index):
            depth = 1
            out[index] = out[index + 1] = " "
            index += 2
            while index < length and depth:
                if source.startswith("/*", index):
                    depth += 1
                    out[index] = out[index + 1] = " "
                    index += 2
                    continue
                if source.startswith("*/", index):
                    depth -= 1
                    out[index] = out[index + 1] = " "
                    index += 2
                    continue
                if source[index] != "\n":
                    out[index] = " "
                index += 1
            continue
        index += 1
    return "".join(out)


def _capture_path_expression(text: str, start: int) -> tuple[str, int]:
    """Capture a Rust path expression, including balanced `use` brace groups."""
    index = start
    depth = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char == "{":
            depth += 1
            index += 1
        elif char == "}":
            if depth == 0:
                break
            depth -= 1
            index += 1
            if depth == 0:
                break
        elif char.isalnum() or char == "_":
            index += 1
        elif text.startswith("::", index):
            index += 2
        elif char == "*":
            index += 1
        elif depth > 0 and (char.isspace() or char == ","):
            index += 1
        else:
            break
    return text[start:index], index


def _split_top_level(text: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in text:
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    parts.append("".join(current))
    return [part.strip() for part in parts if part.strip()]


def _expand_path_expression(expression: str) -> list[str]:
    """Expand `a::{b, c::{d}}` into `['a::b', 'a::c::d']`."""
    expression = expression.strip()
    if not expression:
        return []
    depth = 0
    open_at = -1
    close_at = -1
    for index, char in enumerate(expression):
        if char == "{":
            if depth == 0 and open_at < 0:
                open_at = index
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0 and close_at < 0:
                close_at = index
                break
    if open_at < 0 or close_at < 0:
        # Unbalanced or brace-free: treat the leading path as one leaf. A
        # struct-literal brace (`Foo { field }`) lands here and yields `Foo`.
        head = expression.split("{", 1)[0]
        return [head.strip().strip(":").strip()] if head.strip().strip(":") else []
    prefix = expression[:open_at].strip().rstrip(":").strip()
    inner = expression[open_at + 1 : close_at]
    leaves: list[str] = []
    for part in _split_top_level(inner):
        for leaf in _expand_path_expression(part):
            if leaf == "self":
                if prefix:
                    leaves.append(prefix)
            elif prefix:
                leaves.append(f"{prefix}::{leaf}")
            else:
                leaves.append(leaf)
    return leaves


def extract_crate_paths(source: str, root: str) -> list[tuple[str, int]]:
    """Every item path rooted at `root`, with the 1-based line it appears on."""
    found: list[tuple[str, int]] = []
    pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(root)}::")
    for match in pattern.finditer(source):
        expression, _ = _capture_path_expression(source, match.end())
        line = source.count("\n", 0, match.start()) + 1
        leaves = _expand_path_expression(expression)
        if not leaves:
            found.append((root, line))
            continue
        for leaf in leaves:
            found.append((f"{root}::{leaf}", line))
    return found


def enclosing_function(lines: list[str], line_number: int) -> str:
    for index in range(min(line_number, len(lines)) - 1, -1, -1):
        match = FN_RE.match(lines[index])
        if match:
            return match.group(1)
    return "<module>"


def crate_source_files(repo: Path, package_name: str) -> list[Path]:
    directory = repo / "crates" / package_name / "src"
    if not directory.is_dir():
        return []
    return sorted(path for path in directory.rglob("*.rs") if path.is_file())


def check_source_contracts(
    repo: Path,
    policy: dict[str, Any],
    packages: dict[str, dict[str, Any]],
    contracts: list[dict[str, Any]],
) -> list[str]:
    """Layer 2: exact source-import allowlists and pinned executor call sites."""
    errors: list[str] = []
    raw_source_contracts = policy.get("source_contracts", [])
    if not isinstance(raw_source_contracts, list):
        return ["policy source_contracts must be an array"]
    source_contracts: dict[str, dict[str, Any]] = {}
    for entry in raw_source_contracts:
        if not isinstance(entry, dict):
            errors.append("source contract must be an object")
            continue
        name = entry.get("package")
        if not isinstance(name, str) or not name:
            errors.append("source contract package must be a non-empty string")
            continue
        if name in source_contracts:
            errors.append(f"duplicate source contract for {name}")
            continue
        source_contracts[name] = entry

    # Derive, from metadata alone, which edges MUST carry a source contract.
    # Nothing here reads a crate name out of the policy, so a policy edit
    # cannot delete the obligation.
    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        package_name = contract.get("package")
        if not isinstance(package_name, str) or not package_name:
            continue
        allowed = contract.get("allowed_direct_dependencies", [])
        if not isinstance(allowed, list):
            continue
        needed: list[tuple[str, str]] = []
        for target in sorted({item for item in allowed if isinstance(item, str)}):
            target_package = packages.get(target)
            if target in EXECUTOR_DEPENDENCIES:
                needed.append((target, "it hands the crate an executor"))
            elif target_package is not None and package_has_optional_dependency(
                target_package
            ):
                needed.append(
                    (
                        target,
                        "it declares an optional dependency, so Cargo feature "
                        "unification can widen what it exports in the production "
                        "build regardless of this edge's own feature request",
                    )
                )
        if not needed:
            continue
        source_contract = source_contracts.get(package_name)
        if source_contract is None:
            for target, why in needed:
                errors.append(
                    f"{package_name} allows production dependency {target} but declares"
                    f" no source contract; one is mandatory because {why}"
                )
            continue
        imports = source_contract.get("allowed_imports", {})
        if not isinstance(imports, dict):
            errors.append(f"{package_name} source contract allowed_imports must be an object")
            continue
        for target, why in needed:
            if crate_root_ident(target) not in imports:
                errors.append(
                    f"{package_name} source contract has no allowed_imports entry for"
                    f" {target}; one is mandatory because {why}"
                )

    for package_name, contract in sorted(source_contracts.items()):
        imports = contract.get("allowed_imports", {})
        if not isinstance(imports, dict) or not all(
            isinstance(key, str)
            and key
            and isinstance(values, list)
            and all(isinstance(item, str) and item for item in values)
            for key, values in imports.items()
        ):
            errors.append(
                f"{package_name} source contract allowed_imports must map a crate root"
                " to an array of exact item paths"
            )
            continue
        call_sites = contract.get("executor_call_sites", {})
        if not isinstance(call_sites, dict) or not all(
            isinstance(key, str)
            and key
            and isinstance(values, list)
            and values
            and all(isinstance(item, str) and item for item in values)
            for key, values in call_sites.items()
        ):
            errors.append(
                f"{package_name} source contract executor_call_sites must map an item"
                " path to a non-empty array of file::function sites"
            )
            continue
        extra_forbidden = contract.get("forbidden_source_symbols", [])
        if not isinstance(extra_forbidden, list) or not all(
            isinstance(item, str) and item for item in extra_forbidden
        ):
            errors.append(
                f"{package_name} source contract forbidden_source_symbols must be strings"
            )
            continue

        # Every admitted executor entry point must be pinned to exact sites.
        for root, paths in sorted(imports.items()):
            if root.replace("_", "-") not in EXECUTOR_DEPENDENCIES:
                continue
            for path in sorted(paths):
                if EXECUTOR_ITEM_RE.search(path) and path not in call_sites:
                    errors.append(
                        f"{package_name} admits executor item {path} without pinning it"
                        " to exact call sites in executor_call_sites"
                    )

        files = crate_source_files(repo, package_name)
        if not files:
            errors.append(
                f"{package_name} has a source contract but no readable crate source at"
                f" crates/{package_name}/src"
            )
            continue

        seen_paths: set[str] = set()
        seen_sites: dict[str, set[str]] = {path: set() for path in call_sites}
        for file_path in files:
            relative = file_path.relative_to(repo / "crates" / package_name / "src").as_posix()
            try:
                raw = file_path.read_text(encoding="utf-8")
            except OSError as error:
                errors.append(f"{package_name} cannot read {relative}: {error}")
                continue
            source = strip_rust_comments(raw)
            lines = source.splitlines()

            for pattern, label in FORBIDDEN_SOURCE_SYMBOL_FLOOR + tuple(
                (item, "forbidden by policy") for item in extra_forbidden
            ):
                for match in re.finditer(pattern, source):
                    line = source.count("\n", 0, match.start()) + 1
                    errors.append(
                        f"forbidden source symbol in {package_name} ({label}):"
                        f" {relative}:{line} names {match.group(0)!r}"
                    )

            for root, allowed_paths in sorted(imports.items()):
                allowed_set = set(allowed_paths)
                for path, line in extract_crate_paths(source, root):
                    if path not in allowed_set:
                        errors.append(
                            f"forbidden source import in {package_name}:"
                            f" {relative}:{line} names {path}, which is outside the"
                            " exact reviewed import allowlist"
                        )
                        continue
                    seen_paths.add(path)
                    if path in call_sites:
                        site = f"{relative}::{enclosing_function(lines, line)}"
                        seen_sites[path].add(site)
                        if site not in call_sites[path]:
                            errors.append(
                                f"unreviewed executor call site in {package_name}:"
                                f" {path} at {relative}:{line} is in {site}, which is"
                                " not a pinned call site"
                            )

        for root, allowed_paths in sorted(imports.items()):
            for path in sorted(set(allowed_paths) - seen_paths):
                errors.append(
                    f"stale source import allowance in {package_name}: {path} is"
                    " allowed but never used; the allowlist must stay the exact"
                    " reviewed set"
                )
        for path, sites in sorted(seen_sites.items()):
            for site in sorted(set(call_sites[path]) - sites):
                errors.append(
                    f"stale executor call site in {package_name}: {path} is pinned to"
                    f" {site}, which no longer exists"
                )
    return errors


def validate_exceptions(
    repo: Path, policy: dict[str, Any]
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], list[str]]:
    errors: list[str] = []
    index: dict[tuple[str, str, str], dict[str, Any]] = {}
    exceptions = policy.get("exceptions", [])
    if not isinstance(exceptions, list):
        return index, ["policy exceptions must be an array"]
    for offset, exception in enumerate(exceptions):
        label = f"exception[{offset}]"
        if not isinstance(exception, dict):
            errors.append(f"{label} must be an object")
            continue
        for field in REQUIRED_EXCEPTION_FIELDS:
            value = exception.get(field)
            if field == "verification":
                if not isinstance(value, list) or not value or not all(
                    isinstance(item, str) and item.strip() for item in value
                ):
                    errors.append(f"{label}.{field} must be a non-empty string array")
            elif not isinstance(value, str) or not value.strip():
                errors.append(f"{label}.{field} must be a non-empty string")
        review_after = exception.get("review_after")
        if isinstance(review_after, str) and not DATE_RE.fullmatch(review_after):
            errors.append(f"{label}.review_after must use YYYY-MM-DD")
        adr = exception.get("adr")
        if isinstance(adr, str) and adr:
            adr_path = Path(adr)
            if adr_path.is_absolute() or ".." in adr_path.parts:
                errors.append(f"{label}.adr must be a repository-relative path")
            elif not adr.startswith("product/architecture/adr/"):
                errors.append(f"{label}.adr must live under product/architecture/adr")
            elif not (repo / adr_path).is_file():
                errors.append(f"{label}.adr does not exist: {adr}")
        key_values = (
            exception.get("rule_id"),
            exception.get("from_package"),
            exception.get("to_package"),
        )
        if all(isinstance(value, str) and value for value in key_values):
            key = (key_values[0], key_values[1], key_values[2])
            if key in index:
                errors.append(
                    f"duplicate exception for rule {key[0]} edge {key[1]} -> {key[2]}"
                )
            else:
                index[key] = exception
    return index, errors


def load_metadata(
    repo: Path, policy: dict[str, Any], fixture: Path | None
) -> dict[str, Any]:
    if fixture is not None:
        return load_json(fixture)
    command = policy.get("metadata_command")
    if not isinstance(command, list) or not command or not all(
        isinstance(item, str) and item for item in command
    ):
        raise ValueError("policy metadata_command must be a non-empty string array")
    result = subprocess.run(
        command,
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ValueError(f"cargo metadata failed ({result.returncode}): {detail}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ValueError(f"cargo metadata returned invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ValueError("cargo metadata root must be an object")
    return value


def check_policy(
    repo: Path,
    policy: dict[str, Any],
    metadata: dict[str, Any],
    source_repo: Path | None = None,
) -> list[str]:
    """Evaluate the whole policy.

    `repo` roots ADR lookups; `source_repo` roots crate source scanning and
    defaults to `repo`. They are separable only so focused exception tests can
    stage an ADR tree in a temporary directory while still scanning the real
    crate source. Production always passes one repository root for both.
    """
    source_root = repo if source_repo is None else source_repo
    errors: list[str] = []
    if policy.get("schema_version") != 1:
        errors.append("policy schema_version must be 1")
    raw_packages = metadata.get("packages")
    if not isinstance(raw_packages, list):
        return errors + ["cargo metadata packages must be an array"]
    packages: dict[str, dict[str, Any]] = {}
    for package in raw_packages:
        if not isinstance(package, dict) or not isinstance(package.get("name"), str):
            errors.append("cargo metadata contains a malformed package")
            continue
        name = package["name"]
        if name in packages:
            errors.append(f"cargo metadata contains duplicate package {name}")
        packages[name] = package

    exceptions, exception_errors = validate_exceptions(repo, policy)
    errors.extend(exception_errors)
    used_exceptions: set[tuple[str, str, str]] = set()

    def reject_or_except(rule_id: str, source: str, target: str, reason: str) -> None:
        key = (rule_id, source, target)
        if key in exceptions:
            used_exceptions.add(key)
        else:
            errors.append(
                f"forbidden dependency {source} -> {target} ({rule_id}): {reason}"
            )

    contracts = policy.get("package_contracts", [])
    if not isinstance(contracts, list):
        errors.append("policy package_contracts must be an array")
        contracts = []
    for contract in contracts:
        if not isinstance(contract, dict):
            errors.append("package contract must be an object")
            continue
        package_name = contract.get("package")
        if not isinstance(package_name, str) or not package_name:
            errors.append("package contract package must be a non-empty string")
            continue
        package = packages.get(package_name)
        if package is None:
            errors.append(f"managed package is missing from cargo metadata: {package_name}")
            continue
        allowed = contract.get("allowed_direct_dependencies", [])
        required = contract.get("required_direct_dependencies", [])
        allowed_dev = contract.get("allowed_dev_dependencies", [])
        if not isinstance(allowed, list) or not all(
            isinstance(item, str) for item in allowed
        ):
            errors.append(f"{package_name} allowed_direct_dependencies must be strings")
            continue
        if not isinstance(required, list) or not all(
            isinstance(item, str) for item in required
        ):
            errors.append(f"{package_name} required_direct_dependencies must be strings")
            continue
        if not isinstance(allowed_dev, list) or not all(
            isinstance(item, str) for item in allowed_dev
        ):
            errors.append(f"{package_name} allowed_dev_dependencies must be strings")
            continue
        allowed_features = contract.get("allowed_dependency_features", {})
        if not isinstance(allowed_features, dict) or not all(
            isinstance(key, str)
            and isinstance(values, list)
            and all(isinstance(item, str) and item for item in values)
            for key, values in allowed_features.items()
        ):
            errors.append(
                f"{package_name} allowed_dependency_features must map a dependency"
                " name to an array of feature names"
            )
            continue
        default_features_off = contract.get(
            "dependencies_requiring_default_features_off", []
        )
        if not isinstance(default_features_off, list) or not all(
            isinstance(item, str) for item in default_features_off
        ):
            errors.append(
                f"{package_name} dependencies_requiring_default_features_off must be strings"
            )
            continue
        allowed_set = set(allowed)
        required_set = set(required)
        allowed_dev_set = set(allowed_dev)
        if not required_set.issubset(allowed_set):
            errors.append(f"{package_name} required dependencies must also be allowed")
        try:
            entries = dependency_entries(package)
        except ValueError as error:
            errors.append(str(error))
            continue
        production = {
            entry["name"] for entry in entries if entry["kind"] in PRODUCTION_KINDS
        }
        dev_only = {
            entry["name"] for entry in entries if entry["kind"] == KIND_DEV
        } - production
        for missing in sorted(required_set - production):
            errors.append(f"required dependency is missing: {package_name} -> {missing}")
        for target in sorted(production - allowed_set):
            reject_or_except(
                f"package-contract:{package_name}",
                package_name,
                target,
                "dependency is outside the package allowlist",
            )
        # Test-only edges are default-deny too; they simply answer to their own
        # exact allowlist, because a dev edge is not compiled into the shipped
        # artifact and therefore cannot ship a capability.
        for target in sorted(dev_only - allowed_set - allowed_dev_set):
            reject_or_except(
                f"package-contract-dev:{package_name}",
                package_name,
                target,
                "dev-dependency is outside the package test allowlist",
            )
        # Exact feature allowlist, default-deny. An allowed edge may enable only
        # the features this contract reviewed for it; anything else -- an
        # ambient clock, an I/O reactor, an unreviewed capability feature with a
        # name nobody predicted -- is refused. Scoped to production kinds
        # because only those edges are compiled into the shipped artifact.
        for entry in entries:
            if entry["kind"] not in PRODUCTION_KINDS:
                continue
            permitted = set(allowed_features.get(entry["name"], []))
            unreviewed = sorted(set(entry["features"]) - permitted)
            if unreviewed:
                errors.append(
                    f"unreviewed dependency feature: {package_name} -> "
                    f"{entry['name']} enables {', '.join(unreviewed)}"
                )
            if entry["name"] in default_features_off and entry["uses_default_features"]:
                errors.append(
                    f"dependency must set default-features = false: "
                    f"{package_name} -> {entry['name']}"
                )

    rules = policy.get("rules", [])
    if not isinstance(rules, list):
        errors.append("policy rules must be an array")
        rules = []
    for rule in rules:
        if not isinstance(rule, dict):
            errors.append("dependency rule must be an object")
            continue
        rule_id = rule.get("id")
        from_patterns = rule.get("from_packages", [])
        forbidden_patterns = rule.get("forbidden_dependencies", [])
        allowed_patterns = rule.get("allowed_dependencies", [])
        allowed_dev_patterns = rule.get("allowed_dev_dependencies", [])
        reason = rule.get("reason")
        if not isinstance(rule_id, str) or not rule_id:
            errors.append("dependency rule id must be a non-empty string")
            continue
        if not isinstance(reason, str) or not reason:
            errors.append(f"dependency rule {rule_id} requires a reason")
        if not all(
            isinstance(values, list)
            and all(isinstance(item, str) for item in values)
            for values in (
                from_patterns,
                forbidden_patterns,
                allowed_patterns,
                allowed_dev_patterns,
            )
        ):
            errors.append(f"dependency rule {rule_id} patterns must be string arrays")
            continue
        for source, package in packages.items():
            if not matches_any(source, from_patterns):
                continue
            try:
                entries = dependency_entries(package)
            except ValueError as error:
                errors.append(str(error))
                continue
            production = {
                entry["name"] for entry in entries if entry["kind"] in PRODUCTION_KINDS
            }
            dev_only = {
                entry["name"] for entry in entries if entry["kind"] == KIND_DEV
            } - production
            for target in sorted(production):
                if matches_any(target, forbidden_patterns) and not matches_any(
                    target, allowed_patterns
                ):
                    reject_or_except(
                        rule_id,
                        source,
                        target,
                        reason or "forbidden by policy",
                    )
            for target in sorted(dev_only):
                if matches_any(target, forbidden_patterns) and not (
                    matches_any(target, allowed_patterns)
                    or matches_any(target, allowed_dev_patterns)
                ):
                    reject_or_except(
                        rule_id,
                        source,
                        target,
                        reason or "forbidden by policy",
                    )

    errors.extend(check_source_contracts(source_root, policy, packages, contracts))

    for key in sorted(set(exceptions) - used_exceptions):
        errors.append(
            f"unused dependency exception {key[0]} for edge {key[1]} -> {key[2]}"
        )
    return errors


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".", help="repository root")
    parser.add_argument(
        "--policy",
        default="product/architecture/memory-dependency-policy.json",
        help="policy path relative to the repository",
    )
    parser.add_argument(
        "--metadata-fixture",
        help="read Cargo metadata JSON from this path instead of invoking Cargo",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    repo = Path(args.repo).resolve()
    policy_path = Path(args.policy)
    if not policy_path.is_absolute():
        policy_path = repo / policy_path
    fixture = Path(args.metadata_fixture).resolve() if args.metadata_fixture else None
    try:
        policy = load_json(policy_path)
        metadata = load_metadata(repo, policy, fixture)
        errors = check_policy(repo, policy, metadata)
    except ValueError as error:
        print(f"memory dependency policy error: {error}", file=sys.stderr)
        return 2
    if errors:
        for error in errors:
            print(f"memory dependency policy violation: {error}", file=sys.stderr)
        return 1
    print("memory dependency direction verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
