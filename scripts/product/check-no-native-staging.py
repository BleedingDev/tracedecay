#!/usr/bin/env python3
"""Reject the retired provider-local Native staging path.

Native is a projection over the daemon's canonical V2 authorities.  A Native
implementation must therefore not grow a second observation database, a
provider-owned schema migration, or a direct reopen of the host observation
journal.  This is a deliberately small source gate: it checks only the
production Native source paths and leaves unrelated migration code elsewhere in
the workspace alone.

Rust comments, raw strings, and test modules are removed before code markers
are checked.  Actual string literals are retained for the two markers that
describe durable storage (the retired SQLite filename and provider-local DDL).
The parser is fail-closed: an unreadable or structurally malformed Rust file
is a violation rather than a pass.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _rust_region import (  # noqa: E402
    RustParseError,
    code_mask,
    string_literals,
    strip_cfg_test_modules,
)


# Keep this scope explicit.  Native integration, git, and session files also
# use the word "native" but are not the provider implementation this gate
# protects.  The provider-control directory is included because its authority
# loader can reopen provider observation journals for the Native mount.
NATIVE_PRODUCTION_GLOBS = (
    "crates/*/src/retained_owner/native*.rs",
    "crates/*/src/retained_owner/provider_control/*.rs",
    "crates/*/src/native_provider*.rs",
    "crates/*/src/memory_provider_native*.rs",
    "crates/tracedecay-memory-provider-native/src/*.rs",
)

TEST_PATH_PARTS = frozenset(
    {
        "test",
        "tests",
        "fixture",
        "fixtures",
        "bench",
        "benches",
        "examples",
    }
)

STAGED_STORE_TYPE = re.compile(r"\bStagedObservationStore\b")
DIRECT_JOURNAL_OPEN_EXISTING = re.compile(
    r"\b(?:[A-Za-z_][A-Za-z0-9_]*::)*"
    r"(?:SqliteObservationJournal|ObservationJournal)"
    r"\s*::\s*open_existing\s*\("
    r"|\b(?:observation_)?journal\s*\.\s*open_existing\s*\("
)
PROVIDER_SCHEMA_MIGRATION_OR_BACKFILL = re.compile(
    r"\b(?:migrat\w*|upgrade\w*|downgrade\w*|backfill\w*)\b",
    re.IGNORECASE,
)
PROVIDER_SCHEMA_DDL = re.compile(
    r"\b(?:CREATE|ALTER|DROP)\s+TABLE\b|\bPRAGMA\s+user_version\b",
    re.IGNORECASE,
)
RETIRED_STAGED_DATABASE_NAME = "staged-observations-v1.sqlite3"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path("."))
    return parser.parse_args(sys.argv[1:] if argv is None else argv)


def is_test_or_fixture_path(path: Path) -> bool:
    """Return whether a source path is outside production code."""

    parts = [part.casefold() for part in path.parts]
    if any(part in TEST_PATH_PARTS for part in parts):
        return True
    filename = path.name.casefold()
    return filename.endswith("_test.rs") or filename.endswith("_tests.rs")


def is_native_production_path(path: Path, repo: Path) -> bool:
    """Return whether *path* is one of the production Native source paths."""

    try:
        relative = path.relative_to(repo)
    except ValueError:
        return False
    if relative.suffix != ".rs" or is_test_or_fixture_path(relative):
        return False
    parts = relative.parts
    lower_parts = tuple(part.casefold() for part in parts)
    spelling = relative.as_posix()
    if any(fnmatch.fnmatch(spelling, pattern) for pattern in NATIVE_PRODUCTION_GLOBS):
        return True
    try:
        source_index = lower_parts.index("src")
    except ValueError:
        return False
    after_source = lower_parts[source_index + 1 :]
    if "tracedecay-memory-provider-native" in lower_parts:
        return True
    for index, component in enumerate(after_source):
        if component != "retained_owner":
            continue
        children = after_source[index + 1 :]
        if len(children) == 1 and children[0].startswith("native"):
            return True
        if len(children) == 2 and children[0] == "provider_control":
            return True
    return any(
        component.startswith("native_provider")
        or component.startswith("memory_provider_native")
        for component in after_source
    )


def native_production_paths(repo: Path) -> list[Path]:
    """Discover the sorted, production-only Native Rust files under *repo*."""

    crates = repo / "crates"
    if not crates.is_dir():
        return []
    return sorted(
        path
        for path in crates.rglob("*.rs")
        if is_native_production_path(path, repo)
    )


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def add_code_violations(
    relative: Path,
    production_text: str,
    mask: str,
    errors: list[str],
) -> None:
    checks = (
        (
            STAGED_STORE_TYPE,
            "retired Native staging type reference",
        ),
        (
            DIRECT_JOURNAL_OPEN_EXISTING,
            "direct observation journal open_existing",
        ),
        (
            PROVIDER_SCHEMA_MIGRATION_OR_BACKFILL,
            "provider-local schema migration/backfill",
        ),
    )
    for pattern, rule in checks:
        for match in pattern.finditer(mask):
            errors.append(
                f"{relative}:{line_number(production_text, match.start())}: "
                f"{rule} ({match.group(0).strip()!r})"
            )

    # DDL lives in string literals, which code_mask intentionally blanks.  A
    # literal is checked only after cfg(test) modules have been removed, so a
    # test fixture cannot make a production file fail this gate.
    for offset, literal in string_literals(production_text):
        if RETIRED_STAGED_DATABASE_NAME in literal:
            errors.append(
                f"{relative}:{line_number(production_text, offset)}: "
                "retired Native staging database filename "
                f"({RETIRED_STAGED_DATABASE_NAME!r})"
            )
        if PROVIDER_SCHEMA_DDL.search(literal):
            errors.append(
                f"{relative}:{line_number(production_text, offset)}: "
                "provider-local schema migration/backfill "
                f"({PROVIDER_SCHEMA_DDL.search(literal).group(0)!r})"
            )


def check_source(path: Path, repo: Path) -> list[str]:
    """Check one discovered source file and fail closed on parse/read errors."""

    relative = path.relative_to(repo)
    try:
        text = path.read_text(encoding="utf-8")
        production_text = strip_cfg_test_modules(text)
        mask = code_mask(production_text)
    except (OSError, UnicodeError, RustParseError) as error:
        return [f"{relative}: cannot parse production Native source: {error}"]

    errors: list[str] = []
    add_code_violations(relative, production_text, mask, errors)
    return errors


def check_repository(repo: Path) -> list[str]:
    """Return all retired Native staging violations in *repo*."""

    repo = repo.resolve()
    paths = native_production_paths(repo)
    if not paths:
        return [
            "no production Native Rust paths found; "
            "the staging guard would otherwise be vacuous"
        ]
    errors: list[str] = []
    for path in paths:
        errors.extend(check_source(path, repo))
    return errors


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo = args.repo.resolve()
    errors = check_repository(repo)
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2, sort_keys=True))
        return 1
    paths = native_production_paths(repo)
    print(
        json.dumps(
            {
                "ok": True,
                "checked_files": len(paths),
                "paths": [str(path.relative_to(repo)) for path in paths],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
