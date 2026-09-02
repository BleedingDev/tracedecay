#!/usr/bin/env python3
"""Minimal, fail-closed Rust region reader shared by the memory product gates.

Text gates over Rust are only honest if they can tell code from comments and
strings, and a production item from a ``#[cfg(test)]`` one.  Indentation and
raw substring position cannot: Rust permits indented top-level items, so "the
tail of the file after the test marker is all test code" is false, and
"``X`` appears somewhere after ``Y`` appears" says nothing about whether ``X``
is *inside* ``Y``.

This module gives the gates three primitives instead:

``code_mask``
    the source with every comment, string, raw string and char literal blanked
    to spaces (newlines preserved, so offsets and line numbers still line up).
    Structural searches run against the mask; exact-fragment searches run
    against the original text.

``strip_cfg_test_modules``
    removes every ``#[cfg(...test...)]``-gated ``mod`` by balanced braces,
    wherever it sits and whatever it is called.  Anything after the module's
    real closing brace stays in the production region, indented or not.

``block_after`` / ``body_of``
    balanced-brace extraction, so a check can be scoped to one function body
    or one match arm rather than to the whole file.

Every primitive fails closed: an unbalanced or unreadable construct raises
``RustParseError`` and the calling gate turns that into a violation rather
than into a silent pass.
"""

from __future__ import annotations

import re

__all__ = [
    "RustParseError",
    "block_after",
    "body_of",
    "code_mask",
    "find_all",
    "match_arm_patterns",
    "string_literals",
    "strip_cfg_test_modules",
]


class RustParseError(Exception):
    """The source could not be read structurally; the caller must fail."""


_RAW_STRING = re.compile(r'(?:b|c)?r(#*)"')
_IDENT_CHAR = re.compile(r"[A-Za-z0-9_]")


def _blank(out: list[str], start: int, end: int) -> None:
    for index in range(start, end):
        if out[index] != "\n":
            out[index] = " "


def _scan(text: str) -> list[tuple[str, int, int]]:
    """Return `(kind, start, end)` spans for comments and literals.

    `kind` is one of ``comment``, ``string`` or ``char``.  Everything not
    covered by a returned span is code.
    """

    spans: list[tuple[str, int, int]] = []
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char == "/" and text.startswith("//", index):
            end = text.find("\n", index)
            end = length if end == -1 else end
            spans.append(("comment", index, end))
            index = end
        elif char == "/" and text.startswith("/*", index):
            depth = 1
            cursor = index + 2
            while cursor < length and depth:
                if text.startswith("/*", cursor):
                    depth += 1
                    cursor += 2
                elif text.startswith("*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            if depth:
                raise RustParseError("unterminated block comment")
            spans.append(("comment", index, cursor))
            index = cursor
        elif (
            char in "brc"
            and (index == 0 or not _IDENT_CHAR.match(text[index - 1]))
            and (match := _RAW_STRING.match(text, index))
        ):
            terminator = '"' + match.group(1)
            end = text.find(terminator, match.end())
            if end == -1:
                raise RustParseError("unterminated raw string literal")
            end += len(terminator)
            spans.append(("string", index, end))
            index = end
        elif char == '"' or (
            char in "bc"
            and index + 1 < length
            and text[index + 1] == '"'
            and (index == 0 or not _IDENT_CHAR.match(text[index - 1]))
        ):
            cursor = index + (1 if char == '"' else 2)
            while cursor < length:
                if text[cursor] == "\\":
                    cursor += 2
                    continue
                if text[cursor] == '"':
                    cursor += 1
                    break
                cursor += 1
            else:
                raise RustParseError("unterminated string literal")
            spans.append(("string", index, cursor))
            index = cursor
        elif char == "'":
            # A char literal, or a lifetime.  Only the literal is blanked; a
            # lifetime is ordinary code and must stay visible to the mask.
            if text.startswith("\\", index + 1):
                cursor = index + 2
                while cursor < length and text[cursor] != "'":
                    cursor += 1
                if cursor >= length:
                    raise RustParseError("unterminated char literal")
                spans.append(("char", index, cursor + 1))
                index = cursor + 1
            elif index + 2 < length and text[index + 2] == "'":
                spans.append(("char", index, index + 3))
                index += 3
            else:
                index += 1
        else:
            index += 1
    return spans


def code_mask(text: str) -> str:
    """Return `text` with comments and literals blanked out to spaces."""

    out = list(text)
    for _kind, start, end in _scan(text):
        _blank(out, start, end)
    return "".join(out)


def block_after(mask: str, start: int) -> tuple[int, int]:
    """Return `(open, close)` offsets of the first balanced `{...}` at/after `start`.

    `close` is the offset of the closing brace itself.
    """

    open_index = mask.find("{", start)
    if open_index == -1:
        raise RustParseError("expected a block, found none")
    depth = 0
    for index in range(open_index, len(mask)):
        if mask[index] == "{":
            depth += 1
        elif mask[index] == "}":
            depth -= 1
            if depth == 0:
                return open_index, index
    raise RustParseError("unbalanced braces")


def body_of(text: str, mask: str, marker: str) -> tuple[int, int]:
    """Return `(start, end)` of the body of the single item introduced by `marker`.

    `marker` is matched against the code mask, so it never matches inside a
    comment or a string.  Exactly one occurrence is required: an ambiguous
    marker is a gate defect, not a pass.
    """

    hits = find_all(mask, marker)
    if len(hits) != 1:
        raise RustParseError(
            f"marker must occur exactly once in code, found {len(hits)}: {marker!r}"
        )
    open_index, close_index = block_after(mask, hits[0])
    return open_index + 1, close_index


def find_all(haystack: str, needle: str) -> list[int]:
    """Every offset of `needle` in `haystack`."""

    hits: list[int] = []
    start = 0
    while True:
        found = haystack.find(needle, start)
        if found == -1:
            return hits
        hits.append(found)
        start = found + 1


_CFG_ATTRIBUTE = re.compile(r"#\[cfg\(")
_TEST_TOKEN = re.compile(r"\btest\b")
_MOD_ITEM = re.compile(r"\bmod\s+[A-Za-z_][A-Za-z0-9_]*\s*(\{|;)")


def _attribute_span(mask: str, start: int) -> int:
    """Return the offset just past the `#[...]` attribute beginning at `start`."""

    open_index = mask.find("[", start)
    if open_index == -1:
        raise RustParseError("malformed attribute")
    depth = 0
    for index in range(open_index, len(mask)):
        if mask[index] == "[":
            depth += 1
        elif mask[index] == "]":
            depth -= 1
            if depth == 0:
                return index + 1
    raise RustParseError("unbalanced attribute brackets")


def strip_cfg_test_modules(text: str) -> str:
    """Return `text` with every `#[cfg(..test..)]`-gated `mod` item removed.

    The module is delimited by balanced braces (or by its `;`), so its
    position in the file and the indentation of whatever follows it are
    irrelevant: an indented production item after the test module stays in the
    returned production region and is still scanned.
    """

    mask = code_mask(text)
    removals: list[tuple[int, int]] = []
    for attribute in _CFG_ATTRIBUTE.finditer(mask):
        start = attribute.start()
        end = _attribute_span(mask, start)
        if not _TEST_TOKEN.search(mask[start:end]):
            continue
        # Skip any further attributes (`#[path = "..."]`) and whitespace.
        cursor = end
        while cursor < len(mask):
            if mask[cursor].isspace():
                cursor += 1
                continue
            if mask[cursor] == "#":
                cursor = _attribute_span(mask, cursor)
                continue
            break
        item = _MOD_ITEM.match(mask, cursor)
        if item is None:
            # A cfg(test) item that is not a module (a gated enum variant, a
            # gated parameter, a gated match arm).  Left in place on purpose:
            # keeping it is the fail-closed direction.
            continue
        if item.group(1) == ";":
            removals.append((start, item.end()))
            continue
        _, close_index = block_after(mask, item.end() - 1)
        removals.append((start, close_index + 1))
    if not removals:
        return text
    kept: list[str] = []
    cursor = 0
    for start, end in sorted(removals):
        if start < cursor:
            raise RustParseError("overlapping cfg(test) module spans")
        kept.append(text[cursor:start])
        cursor = end
    kept.append(text[cursor:])
    return "".join(kept)


def string_literals(text: str) -> list[tuple[int, str]]:
    """Return `(offset, literal)` for every real string literal in `text`.

    Literals are located by the same scanner that builds the mask, so a
    "string" inside a comment is never reported and a closing quote is never
    mistaken for an opening one.
    """

    return [
        (start, text[start:end])
        for kind, start, end in _scan(text)
        if kind == "string"
    ]


def match_arm_patterns(mask: str, text: str, open_index: int, close_index: int) -> list[str]:
    """Return the top-level arm patterns of the `match` block `{open..close}`.

    Nested blocks, tuples and calls are skipped by depth, so a `=>` inside an
    arm body is never mistaken for a new arm.  Whitespace inside each returned
    pattern is normalised so the caller can compare against an exact expected
    table -- which is the point: an *added* arm shifts the list and fails,
    where "every required arm is present somewhere" would not.
    """

    patterns: list[str] = []
    depth = 0
    index = open_index + 1
    arm_start = index
    arm_start_body = index
    in_body = False
    body_is_block = False
    while index <= close_index:
        char = mask[index]
        if char in "([{":
            if in_body and char == "{" and not text[arm_start_body:index].strip():
                body_is_block = True
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth < 0:
                break
            if depth == 0 and in_body and body_is_block:
                in_body = False
                body_is_block = False
                arm_start = index + 1
        elif depth == 0 and in_body and char == ",":
            in_body = False
            arm_start = index + 1
        elif depth == 0 and not in_body and mask.startswith("=>", index):
            patterns.append(" ".join(text[arm_start:index].split()))
            in_body = True
            body_is_block = False
            index += 2
            arm_start_body = index
            continue
        index += 1
    if depth > 0:
        raise RustParseError("unbalanced match block")
    return patterns
