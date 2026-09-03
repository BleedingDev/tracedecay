#!/usr/bin/env python3
"""Verify the narrow, default-off, dormant TraceDecay memory-provider host mount.

Why this gate's shape changed (and which invariant still holds)
---------------------------------------------------------------
The pre-mount shape of this program had one host feature edge, kept the
enabled Native activation inside a ``#[cfg(any(test, feature =
"test-transport"))]`` match arm, and let no root-crate file outside
``project_composition.rs`` name the provider registry.  Production has since
mounted two provider-boundary consumers -- the durable observation journey and
the cognitive-recall route -- and resolves activation from the authoritative
runtime configuration instead of a compile-time pin.  Those mounts are the
point of the program, so the pre-mount checks were re-expressed.

Re-expressed, not relaxed.  Every check below is scoped to a *parsed*
production region: comments and string literals are blanked before any
structural search (``scripts/product/_rust_region.py``), every
``#[cfg(..test..)] mod`` is removed by balanced braces rather than by
indentation, and containment questions ("is this construction inside that
match arm?") are answered by brace ranges rather than by "does this offset
come after that offset".  A presence-and-order heuristic is not a proof and is
not used here.

The invariants this gate enforces:

* **Off by default.**  ``memory-provider-host`` is optional and absent from
  the whole transitive ``default`` feature closure, so a shipped default build
  compiles no registry, journal or hygiene code and behaves exactly as
  upstream.  Every host-support dependency is optional, path-local, free of
  implicit feature selection, and named by no feature but the host feature.
* **Dormant when configured off.**  Default runtime configuration is ``false``
  with no active recall route; a ``Disabled`` activation constructs no port,
  no fabric and no enabled composition; and each mounted consumer *opens* with
  its registry refusal, before any journal, ledger, replay or worker.
* **Explicit activation.**  The production entry can pass only
  ``FromRuntimeConfiguration``; the resolved activation is bound exactly once
  and never rebound, shadowed or overwritten; and the resolver keeps its
  explicit four-way table.  There is no pinned-activation seam at all: the
  selector carries exactly one variant, so no build -- test, transport or
  production -- can express an activation the runtime configuration did not
  decide.  The pinned spellings stay forbidden everywhere below.
* **Narrow registry ownership.**  Only ``project_composition.rs`` composes,
  and the enabled Native activation is constructed exactly once *inside* the
  resolved ``Some(mode)`` arm of ``mount_project_memory_provider_host``.
* **No provider-name branching outside the registry/adapter layer.**  Neither
  the composition root nor either boundary mount may compare, match or
  dispatch on a provider identity in any spelling.  Recognition lives in
  ``tracedecay_memory_provider_registry::is_mountable_active_provider``; the
  composition branches only on its boolean answer.  Naming the registry's own
  exported identity constant to *construct* the one registered provider is not
  a branch and stays legal; every comparison against it is a violation.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _rust_region import (  # noqa: E402
    RustParseError,
    block_after,
    body_of,
    code_mask,
    find_all,
    match_arm_patterns,
    string_literals,
    strip_cfg_test_modules,
)

FEATURE = "memory-provider-host"
REGISTRY_PACKAGE = "tracedecay-memory-provider-registry"
OBSERVATION_PACKAGE = "tracedecay-memory-observation"
HYGIENE_PACKAGE = "tracedecay-memory-hygiene"
REGISTRY_CRATE_IDENT = "tracedecay_memory_provider_registry"
OBSERVATION_CRATE_IDENT = "tracedecay_memory_observation"
HYGIENE_CRATE_IDENT = "tracedecay_memory_hygiene"
# The host feature carries exactly these dependency edges, in this order.  The
# journal and the hygiene pipeline joined the registry when the observation
# journey was mounted: a mounted host that cannot journal or sanitize would be
# a dispatch path with no outbox and no secret gate.  Naming them exactly keeps
# a fourth edge from being added without re-reading this gate.
HOST_FEATURE_EDGES = [
    f"dep:{REGISTRY_PACKAGE}",
    f"dep:{OBSERVATION_PACKAGE}",
    f"dep:{HYGIENE_PACKAGE}",
]
HOST_SUPPORT_DEPENDENCIES = {
    REGISTRY_PACKAGE: "../tracedecay-memory-provider-registry",
    OBSERVATION_PACKAGE: "../tracedecay-memory-observation",
    HYGIENE_PACKAGE: "../tracedecay-memory-hygiene",
}
ROOT_MANIFEST = Path("crates/tracedecay/Cargo.toml")
COMPOSITION_MOUNT = Path("crates/tracedecay/src/daemon/project_composition.rs")
ACTIVATION_HARNESS = Path("crates/tracedecay/src/daemon/production_harness.rs")
CONFIG_SOURCE = Path("crates/tracedecay/src/config.rs")
ROUTING_GATE_SOURCE = Path("crates/tracedecay-domain/src/configuration.rs")
# Files that may retain the composed host for the project-server lifetime.
# They may name the registry crate's composition type but must never compose
# providers themselves.
RETENTION_MOUNTS = (
    Path("crates/tracedecay/src/mcp/server.rs"),
    Path("crates/tracedecay/src/mcp/server/construction.rs"),
)
# These are the only root-owned files that may name the registry and the
# concrete Native adapter.  Keep this an exact path allowlist: a similarly
# named file elsewhere must still be treated as a leak.
NATIVE_PROVIDER_FILE = Path(
    "crates/tracedecay/src/daemon/retained_owner/native_provider.rs"
)
NATIVE_PROVIDER_TESTS_FILE = Path(
    "crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs"
)
NATIVE_PROVIDER_PARITY_TESTS_FILE = Path(
    "crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs"
)
NATIVE_BASELINE_TESTS_FILE = Path(
    "crates/tracedecay/src/daemon/retained_owner/native_baseline_tests.rs"
)
# The Native application port's own provider-local staging store.  It is root
# owned and feature gated exactly like the port it serves, and it names the
# registry only for the scope and error types the port hands it.
NATIVE_STAGED_OBSERVATIONS_FILE = Path(
    "crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs"
)
# The Claude Code host memory journey. It is a test-only file mounted from the
# observation journey and gated on both `test` and the feature, so it names the
# registry only for the routed provider identity it asserts against and cannot
# exist in a feature-off or non-test build.
CLAUDE_HOST_JOURNEY_TESTS_FILE = Path(
    "crates/tracedecay/src/daemon/retained_owner/claude_host_journey_tests.rs"
)
NATIVE_ADAPTER_FILES = (
    NATIVE_PROVIDER_FILE,
    NATIVE_PROVIDER_TESTS_FILE,
    NATIVE_PROVIDER_PARITY_TESTS_FILE,
    NATIVE_BASELINE_TESTS_FILE,
    NATIVE_STAGED_OBSERVATIONS_FILE,
    CLAUDE_HOST_JOURNEY_TESTS_FILE,
)
NATIVE_PROVIDER_MODULE_FILE = Path("crates/tracedecay/src/daemon/retained_owner.rs")
NATIVE_PROVIDER_MODULE_DECLARATION = (
    f'#[cfg(feature = "{FEATURE}")]\n'
    "pub(crate) mod native_provider;"
)
NATIVE_PROVIDER_TESTS_MODULE_DECLARATION = (
    '#[cfg(test)]\n'
    '#[path = "native_provider_tests.rs"]\n'
    "mod tests;"
)
NATIVE_BASELINE_TESTS_MODULE_DECLARATION = (
    '#[cfg(test)]\n'
    '#[path = "native_baseline_tests.rs"]\n'
    "mod baseline_tests;"
)
NATIVE_PROVIDER_PARITY_TESTS_MODULE_DECLARATION = (
    f'#[cfg(all(test, feature = "{FEATURE}"))]\n'
    '#[path = "retained_owner/native_provider_parity_tests.rs"]\n'
    "mod native_provider_parity_tests;"
)
NATIVE_STAGED_OBSERVATIONS_MODULE_DECLARATION = (
    f'#[cfg(feature = "{FEATURE}")]\n'
    "pub(crate) mod native_staged_observations;"
)
CLAUDE_HOST_JOURNEY_TESTS_MODULE_FILE = Path(
    "crates/tracedecay/src/daemon/retained_owner/observation_journey.rs"
)
CLAUDE_HOST_JOURNEY_TESTS_MODULE_DECLARATION = (
    f'#[cfg(all(test, feature = "{FEATURE}"))]\n'
    '#[path = "claude_host_journey_tests.rs"]\n'
    "mod claude_host_journey_tests;"
)
NATIVE_ADAPTER_CONSTRAINTS = {
    NATIVE_PROVIDER_FILE: (
        (NATIVE_PROVIDER_MODULE_FILE, NATIVE_PROVIDER_MODULE_DECLARATION),
    ),
    NATIVE_PROVIDER_TESTS_FILE: (
        (NATIVE_PROVIDER_MODULE_FILE, NATIVE_PROVIDER_MODULE_DECLARATION),
        (NATIVE_PROVIDER_FILE, NATIVE_PROVIDER_TESTS_MODULE_DECLARATION),
    ),
    NATIVE_PROVIDER_PARITY_TESTS_FILE: (
        (
            NATIVE_PROVIDER_MODULE_FILE,
            NATIVE_PROVIDER_PARITY_TESTS_MODULE_DECLARATION,
        ),
    ),
    NATIVE_BASELINE_TESTS_FILE: (
        (NATIVE_PROVIDER_MODULE_FILE, NATIVE_PROVIDER_MODULE_DECLARATION),
        (NATIVE_PROVIDER_FILE, NATIVE_BASELINE_TESTS_MODULE_DECLARATION),
    ),
    NATIVE_STAGED_OBSERVATIONS_FILE: (
        (
            NATIVE_PROVIDER_MODULE_FILE,
            NATIVE_STAGED_OBSERVATIONS_MODULE_DECLARATION,
        ),
    ),
    CLAUDE_HOST_JOURNEY_TESTS_FILE: (
        (
            CLAUDE_HOST_JOURNEY_TESTS_MODULE_FILE,
            CLAUDE_HOST_JOURNEY_TESTS_MODULE_DECLARATION,
        ),
    ),
}
# The two exact production consumers of the provider boundary: the mounted
# observation journey and the mounted cognitive-recall route.  They may
# *consume* an already-composed registry; they may not compose or enable one.
# This is an exact-path allowlist on purpose -- never widen it to a glob.
COGNITIVE_RECALL_MOUNT = Path(
    "crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs"
)
OBSERVATION_JOURNEY_MOUNT = Path(
    "crates/tracedecay/src/daemon/retained_owner/observation_journey.rs"
)
PROVIDER_BOUNDARY_MOUNTS = (COGNITIVE_RECALL_MOUNT, OBSERVATION_JOURNEY_MOUNT)
PROVIDER_BOUNDARY_MODULE_DECLARATIONS = {
    COGNITIVE_RECALL_MOUNT: (
        f'#[cfg(feature = "{FEATURE}")]\npub(crate) mod cognitive_recall;'
    ),
    OBSERVATION_JOURNEY_MOUNT: (
        f'#[cfg(feature = "{FEATURE}")]\npub(crate) mod observation_journey;'
    ),
}
# Each mounted consumer *opens* with its registry refusal.  Requiring the
# refusal to be the mount body's first statement -- not merely present, and
# not merely earlier in the file -- is what makes it dominate every side
# effect below it.  The side-effect list is checked separately so a reordering
# fails loudly even if the wording of this fragment ever changes.
PROVIDER_BOUNDARY_MOUNT_ENTRIES = {
    COGNITIVE_RECALL_MOUNT: "pub(crate) fn mount_project_cognitive_recall(",
    OBSERVATION_JOURNEY_MOUNT: "pub(crate) fn mount_project_observation_journey(",
}
PROVIDER_BOUNDARY_DISABLED_REFUSALS = {
    COGNITIVE_RECALL_MOUNT: (
        "inputs\n"
        "        .composition\n"
        "        .registry()\n"
        "        .ok_or(CognitiveRecallMountError::CompositionDisabled)?;"
    ),
    OBSERVATION_JOURNEY_MOUNT: (
        "inputs\n"
        "        .composition\n"
        "        .registry()\n"
        "        .ok_or(ObservationJourneyError::CompositionDisabled)?;"
    ),
}
# Anything in a mount body that touches durable storage, replays, or starts
# background work.  Every one of these must come *after* the refusal, or a
# disabled composition would already have done work before being refused.
PROVIDER_BOUNDARY_SIDE_EFFECTS = (
    "::open(",
    ".open(",
    "spawn",
    "::create",
    "create_dir",
    "replay",
    "Connection::",
    "::connect(",
)
# Composing or enabling a provider stays the composition root's job alone.
PROVIDER_BOUNDARY_FORBIDDEN = (
    "ProjectMemoryProviderComposition::compose",
    "NativeProviderActivation::Enabled",
    "NcmProviderAdapter",
)
# The durable journal and the hygiene pipeline are host-support crates; only
# the observation journey mount may name them.
SUPPORT_CRATE_OWNERS = {
    OBSERVATION_CRATE_IDENT: OBSERVATION_JOURNEY_MOUNT,
    HYGIENE_CRATE_IDENT: OBSERVATION_JOURNEY_MOUNT,
}
ROOT_SOURCE = Path("crates/tracedecay/src")
# The concrete Native adapter type; a word boundary keeps the registry's
# NativeProviderActivation seam from matching.
CONCRETE_NATIVE_ADAPTER = re.compile(r"\bNativeProvider\b")

# ---------------------------------------------------------------------------
# Provider-identity branching
#
# A provider identity reaches these files three ways: the registry's exported
# constants, a field or accessor whose name says it holds one, and a literal
# spelling of one.  Comparing, matching or string-testing *any* of those is
# provider-name dispatch and belongs in the registry/adapter layer.  Passing
# one along, or constructing the single registered identity from the
# registry's own constant, is not a branch.
# ---------------------------------------------------------------------------
PROVIDER_IDENTITY_TOKEN = re.compile(
    r"\b(?:[A-Z][A-Z0-9_]*_PROVIDER_ID|provider_id|providers_id|active_provider"
    r"|target_provider|provider_name|provider_ids)\b"
)
# A literal that *spells* a provider identity: one bare token, no spaces.
# Prose that merely contains the word "native" (an error message) is not one.
PROVIDER_NAME_LITERAL = re.compile(r'^"[A-Za-z0-9_.:\-]+"$')
PROVIDER_NAME_LITERAL_MARK = re.compile(r"native|ncm", re.IGNORECASE)
BARE_PROVIDER_IDENT = re.compile(r"\bproviders?\b")
STRING_METHOD_DISPATCH = re.compile(
    r"\.\s*(?:starts_with|ends_with|contains|eq|eq_ignore_ascii_case|matches)\s*\("
)
COMPARISON = re.compile(r"==|!=")
MATCH_KEYWORD = re.compile(r"\bmatch\b")
MATCHES_MACRO = re.compile(r"\bmatches!\s*\(")
STATEMENT_BOUNDARY = ";{}"

# Exact composition fragments that prove the *gating* of the host mount.
# These are checked against the whole file, because their whole point is the
# `#[cfg(...)]` attribute that removes them from a production build.
#
# The two `Pinned` fragments this tuple used to carry are gone on purpose.
# The `ProjectMemoryProviderActivationSelector::Pinned` variant, its resolve
# arm and its only construction (the `open_with_native_provider_for_test`
# harness seam) no longer exist: `production_harness.rs` is upstream-owned and
# its convergence-map entry (`shutdown_deadline` /
# `production_harness_shutdown`) authorizes only the shared shutdown deadline,
# so the seam was unauthorized and was removed. A gating requirement can only
# be stated about a construct that exists; requiring these fragments would
# force the unauthorized seam back into an upstream file. The pinned spellings
# stay *forbidden* below (see `check_composition_mount` and the root-source
# scan), which is the stronger invariant the removal leaves behind.
COMPOSITION_GATING_FRAGMENTS = (
    f'#[cfg(feature = "{FEATURE}")]\nasync fn mount_project_memory_provider_host(',
)
# The four-way resolution table, checked inside the resolver body: disabled
# stays disabled, a routing gate while disabled is a hard error, host-on alone
# is Observer only, and Active needs the separately named route.
# ...and the *complete, ordered* arm list of that table.  Requiring the exact
# sequence is what an "every required arm is present" check cannot do: an
# inserted `(false, Some(_)) => Ok(Disabled)` in front of the hard error would
# leave every required arm present while silently downgrading a routed
# provider to dormant.  An added, removed or reordered arm shifts this list.
RESOLVER_TABLE_SCRUTINEE = "match ("
RESOLVER_ARM_PATTERNS = [
    "(false, None)",
    "(false, Some(provider))",
    "(true, None)",
    "(true, Some(provider)) if is_mountable_active_provider(provider)",
    "(true, Some(provider))",
]
RESOLVER_TABLE_ARMS = (
    "(false, None) => Ok(ProjectMemoryProviderActivation::Disabled),",
    "(false, Some(provider)) => Err(TraceDecayError::Config {",
    "(true, None) => Ok(ProjectMemoryProviderActivation::NativeObserver),",
    "(true, Some(provider)) if is_mountable_active_provider(provider) => {",
    "Ok(ProjectMemoryProviderActivation::NativeActive)",
    "(true, Some(provider)) => Err(TraceDecayError::Config {",
)
ENABLED_ACTIVATION = f"{REGISTRY_CRATE_IDENT}::NativeProviderActivation::Enabled"
ENABLED_ARM = "Some(mode) => {"
RESOLVED_ACTIVATION_BINDING = (
    "let memory_provider_activation = activation.resolve(&runtime_configuration)?;"
)
ACTIVATION_VARIABLE = "memory_provider_activation"
TEST_NATIVE_HARNESS_ENTRY = re.compile(
    r'#\[cfg\(any\(test, feature = "test-transport"\)\)\]\s*'
    r'#\[doc\(hidden\)\]\s*pub async fn open_with_native_provider_for_test\('
)
# Runtime configuration is dormant by default.
CONFIG_REQUIRED_FRAGMENTS = (
    "    #[serde(default)]\n    pub memory_provider_native_enabled: bool,",
    "    #[serde(default)]\n"
    "    pub memory_provider_recall_routing: MemoryProviderRecallRoutingV1,",
    "            memory_provider_native_enabled: false,",
    "            memory_provider_recall_routing: MemoryProviderRecallRoutingV1::default(),",
)
# The routing gate defaults to no active provider, so enabling the host alone
# can never promote a provider to active output.
ROUTING_GATE_REQUIRED_FRAGMENT = (
    "pub struct MemoryProviderRecallRoutingV1 {\n"
    "    #[serde(default)]\n"
    "    pub active_provider: Option<String>,"
)


def read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            value = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"cannot read TOML {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"TOML root must be a table: {path}")
    return value


# ---------------------------------------------------------------------------
# Manifest: the host feature is optional and outside the default closure
# ---------------------------------------------------------------------------


def feature_closure(features: dict[str, Any], root: str) -> set[str]:
    """Every feature and `dep:`/`pkg/feat` edge `root` transitively enables."""

    reached: set[str] = set()
    pending = [root]
    while pending:
        name = pending.pop()
        if name in reached:
            continue
        reached.add(name)
        entries = features.get(name)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, str):
                continue
            if entry.startswith("dep:") or "/" in entry:
                reached.add(entry)
                continue
            pending.append(entry)
    return reached


def check_manifest(manifest: dict[str, Any], errors: list[str]) -> None:
    features = manifest["features"]
    dependencies = manifest["dependencies"]

    feature_values = features.get(FEATURE)
    if feature_values != HOST_FEATURE_EDGES:
        errors.append(
            f"feature {FEATURE} must contain exactly {HOST_FEATURE_EDGES}, "
            f"found {feature_values!r}"
        )
    if features.get("default") != ["production"]:
        errors.append(
            "default features must remain exactly ['production'], "
            f"found {features.get('default')!r}"
        )
    if not isinstance(features.get("production"), list):
        errors.append("feature production must be an array")

    # The whole point of the host feature is that a shipped build does not
    # have it.  Checking only the literal contents of `production` would miss
    # `default -> production -> ... -> memory-provider-host`, so the closure is
    # computed from every root a plain `cargo build` selects.
    for root in ("default", "production"):
        if not isinstance(features.get(root), list):
            continue
        closure = feature_closure(features, root)
        if FEATURE in closure:
            errors.append(
                f"feature {FEATURE} must stay outside the {root} feature closure; "
                "the provider host is opt-in and a default build must compile "
                "no provider-registry, journal or hygiene code"
            )
        for package in HOST_SUPPORT_DEPENDENCIES:
            if f"dep:{package}" in closure or package in closure:
                errors.append(
                    f"host-support dependency {package} must stay outside the "
                    f"{root} feature closure; it may be reached only by "
                    f"explicitly selecting {FEATURE}"
                )

    # No feature other than the host feature may pull a support dependency in,
    # so the closure result above cannot be routed around by a second door.
    for name, entries in features.items():
        if name == FEATURE or not isinstance(entries, list):
            continue
        for package in HOST_SUPPORT_DEPENDENCIES:
            if f"dep:{package}" in entries or package in entries:
                errors.append(
                    f"feature {name} must reach {package} only through {FEATURE}"
                )

    for package, expected_path in HOST_SUPPORT_DEPENDENCIES.items():
        dependency = dependencies.get(package)
        if not isinstance(dependency, dict):
            errors.append(f"optional dependency {package} is missing")
            continue
        if dependency.get("optional") is not True:
            errors.append(f"dependency {package} must be optional")
        if dependency.get("path") != expected_path:
            errors.append(f"dependency {package} path must be {expected_path}")
        forbidden_keys = sorted(set(dependency) & {"default-features", "features"})
        if forbidden_keys:
            errors.append(
                f"dependency {package} must not silently enable features: {forbidden_keys}"
            )


def check_activation_defaults(repo: Path, errors: list[str]) -> None:
    """Default configuration keeps a feature-on build dormant."""

    config_path = repo / CONFIG_SOURCE
    try:
        config = config_path.read_text(encoding="utf-8")
    except OSError as error:
        errors.append(f"cannot read runtime configuration {CONFIG_SOURCE}: {error}")
    else:
        for fragment in CONFIG_REQUIRED_FRAGMENTS:
            if fragment not in config:
                errors.append(
                    "runtime configuration must default the provider host to "
                    f"dormant; {CONFIG_SOURCE} is missing exact fragment: {fragment}"
                )
    routing_path = repo / ROUTING_GATE_SOURCE
    try:
        routing = routing_path.read_text(encoding="utf-8")
    except OSError as error:
        errors.append(f"cannot read routing gate {ROUTING_GATE_SOURCE}: {error}")
        return
    if ROUTING_GATE_REQUIRED_FRAGMENT not in routing:
        errors.append(
            "recall routing gate must default to no active provider; "
            f"{ROUTING_GATE_SOURCE} is missing exact fragment: "
            f"{ROUTING_GATE_REQUIRED_FRAGMENT}"
        )


# ---------------------------------------------------------------------------
# Provider-identity branching
# ---------------------------------------------------------------------------


def _statement_slice(mask: str, text: str, index: int) -> str:
    start = index
    while start > 0 and mask[start - 1] not in STATEMENT_BOUNDARY:
        start -= 1
    end = index
    while end < len(mask) and mask[end] not in STATEMENT_BOUNDARY:
        end += 1
    return text[start:end]


def _names_provider_identity(fragment: str) -> bool:
    if PROVIDER_IDENTITY_TOKEN.search(fragment):
        return True
    if not BARE_PROVIDER_IDENT.search(fragment):
        return False
    # `provider` plus a spelled-out name is name-based dispatch whatever the
    # provider is called, which is what catches an identity this gate has
    # never heard of.
    return bool(re.search(r'"[^"\n]*"', fragment))


def _paren_after(mask: str, start: int) -> tuple[int, int]:
    open_index = mask.find("(", start)
    if open_index == -1:
        raise RustParseError("expected a parenthesised group")
    depth = 0
    for index in range(open_index, len(mask)):
        if mask[index] == "(":
            depth += 1
        elif mask[index] == ")":
            depth -= 1
            if depth == 0:
                return open_index, index
    raise RustParseError("unbalanced parentheses")


def check_provider_name_branching(
    label: str, relative: Path, production: str, errors: list[str]
) -> None:
    """Refuse every provider-identity comparison, match and string test."""

    mask = code_mask(production)

    def fail(reason: str, fragment: str) -> None:
        errors.append(
            f"{label} must not branch on a provider identity ({reason}); "
            "provider-name dispatch belongs to the registry/adapter layer: "
            f"{relative}: {' '.join(fragment.split())[:160]}"
        )

    for match in COMPARISON.finditer(mask):
        fragment = _statement_slice(mask, production, match.start())
        if _names_provider_identity(fragment):
            fail("comparison", fragment)

    for match in MATCHES_MACRO.finditer(mask):
        try:
            open_index, close_index = _paren_after(mask, match.end() - 1)
        except RustParseError as error:
            errors.append(f"{label} {relative} cannot be parsed: {error}")
            continue
        fragment = production[open_index : close_index + 1]
        if _names_provider_identity(fragment):
            fail("matches! macro", fragment)

    for match in MATCH_KEYWORD.finditer(mask):
        if MATCHES_MACRO.match(mask, match.start()):
            continue
        brace = mask.find("{", match.end())
        if brace == -1:
            continue
        scrutinee = production[match.end() : brace]
        if not (
            _names_provider_identity(scrutinee)
            or BARE_PROVIDER_IDENT.search(scrutinee)
        ):
            continue
        try:
            open_index, close_index = block_after(mask, match.end())
        except RustParseError as error:
            errors.append(f"{label} {relative} cannot be parsed: {error}")
            continue
        arms = production[open_index : close_index + 1]
        # Matching on presence (`None` / `Some(provider)`) is not name
        # dispatch; matching a provider against a spelled-out identity, or
        # against the registry's identity constant, is.
        named = False
        for offset, literal in string_literals(arms):
            if re.match(r'^"[^"\n]*"\s*(?:\||=>|if\b)', arms[offset:]):
                fail("match arm pattern", f"match {scrutinee.strip()} {{ {literal} =>")
                named = True
                break
        if not named:
            constant = re.search(r"[A-Z][A-Z0-9_]*_PROVIDER_ID\s*(?:\||=>|if\b)", arms)
            if constant is not None:
                fail(
                    "match arm pattern",
                    f"match {scrutinee.strip()} {{ {constant.group()}",
                )

    for match in STRING_METHOD_DISPATCH.finditer(mask):
        fragment = _statement_slice(mask, production, match.start())
        if _names_provider_identity(fragment):
            fail("string test", fragment)

    for _offset, literal in string_literals(production):
        if PROVIDER_NAME_LITERAL.match(literal) and PROVIDER_NAME_LITERAL_MARK.search(
            literal
        ):
            fail("spelled-out provider identity", literal)


# ---------------------------------------------------------------------------
# Composition mount
# ---------------------------------------------------------------------------


def check_composition_mount(text: str, errors: list[str]) -> None:
    for fragment in COMPOSITION_GATING_FRAGMENTS:
        if fragment not in text:
            errors.append(
                f"composition mount is missing exact gating fragment: {fragment}"
            )

    try:
        production = strip_cfg_test_modules(text)
    except RustParseError as error:
        errors.append(f"composition mount cannot be parsed structurally: {error}")
        return
    mask = code_mask(production)

    def body(marker: str) -> str | None:
        try:
            start, end = body_of(production, mask, marker)
        except RustParseError as error:
            errors.append(
                f"composition mount must expose exactly one {marker!r}: {error}"
            )
            return None
        return production[start:end]

    # 1. The production entry can express only one selector.
    entry_body = body("pub(super) async fn production_project_server(")
    if entry_body is not None:
        if (
            "ProjectMemoryProviderActivationSelector::FromRuntimeConfiguration,"
            not in entry_body
        ):
            errors.append(
                "production_project_server must pass "
                "ProjectMemoryProviderActivationSelector::FromRuntimeConfiguration"
            )
        if "Pinned" in entry_body:
            errors.append(
                "production_project_server must not pin an activation; only the "
                "test-gated harness may pass a pinned selector"
            )
        if "production_project_server_with_activation(" not in entry_body:
            errors.append(
                "production_project_server must delegate to "
                "production_project_server_with_activation"
            )

    forwarder_body = body(
        "pub(super) async fn production_project_server_with_activation("
    )
    if forwarder_body is not None:
        if "ProjectMemoryProviderActivationSelector::" in forwarder_body:
            errors.append(
                "production_project_server_with_activation must forward the "
                "caller's selector unchanged, never name a selector variant"
            )

    # 2. The resolved activation is bound once and never rebound.
    inner_body = body("async fn production_project_server_inner(")
    if inner_body is not None:
        inner_mask = code_mask(inner_body)
        if inner_body.count(RESOLVED_ACTIVATION_BINDING) != 1:
            errors.append(
                "production_project_server_inner must resolve the activation "
                "exactly once, with the exact fragment: "
                f"{RESOLVED_ACTIVATION_BINDING}"
            )
        bindings = re.findall(
            rf"\blet\s+(?:mut\s+)?{ACTIVATION_VARIABLE}\b", inner_mask
        )
        if len(bindings) != 1 or "mut" in " ".join(bindings):
            errors.append(
                f"{ACTIVATION_VARIABLE} must be bound exactly once and immutably; "
                "a second binding shadows the configuration-resolved activation"
            )
        assignments = re.findall(rf"\b{ACTIVATION_VARIABLE}\s*=(?!=)", inner_mask)
        if len(assignments) != 1:
            errors.append(
                f"{ACTIVATION_VARIABLE} must never be reassigned after it is "
                "resolved from the runtime configuration"
            )
        if re.search(r"\bProjectMemoryProviderActivation::", inner_mask):
            errors.append(
                "production_project_server_inner must not construct a "
                "ProjectMemoryProviderActivation; it may only pass along the "
                "activation the selector resolved"
            )
        if (
            f"mount_project_memory_provider_host(\n        {ACTIVATION_VARIABLE},"
            not in inner_body
        ):
            errors.append(
                "the composition must mount the configuration-resolved "
                f"activation: mount_project_memory_provider_host({ACTIVATION_VARIABLE}, ...)"
            )
        for fragment in (
            "memory_provider_host_mount.registry().is_some(),",
            "if memory_provider_host_mount.registry().is_some() {",
        ):
            if fragment not in inner_body:
                errors.append(
                    "both mounted consumers must be reached only from an enabled "
                    f"composition; missing exact fragment: {fragment}"
                )

    # 3. The selector resolves through configuration and constructs nothing.
    selector_body = body("fn resolve(\n        self,")
    if selector_body is not None:
        for fragment in (
            "Self::FromRuntimeConfiguration => {",
            "resolve_memory_provider_activation(&runtime_configuration.config)",
        ):
            if fragment not in selector_body:
                errors.append(
                    "the activation selector must resolve from runtime "
                    f"configuration; missing exact fragment: {fragment}"
                )
        if "ProjectMemoryProviderActivation::" in code_mask(selector_body):
            errors.append(
                "the activation selector must not construct an activation; the "
                "resolution table is the only place activations are decided"
            )

    # 4. The resolution table stays explicit and four-way.
    try:
        resolver_start, resolver_end = body_of(
            production, mask, "fn resolve_memory_provider_activation("
        )
    except RustParseError as error:
        errors.append(
            "composition mount must expose exactly one "
            f"resolve_memory_provider_activation: {error}"
        )
    else:
        resolver_body = production[resolver_start:resolver_end]
        for arm in RESOLVER_TABLE_ARMS:
            if resolver_body.count(arm) != 1:
                errors.append(
                    "the activation resolution table must keep its explicit "
                    f"shape; missing or duplicated exact arm: {arm}"
                )
        table = mask.find(RESOLVER_TABLE_SCRUTINEE, resolver_start)
        if table == -1 or table >= resolver_end:
            errors.append(
                "the activation resolution table must decide from one explicit "
                "`match (host gate, routing gate)` over the runtime configuration"
            )
        else:
            try:
                open_index, close_index = block_after(mask, table)
                patterns = match_arm_patterns(mask, production, open_index, close_index)
            except RustParseError as error:
                errors.append(f"composition mount cannot be parsed: {error}")
            else:
                if patterns != RESOLVER_ARM_PATTERNS:
                    errors.append(
                        "the activation resolution table must be exactly, and in "
                        f"this order, {RESOLVER_ARM_PATTERNS}; found {patterns}. An "
                        "extra or reordered arm can shadow the refusal that keeps "
                        "a routed provider from being inferred while the host is off"
                    )

    # 5. The enabled activation exists exactly once, inside the resolved arm.
    enabled_sites = find_all(mask, ENABLED_ACTIVATION)
    try:
        mount_start, mount_end = body_of(
            production, mask, "fn mount_project_memory_provider_host("
        )
    except RustParseError as error:
        errors.append(
            "composition mount must expose exactly one "
            f"mount_project_memory_provider_host: {error}"
        )
        if enabled_sites:
            errors.append(
                "the enabled Native activation may only be constructed inside "
                "mount_project_memory_provider_host"
            )
    else:
        mount_body = production[mount_start:mount_end]
        for fragment in (
            "ProjectMemoryProviderActivation::Disabled => None,",
            f"None => {REGISTRY_CRATE_IDENT}::NativeProviderActivation::Disabled,",
            f"{REGISTRY_CRATE_IDENT}::ProjectMemoryProviderComposition::compose(activation)",
        ):
            if mount_body.count(fragment) != 1:
                errors.append(
                    "mount_project_memory_provider_host must keep its dormant "
                    f"path; missing or duplicated exact fragment: {fragment}"
                )
        arm_sites = find_all(mask[mount_start:mount_end], ENABLED_ARM)
        if len(arm_sites) != 1:
            errors.append(
                "mount_project_memory_provider_host must have exactly one "
                f"resolved `{ENABLED_ARM}` arm, found {len(arm_sites)}"
            )
        else:
            try:
                arm_open, arm_close = block_after(
                    mask, mount_start + arm_sites[0] + len(ENABLED_ARM) - 1
                )
            except RustParseError as error:
                errors.append(f"composition mount cannot be parsed: {error}")
            else:
                inside = [site for site in enabled_sites if arm_open < site < arm_close]
                if len(enabled_sites) != 1 or len(inside) != 1:
                    errors.append(
                        "the enabled Native activation must be constructed exactly "
                        "once in production, and only inside the resolved "
                        f"`{ENABLED_ARM}` arm of mount_project_memory_provider_host "
                        f"(found {len(enabled_sites)} construction(s), "
                        f"{len(inside)} of them inside the arm)"
                    )

    if "ProjectMemoryProviderActivationSelector::Pinned" in mask:
        errors.append(
            "composition mount must not pin an activation selector; production may "
            "only pass ProjectMemoryProviderActivationSelector::FromRuntimeConfiguration"
        )
    for forbidden in ("tracedecay_memory_provider_native", "NcmProviderAdapter"):
        if forbidden in text:
            errors.append(
                f"composition mount must delegate through the registry, not name {forbidden}"
            )
    if CONCRETE_NATIVE_ADAPTER.search(text):
        errors.append(
            "composition mount must delegate through the registry, not name NativeProvider"
        )
    check_provider_name_branching(
        "composition mount", COMPOSITION_MOUNT, production, errors
    )


# ---------------------------------------------------------------------------
# Provider-boundary mounts
# ---------------------------------------------------------------------------


def gated_allowlist(
    repo: Path,
    files: tuple[Path, ...],
    constraints: dict[Path, tuple[tuple[Path, str], ...]],
    label: str,
    errors: list[str],
) -> set[Path]:
    """Return present allowlisted files whose module declarations are gated."""

    gated: set[Path] = set()
    for allowlisted in files:
        if not (repo / allowlisted).is_file():
            continue
        valid = True
        for source_relative, required_declaration in constraints[allowlisted]:
            source_path = repo / source_relative
            try:
                source = source_path.read_text(encoding="utf-8")
            except OSError as error:
                errors.append(
                    f"{label} {allowlisted} cannot verify its "
                    f"feature gate in {source_relative}: {error}"
                )
                valid = False
                continue
            if required_declaration not in source:
                errors.append(
                    f"{label} {allowlisted} must be feature-gated; "
                    f"{source_relative} is missing exact module declaration: "
                    f"{required_declaration}"
                )
                valid = False
        if valid:
            gated.add(allowlisted)
    return gated


def feature_gated_native_adapter_files(repo: Path, errors: list[str]) -> set[Path]:
    """Return present adapter files whose module declarations are feature-gated."""

    return gated_allowlist(
        repo,
        NATIVE_ADAPTER_FILES,
        NATIVE_ADAPTER_CONSTRAINTS,
        "native adapter file",
        errors,
    )


def feature_gated_provider_boundary_mounts(
    repo: Path, errors: list[str]
) -> set[Path]:
    """Return the mounted provider-boundary files whose modules are gated.

    With `memory-provider-host` off neither mount compiles at all, which is
    what keeps a feature-off build identical to upstream.
    """

    constraints = {
        mount: ((NATIVE_PROVIDER_MODULE_FILE, declaration),)
        for mount, declaration in PROVIDER_BOUNDARY_MODULE_DECLARATIONS.items()
    }
    return gated_allowlist(
        repo,
        PROVIDER_BOUNDARY_MOUNTS,
        constraints,
        "provider-boundary mount",
        errors,
    )


def check_provider_boundary_mount(
    relative: Path, text: str, errors: list[str]
) -> str:
    """Verify one mounted consumer; return the parsed production region.

    The returned region is what the repository sweep scans for this file, so
    an enabled composition built as a `#[cfg(test)]` fixture is not read as
    production activation -- and, because the region is cut by balanced
    braces, an indented production item placed after that fixture module is
    still scanned.
    """

    try:
        production = strip_cfg_test_modules(text)
    except RustParseError as error:
        errors.append(
            f"provider-boundary mount cannot be parsed structurally: {relative}: {error}"
        )
        return ""
    mask = code_mask(production)

    entry = PROVIDER_BOUNDARY_MOUNT_ENTRIES[relative]
    refusal = PROVIDER_BOUNDARY_DISABLED_REFUSALS[relative]
    try:
        start, end = body_of(production, mask, entry)
    except RustParseError as error:
        errors.append(
            "provider-boundary mount must expose exactly one production mount "
            f"entry {entry!r}: {relative}: {error}"
        )
    else:
        mount_body = production[start:end]
        if not mount_body.lstrip().startswith(refusal):
            errors.append(
                "provider-boundary mount must open with its registry refusal so a "
                "disabled composition is refused before any other work: "
                f"{relative} is missing the exact opening statement: {refusal}"
            )
        else:
            refusal_end = mount_body.index(refusal) + len(refusal)
            body_mask = code_mask(mount_body)
            for token in PROVIDER_BOUNDARY_SIDE_EFFECTS:
                site = body_mask.find(token)
                if site != -1 and site < refusal_end:
                    errors.append(
                        "provider-boundary mount must refuse a disabled composition "
                        "before opening storage, replaying, or spawning work: "
                        f"{relative} reaches {token!r} first"
                    )

    for forbidden in PROVIDER_BOUNDARY_FORBIDDEN:
        if forbidden in production:
            errors.append(
                "provider-boundary mount may consume the registry but must not "
                f"name {forbidden}: {relative}"
            )
    if CONCRETE_NATIVE_ADAPTER.search(production):
        errors.append(
            f"provider-boundary mount must not name the concrete Native adapter: {relative}"
        )
    check_provider_name_branching(
        "provider-boundary mount", relative, production, errors
    )
    return production


# ---------------------------------------------------------------------------
# Repository sweep
# ---------------------------------------------------------------------------


def check_repository(repo: Path) -> list[str]:
    errors: list[str] = []
    manifest_path = repo / ROOT_MANIFEST
    mount_path = repo / COMPOSITION_MOUNT
    manifest = read_toml(manifest_path)
    if not isinstance(manifest.get("features"), dict):
        return ["root manifest [features] table is missing"]
    if not isinstance(manifest.get("dependencies"), dict):
        return ["root manifest [dependencies] table is missing"]
    check_manifest(manifest, errors)
    check_activation_defaults(repo, errors)

    try:
        mount = mount_path.read_text(encoding="utf-8")
    except OSError as error:
        return errors + [f"cannot read composition mount {mount_path}: {error}"]
    check_composition_mount(mount, errors)

    feature_gated_adapters = feature_gated_native_adapter_files(repo, errors)
    boundary_mounts = feature_gated_provider_boundary_mounts(repo, errors)

    source_root = repo / ROOT_SOURCE
    if source_root.is_dir():
        for path in sorted(source_root.rglob("*.rs")):
            relative = path.relative_to(repo)
            if relative == COMPOSITION_MOUNT:
                continue
            text = path.read_text(encoding="utf-8")
            is_feature_gated_adapter = relative in feature_gated_adapters
            is_boundary_mount = relative in boundary_mounts
            if is_boundary_mount:
                # Everything below scans this file's production region only;
                # the mount's own `#[cfg(test)]` fixtures legitimately build an
                # enabled composition to exercise the refusal.
                text = check_provider_boundary_mount(relative, text, errors)
            if relative in RETENTION_MOUNTS:
                if "::compose(" in text:
                    errors.append(
                        f"retention mount must not compose providers: {relative}"
                    )
            elif (
                not is_feature_gated_adapter
                and not is_boundary_mount
                and REGISTRY_CRATE_IDENT in text
            ):
                errors.append(
                    f"registry dependency leaked outside the composition mount: {relative}"
                )
            for support_ident, owner in SUPPORT_CRATE_OWNERS.items():
                if support_ident in text and relative != owner:
                    errors.append(
                        f"host-support crate {support_ident} leaked outside "
                        f"{owner}: {relative}"
                    )
            if "NativeProviderActivation::Enabled" in text:
                errors.append(
                    f"production sources must keep the provider host dormant: {relative}"
                )
            native_active_count = text.count(
                "ProjectMemoryProviderActivation::NativeActive"
            )
            if native_active_count:
                if (
                    relative != ACTIVATION_HARNESS
                    or native_active_count != 1
                    or TEST_NATIVE_HARNESS_ENTRY.search(text) is None
                ):
                    errors.append(
                        "test-only Native provider activation leaked outside its gated "
                        f"harness entry: {relative}"
                    )
            pinned_selector_count = text.count(
                "ProjectMemoryProviderActivationSelector::Pinned"
            )
            if pinned_selector_count:
                if (
                    relative != ACTIVATION_HARNESS
                    or pinned_selector_count != 1
                    or TEST_NATIVE_HARNESS_ENTRY.search(text) is None
                ):
                    errors.append(
                        "pinned activation selector leaked outside its gated harness "
                        f"entry: {relative}"
                    )
            if not is_feature_gated_adapter and (
                "tracedecay_memory_provider_native" in text
                or CONCRETE_NATIVE_ADAPTER.search(text)
            ):
                errors.append(
                    f"concrete Native adapter leaked into root source: {relative}"
                )
    return errors


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".", help="repository root")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    repo = Path(args.repo).resolve()
    try:
        errors = check_repository(repo)
    except ValueError as error:
        print(f"memory composition feature error: {error}", file=sys.stderr)
        return 2
    if errors:
        for error in errors:
            print(f"memory composition feature violation: {error}", file=sys.stderr)
        return 1
    print("memory composition feature verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
