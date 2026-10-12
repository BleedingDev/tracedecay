# Host delivery evidence

Audited revision: `571daf3a9612e5247443e4da3a107b542686c1ef` in the assigned
worktree. The Native source reference is `b3b43410e47115056f2066449aafa1822bbb6049`,
the upstream-authored second parent of this head. The upstream Native
`tracedecay-session-memory/src/fact_store/**` and `src/memory/**` trees are the
canonical authority; the host-selection and delivery code below is product-side
integration. Where this report says that an original host behavior is unknown,
that is deliberate: this audit did not find evidence that the original Native
implementation automatically recalled ordinary chat.

## Short answer

The current product has one shared delivery path:

```text
project configuration snapshot
  -> MemoryProviderSelectionV1::resolve
  -> project composition mounts Native and/or NCM
  -> selected active registration and observer journeys
  -> exact tracedecay_context handler
  -> canonical Native memory_matches + ContextMemoryContributionV1 sidecar
  -> selected-provider advisory recall (only for tracedecay_context)
  -> bounded, provenance-checked context pack
  -> original MCP result plus one advisory member/section
```

Configuration defaults keep Native disabled, NCM disabled, and recall routing
without an active provider. Enabling the provider-host feature compiles the
mount, but it does not promote a provider by itself. An enabled unselected
provider is an observer. A selected active provider is the only registration
used by the advisory recall route; the project composition intentionally keeps
configured observer journeys, including an NCM observer, alive.

Canonical Native facts are already assembled by `tracedecay_context` before the
advisory lane runs. The selected provider can then contribute history candidates
to the same model-visible result. This is the important duplicate boundary: the
sidecar carries canonical fact identities, temporal policy, and exclusions but
not fact text, and current tests prove uniqueness inside the provider lane. They
do not yet prove that a canonical `memory_matches` item cannot be repeated by a
provider candidate with the same fact identity or text. That cross-lane identity
check needs an explicit contract and test owner.

The host hooks are observation, lifecycle, and guidance surfaces. The memory
injection gate reads `TRACEDECAY_MEMORY_INJECTION` or `UserConfig`, but its
module explicitly does not query facts/LCM or persist recall state. No current
source proves automatic provider recall on an ordinary Claude or Codex prompt.
The provider advisory lane is tool-return augmentation for the exact
`tracedecay_context` tool. Claude and Codex differ in lifecycle commands and
bundle rendering, while sharing the MCP result shape and
`hookSpecificOutput.additionalContext` hook envelope.

## Evidence

| Original revision/path/symbol | Current path/symbol | Original behavior | Difference or integration requirement |
| --- | --- | --- | --- |
| `b3b43410e47115056f2066449aafa1822bbb6049`: complete Native authority in [`tracedecay-session-memory/src/fact_store/**`](https://github.com/ScriptedAlchemy/tracedecay/tree/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/fact_store) and [`src/memory/**`](https://github.com/ScriptedAlchemy/tracedecay/tree/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/memory) | [`TraceDecayConfig::default`](../../../../crates/tracedecay-configuration/src/config/mod.rs#L60-L77), [`runtime_config_from_snapshot`](../../../../crates/tracedecay-configuration/src/config/mod.rs#L238-L278), and the provider keys in [`configuration.rs`](../../../../crates/tracedecay-domain/src/configuration.rs#L49-L60) | Native owns the original fact-store and memory use cases. No matching original host-selection path was established in this lane. | A provider adapter must call the owner-bound Native application and preserve its state changes, ranking, ownership, and failures. It must not treat the staged observation store, a wrapper, or an empty fallback as Native. Host selection is an outer integration policy and must remain explicit. |
| No original host-selection counterpart was found in the b3 Native trees. | [`register_project_settings`](../../../../crates/tracedecay-global-db/src/configuration/registry.rs#L530-L617) and its regression test ([`memory_provider_registration_tests`](../../../../crates/tracedecay-global-db/src/configuration/registry.rs#L1209-L1247)) | — | The three provider settings are project-scoped and require daemon restart. Defaults are `native_enabled=false`, NCM `{"mode":"disabled"}`, and recall routing with no active or fallback provider. Keep these defaults; do not make feature compilation or an environment variable silently activate a provider. |
| No original counterpart established for the product activation enum. | [`MemoryProviderSelectionV1::resolve`](../../../../crates/tracedecay-domain/src/configuration.rs#L1128-L1167), [`active_provider`](../../../../crates/tracedecay-domain/src/configuration.rs#L1170-L1183), and selection errors ([`configuration.rs`](../../../../crates/tracedecay-domain/src/configuration.rs#L1186-L1198)) | — | Disabled, observer, and active are distinct. Selecting a disabled provider errors; an enabled provider not named by routing remains an observer; no active provider is a valid state. Do not infer a selected provider from an empty answer or provider display name. |
| No original counterpart established for the current mount. | [`mount_project_memory_provider_host`](../../../../crates/tracedecay/src/daemon/project_composition.rs#L356-L501) | — | Composition loops Native and NCM independently. Active registrations and Native-without-an-active-provider are required; other observers are optional. Native constructs the owner-bound application port and registration. NCM constructs its configured worker registration. The result is `Injected` for one selected provider plus observers, or `ObserversOnly`. Preserve the NCM host-state authority when Native is selected or observed; do not invent an all-Native shutdown. |
| No original counterpart established for product project routing. | [`compose_core_server`](../../../../crates/tracedecay/src/daemon/project_composition.rs#L1286-L1374), [`project_recall_routing_policy`](../../../../crates/tracedecay/src/daemon/project_composition.rs#L227-L278), and recall mount admission ([`project_composition.rs`](../../../../crates/tracedecay/src/daemon/project_composition.rs#L1376-L1447)) | — | The authoritative code-index scope is obtained before the provider mount and is passed to the provider, history, and recall surfaces. Only a selected active registration gets a recall route. Disabled/observer-only compositions get no route. A configured fallback is refused because this project registry contains only the selected provider; no fake readiness or silent fallback is allowed. |
| b3 Native history/fact ownership remains the source of canonical project evidence. | Every configured provider receives an observation journey in [`resolved`](../../../../crates/tracedecay/src/daemon/project_composition.rs#L1881-L1976); selected history binds only when provider identity matches. | Original Native ownership must remain project/profile scoped. NCM has its own configured provider state and history authority. | Hook observations may be delivered to Native and NCM journeys independently. Selection controls advisory recall, not whether an already configured observer receives host state. Keep provider-local NCM state and its receipts; do not collapse all state into Native or stop NCM when Native becomes active. |
| Native canonical context remains the host-visible fact projection. | [`ContextMemoryOptions`](../../../../crates/tracedecay-mcp/src/handlers/graph/context_support.rs#L195-L247) and [`handle_context`](../../../../crates/tracedecay-mcp/src/handlers/graph/search.rs#L787-L1064) | — | `tracedecay_context` defaults to `include_memory=true`, a memory limit of 3 (clamped), minimum trust `.5`, and a current-only temporal policy. Memory and code/search run independently under the admitted deadline and cancellation signal. The handler renders `Memory Matches` and serializes `ContextResultV1.memory_matches`. These host policy filters must not be copied into or used to narrow the complete Native backend without source proof. |
| The sidecar preserves the canonical contribution without reparsing rendered text. | [`ContextMemoryContributionV1::from_matches`](../../../../crates/tracedecay-contracts/src/retrieval/primitive_surface.rs#L139-L190) and [`ContextResultV1`](../../../../crates/tracedecay-contracts/src/retrieval/primitive_surface.rs#L437-L463) | — | The sidecar retains owner, fact, assertion/event, and source identities plus graph/temporal coverage and exclusions. It intentionally carries no canonical fact text. A future duplicate policy must compare typed identities where available and state what happens when only text matches; it must not guess identity by parsing Markdown. |
| No original automatic host-recall path was established. | [`execute_tool_dispatch`](../../../../crates/tracedecay/src/mcp/server/requests/tool_dispatch.rs#L257-L313) and post-handler augmentation ([`tool_dispatch.rs`](../../../../crates/tracedecay/src/mcp/server/requests/tool_dispatch.rs#L409-L429)) | — | Only the exact `tracedecay_context` tool name creates an advisory call. The canonical handler completes first; the selected provider lane runs afterward. A non-context MCP tool, an ordinary host prompt, or a coalesced result does not receive provider candidates through this seam. |
| No original host-output augmentation counterpart was established. | [`advisory_context_call`](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L2136-L2160) and [`advisory_memory_context_for_call`](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L2162-L2291) | — | Dormant/observer-only composition is no lane. A live lane requires a bound session, deadline, and cancellation identity; the provider gets at most a two-second slice plus a 250 ms host grace. A timeout or refusal leaves the canonical host answer available and attributes the typed outcome to the pinned provider. |
| No original provider-history projection was established. | [`advisory_context_recall_with_retention`](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L2385-L2562) and provenance/hydration ([`cognitive_recall.rs`](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L2626-L2929)) | — | The request is built from the mount's exact scope and selected history authority. Provider `record:`, `source:`, and `session:` claims are host-confirmed in that scope. Unresolvable claims are excluded by default; provider text, explanations, identities, and provenance pass the untrusted-memory gate. This is output admission around the provider contract, not permission to replace Native's scorer or persistence semantics. |
| No original context pack was established. | [`AdvisoryMemoryContextV1::context_pack`](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L3881-L4058), pack budgets ([`cognitive_recall.rs`](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L3160-L3181)), and [`appended_to`](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L4183-L4269) | — | The host answer is required evidence and is budgeted with headings, attribution, candidate metadata, and explanations. Total budget is 128,000 tokens; provider quota is 8,192. JSON adds only the reserved advisory member ([`merge_compiled_advisory`](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L4290-L4332)); Markdown appends. A refused pack emits a typed withheld notice while preserving the host answer ([`withheld_rendering`](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L4334-L4394)). |
| No original hook gate behavior was established. | [`memory_injection_enabled`](../../../../crates/tracedecay-agent-hosts/src/hooks/memory_inject.rs#L1-L25) and shared hook dispatch/ingest ([`hooks/mod.rs`](../../../../crates/tracedecay-agent-hosts/src/hooks/mod.rs#L388-L406), [`hooks/mod.rs`](../../../../crates/tracedecay-agent-hosts/src/hooks/mod.rs#L450-L680)) | — | The environment override wins over `UserConfig`; the user config defaults the lifecycle injection flag to true. The gate does not query facts/LCM or persist recall/dedupe state. Hooks can notify/ingest observations and return guidance, but this code does not prove automatic provider recall on ordinary chat. |
| No original Claude/Codex bundle counterpart was established. | Claude lifecycle handlers ([`claude.rs`](../../../../crates/tracedecay-agent-hosts/src/hooks/claude.rs#L143-L221), [`claude.rs`](../../../../crates/tracedecay-agent-hosts/src/hooks/claude.rs#L224-L259), [`claude.rs`](../../../../crates/tracedecay-agent-hosts/src/hooks/claude.rs#L262-L392)) and Codex handlers ([`codex.rs`](../../../../crates/tracedecay-agent-hosts/src/hooks/codex.rs#L49-L89), [`codex.rs`](../../../../crates/tracedecay-agent-hosts/src/hooks/codex.rs#L223-L357), [`codex.rs`](../../../../crates/tracedecay-agent-hosts/src/hooks/codex.rs#L389-L432)) | — | Claude SessionStart routes/ingests, PostCompact is a read-only unavailable probe, PostToolUse provides guidance, and Stop ingests. Codex SessionStart routes, UserPromptSubmit resets/returns steering, PostToolUse provides guidance, and Stop enqueues retained work. Both share `hookSpecificOutput.additionalContext`; lifecycle hooks are not the provider recall result. |
| No original plugin renderer counterpart was established. | Claude raw manifest [`plugin/hooks/hooks-claude.json`](../../../../plugin/hooks/hooks-claude.json#L1-L71); Codex raw scaffold [`plugin/hooks/hooks-codex.json`](../../../../plugin/hooks/hooks-codex.json#L1-L3); shared MCP [`plugin/.mcp.json`](../../../../plugin/.mcp.json#L1-L13); Codex policy/renderer [`codex.rs`](../../../../crates/tracedecay-agent-hosts/src/agents/codex.rs#L740-L805), [`codex.rs`](../../../../crates/tracedecay-agent-hosts/src/agents/codex.rs#L886-L1005) | — | Claude's raw manifest declares lifecycle commands. Codex's raw hooks file is intentionally empty until global rendering; a global bundle has hooks, `serve`, and `TRACEDECAY_ENABLE_GLOBAL_DB=1`, while a repo-local bundle has no hooks, `serve --path .`, and no env. Staged source/marketplace files do not activate Codex; `[plugins."tracedecay@…"] enabled=true` is required ([`codex.rs`](../../../../crates/tracedecay-agent-hosts/src/agents/codex.rs#L2179-L2205)). Never treat the raw scaffold as an installed hook surface. |

## Native, NCM, and duplicate delivery boundaries

The canonical Native contribution and the selected-provider contribution are
different lanes in the same `tracedecay_context` result:

1. `handle_context` performs code/search and canonical project-memory work in
   parallel. Its `memory_matches` are Native fact-store evidence and are
   rendered under `Memory Matches` as well as retained in `ContextResultV1`.
2. `ContextMemoryContributionV1` carries only the identities and policies that
   the later provider request may need. It does not carry the canonical fact
   text, so the advisory lane cannot use the sidecar as a second text source.
3. After the handler returns, the exact `tracedecay_context` call is admitted to
   the selected active provider. The provider reads its selected history
   authority and returns advisory candidates. NCM active and Native active use
   the same route shape, but their provider IDs and history authorities differ.
4. Host admission confirms provenance, hardens provider-controlled text, and
   packs the provider lane after required host evidence. An observer has no
   recall route and cannot appear as selected output.

This means selecting NCM does not, by itself, remove canonical Native facts
from `memory_matches`; canonical facts are a separate authority path. Likewise,
enabling Native does not authorize shutting down NCM's configured host-state
authority. If product requirements later call for one model-visible copy across
the canonical and advisory lanes, the implementation needs a typed identity
decision. Current provider-lane tests assert unique provenance among provider
candidates, but the source inspected here has no complete cross-lane
canonical-ID/content assertion. Treat that as an integration gap, not as proof
that a duplicate already occurs in every response.

The other filtering layers are also distinct. Canonical context applies its
request memory limit, trust threshold, and temporal policy. Provider admission
applies exact scope, history grant, provenance hydration, untrusted-text
hardening, and the advisory token quota. None of these outer filters proves the
complete original Native ranking or operation semantics. A Native adapter must
delegate those semantics to the b3 authority and keep host delivery policy at
the boundary.

## Supported host inventory

`HostKindV1::ALL` contains 18 catalog entries. Sixteen have receipt-backed
first-party component lifecycles; CursorCloud and ClineFamily remain typed
unavailable. Only seven hosts have authentic native event fixtures, so an MCP
component can be installable without evidence that a native hook route exists.
The catalog and reasons are in [`integration.rs`](../../../../crates/tracedecay-domain/src/integration.rs#L32-L97),
the capability matrix in [`integration.rs`](../../../../crates/tracedecay-domain/src/integration.rs#L170-L302),
and lifecycle admission/default components in [`host_bundle_registry.rs`](../../../../crates/tracedecay-agent-hosts/src/agents/host_bundle_registry.rs#L22-L42)
and [`host_bundle_registry.rs`](../../../../crates/tracedecay-agent-hosts/src/agents/host_bundle_registry.rs#L107-L190).

| Hosts | Catalog/capability evidence | Default component and delivery implication |
| --- | --- | --- |
| ClaudeCode | Receipt-backed; LSP, hooks, MCP, CLI; native diagnostics unavailable; authentic fixture | Core + ContextMcp. Full shipped hook journey exists. |
| Codex | Receipt-backed; hooks, MCP, CLI; LSP/diagnostics unavailable; authentic fixture | Core + ContextMcp. Global hooks are rendered and require host trust/activation. |
| CursorDesktop | Receipt-backed; native diagnostics, hooks, MCP, CLI; LSP unavailable; authentic fixture | Core + Agent + ContextMcp. Shared MCP/context changes apply; lifecycle evidence is fixture-backed. |
| OpenCode | Receipt-backed; all five capabilities; authentic fixture | Core + Agent + ContextMcp. Shared context and native-event surfaces apply. |
| Hermes | Receipt-backed; hooks, MCP, CLI; LSP/diagnostics unavailable; authentic fixture | Core. Hook/event changes apply to its fixture-backed route. |
| KimiCode | Receipt-backed; hooks, MCP, CLI; LSP/diagnostics unavailable; authentic fixture | Core. Hook/event changes apply to its fixture-backed route. |
| Kiro | Receipt-backed; MCP/CLI; hooks degraded by `NativeFixtureLimited`; no LSP/diagnostics | ContextMcp. Keep its supported MCP route and degraded hook evidence separate. |
| Devin, Zed, Antigravity | Receipt-backed; MCP/CLI only; hook evidence missing | ContextMcp. Shared MCP/context policy applies; do not claim native hook capture. |
| Vibe | Receipt-backed; MCP/CLI only; hook evidence missing | ContextMcp + Core. Shared MCP applies; lifecycle capture remains unavailable. |
| Gemini | Receipt-backed; MCP/CLI; hooks unavailable because checked-in native evidence is missing | ContextMcp through its CLI-installed extension. |
| Copilot | Receipt-backed; MCP/CLI; hooks `HostApiAbsent` | ContextMcp through `copilot mcp add|remove`; do not invent hook support. |
| Cline | Receipt-backed; MCP/CLI; hook route `NativeFixtureLimited` | ContextMcp. The documented MCP lifecycle does not promote absent native events. |
| RooCode, Kilo | Receipt-backed; MCP/CLI; hooks unavailable by checked-in evidence | ContextMcp. Shared context applies; no native hook claim. |
| CursorCloud | Catalog entry only; component set unavailable for unsupported host registration | No installable component. Keep it out of lifecycle sweeps. |
| ClineFamily | Family alias only; component set unavailable for missing concrete authority | No installable component. Do not treat the family alias as Cline support. |

The authentic fixture list is explicit in
[`native_host_edit_stop_conformance_evidence_from_embedded_assets`](../../../../crates/tracedecay-host-integration/src/lib.rs#L557-L572):
Claude, Codex, Cursor Desktop, Hermes, Kiro, KimiCode, and OpenCode. The
absence of a fixture is not evidence that the host can never support a hook;
it is evidence that this revision must not claim that route.

## Exact future write ownership

The implementation should be split at the current seams:

| Owner | Files or narrow module | Responsibility and dependency |
| --- | --- | --- |
| Native adapter owner | `crates/tracedecay-memory-provider-native/**`; the narrow Native integration in `crates/tracedecay/src/daemon/retained_owner/native_provider.rs` and focused tests | Delegate complete Native operations to the owner-bound b3 application. Depends on the source lane's accepted floor and operation/effect map. Must not edit `session-memory/src/fact_store/**` or `src/memory/**` as part of the adapter. |
| Configuration/selection owner | `crates/tracedecay-configuration/src/config/**`; `crates/tracedecay-domain/src/configuration.rs`; `crates/tracedecay-global-db/src/configuration/registry.rs` | Own setting definitions, defaults, restart policy, and selection validation. Coordinate with the composition owner; do not add an implicit activation path. |
| Composition/lifecycle owner | `crates/tracedecay/src/daemon/project_composition.rs` and the narrow project-server mount/lifecycle seams | Preserve authoritative scope ordering, Native/NCM independent construction, selected active registration, observer journeys, and NCM host-state authority. Depends on adapter and provider API identities. |
| MCP/context owner | `crates/tracedecay-mcp/src/handlers/graph/context_support.rs`, `search.rs`, and the canonical retrieval contract owner for `primitive_surface.rs` | Keep canonical Native memory assembly and its typed sidecar. Any cross-lane duplicate rule needs a contract decision before changing this owner. |
| Host recall/output owner | Narrow regions of `crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs` and `crates/tracedecay/src/mcp/server/requests/tool_dispatch.rs` | Keep exact-tool admission, provider identity/scope, hydration, untrusted gate, pack budgets, JSON/Markdown merge, and typed-withheld behavior. `cognitive_recall.rs` is a shared write hotspot and should not be edited opportunistically by the Native adapter owner; split recall/provenance/packing changes or assign one explicit delivery owner. |
| Host hooks/package owner | `crates/tracedecay-agent-hosts/src/hooks/**`, `agents/codex.rs`, `plugin/**`, and host-integration evidence | Preserve Claude/Codex lifecycle differences, rendered Codex activation/trust, NCM observer delivery, and fixture-gated host claims. This owner must not add provider recall to ordinary prompts. |

The following stay untouched by a host-delivery implementation unless their
existing owners approve a contract change: all Native fact-store and memory
implementation files; `crates/tracedecay/src/tracedecay/facts.rs`; runtime-core
database/migration/lease/reconciliation code; `crates/tracedecay-store/src/memory/**`
contracts without a canonical update; the NCM worker/state authority; and
unsupported host catalogs. Do not turn ADR-0010's read-only advisory projection
into a restriction on complete Native behavior.

## Observable acceptance checklist

- With default configuration, no provider adapter, worker, journal, or recall
  route is mounted. Enabling the provider-host feature alone still produces no
  provider output.
- With Native enabled and no active routing target, Native observation replay is
  required and provider infrastructure is observable, but no advisory recall
  route is selected. With an NCM observer configured, its state authority and
  receipts remain present beside Native.
- With one explicit active provider, project open uses the authoritative
  code-index scope, registers only that provider for recall, and refuses a
  fallback target that cannot be registered. An observer cannot answer the
  product recall lane.
- A `tracedecay_context` request returns canonical `memory_matches` and the
  `Memory Matches` section according to its own memory policy. Selecting NCM
  does not silently erase canonical Native facts; selecting Native does not
  shut down configured NCM observation state.
- The same request then appends at most the contract-approved provider
  contribution. A candidate repeated across canonical fact identity and
  provider provenance is either suppressed with an auditable identity decision
  or represented once under the explicit contract; it must not become two
  indistinguishable model-visible copies. Provider-only duplicate provenance is
  also rejected.
- A provider candidate with unconfirmed `record:`, `source:`, or `session:`
  provenance is excluded by the default host policy. A scope mismatch,
  cancellation, deadline, untrusted-gate fault, or pack refusal produces a
  typed advisory outcome/notice and leaves the canonical host answer intact.
- A non-`tracedecay_context` MCP tool and an ordinary Claude/Codex prompt do not
  trigger provider advisory recall through the inspected path. Hook output may
  contain lifecycle guidance or steering, while facts remain owned by the
  canonical/tool delivery surfaces.
- Claude's shipped SessionStart/Stop journey and Codex's shipped Stop journey
  capture bounded live observations, replay idempotently, and recall the same
  project history after daemon restart and a new session. The existing
  product journey is [`product_memory_provider_claude_host_journey.rs`](../../../../crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs#L1808-L1861)
  with shared lifecycle assertions at [`assert_host_memory_journey_with_provider`](../../../../crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs#L1872-L2069)
  and provenance/tail/duplicate checks at [`assert_recalled_session_messages`](../../../../crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs#L2589-L2734).
- A real NCM observer run receives its configured host observations while Native
  remains the answering provider; a real NCM active run answers with Native
  advisory disabled. These are ignored/pinned-model journeys and must not be
  replaced by a shutdown or empty-result shortcut.
- Claude's raw hook manifest and Codex's rendered global bundle activate the
  expected lifecycle commands. A Codex repo-local bundle has no hooks and uses
  `serve --path .`; a global bundle requires plugin activation and current hook
  trust. A staged marketplace entry alone does not count as delivery.
- Shared MCP/context behavior is accepted for every installable host with an
  MCP component. Native hook claims are accepted only for the seven fixture-
  backed hosts; unsupported CursorCloud/ClineFamily remain typed unavailable.

## Unknowns and conflicts

1. The b3 source establishes the complete Native fact-store/memory authority,
   but this lane found no original host wrapper proving automatic learning or
   recall on ordinary chat. That behavior must remain unknown until a source
   or real-host artifact proves it.
2. Current code intentionally preserves canonical Native `memory_matches` while
   appending selected-provider advisory candidates. Provider-lane uniqueness is
   tested, but a complete canonical-ID/content duplicate rule across the two
   lanes is not established here. The contract owner must decide whether typed
   Native identity, a provider `record:` claim, or an explicit content collision
   is the governing key before implementation.
3. The `memory_injection_enabled` name and default-true user setting can look
   like automatic memory recall, but the gate source says hooks do not query
   facts/LCM or persist recall state. A real Claude/Codex output check should
   verify the intended guidance-only behavior.
4. The provider-host feature is default-off in both [`tracedecay/Cargo.toml`](../../../../crates/tracedecay/Cargo.toml#L143-L163)
   and [`tracedecay-cli/Cargo.toml`](../../../../crates/tracedecay-cli/Cargo.toml#L82-L87),
   while the product memory journey requires that feature ([`Cargo.toml`](../../../../crates/tracedecay-cli/Cargo.toml#L39-L45)).
   Release ownership must decide which shipped binary is expected to carry the
   opt-in host; this report does not authorize changing the default.
5. The raw Codex hooks scaffold is empty but the global renderer fills it. The
   installed artifact and host activation/trust state, rather than the raw
   template, are the acceptance evidence.
6. The host catalog intentionally distinguishes installable MCP support from
   native fixture evidence. Missing fixtures for Devin, Zed, Antigravity, Vibe,
   Gemini, Copilot, Roo, and Kilo do not justify adding hook claims.
7. ADR-0010's read-only projection restrictions conflict with the user's
   complete Native requirement if applied to direct Native operations. The
   advisory recall lane may remain read-only where its contract says so; the
   Native adapter must not suppress original Native state changes or retrieval
   effects.

## Bounded implementation proposal

**Goal.** Restore and expose the complete b3 Native behavior through the
selected-provider integration while keeping canonical Native facts, selected
provider advisory output, and configured NCM observer state distinct and
observable on Claude, Codex, and the shared MCP path.

**Prerequisites.** The source lane must accept the Native floor and operation/
effect map. The provider API owner must settle any Native effect semantics at
the provider boundary. The MCP contract owner must decide the cross-lane
identity/dedup rule. Host owners must confirm the intended guidance-only hook
behavior and the global/local Codex bundle policy.

**Files.** The Native adapter owner works in
`crates/tracedecay-memory-provider-native/**` and its narrow existing Native
integration. The host-delivery owner works only in the named recall/dispatch
seams and focused product journeys. Configuration, MCP contract, host hooks,
and plugin renderers remain with their owners. No Native source tree or NCM
state authority is copied, deleted, or rewritten.

**Steps.**

1. Freeze the default/observer/active matrix and the selected-provider route
   against the current configuration and composition behavior.
2. Implement the Native adapter as delegation to the owner-bound Native
   application, carrying all proven Native operations, ranking, state effects,
   scope, and typed failures. Do not place a staged observation table or a new
   lexical/recency scorer behind the Native identity.
3. Keep composition order and lifecycle ownership: obtain the authoritative
   scope first, mount Native/NCM independently, bind each configured history
   authority, and route advisory recall only to the selected active provider.
4. After the contract decision, add the smallest typed cross-lane identity
   check needed to avoid duplicate model-visible contributions. If the contract
   cannot define the identity or collision behavior, stop before editing the
   context assembly.
5. Exercise the real Claude and Codex journeys, including Native active, NCM
   observer, and NCM active variants; check live observation, replay
   idempotency, complete tail content, provenance, restart, and scope.
6. Exercise MCP JSON and Markdown packing, provider timeout/withholding,
   canonical memory policy, and Codex global/local activation. Verify host
   inventory claims against fixture evidence and keep unsupported hosts typed.

**Verification.** Future verification should use the existing feature-gated
product journeys and host lifecycle acceptance suites, plus focused provider,
context-sidecar, cross-lane duplicate, pack, and hook-output cases. It must
observe persisted state, returned JSON/Markdown, provider identity, provenance,
scope, restart behavior, and typed failures. No tests were run for this report.

**Forbidden shortcuts.** Do not enable Native or NCM by default; select by
environment or empty-result inference; substitute staged observations for
Native; disable NCM observer state; stop all providers when Native is selected;
query provider recall from every hook or ordinary prompt; apply an invented
host scorer; drop canonical `memory_matches`; accept unconfirmed provenance;
truncate the host answer to fit advisory text; treat the raw Codex scaffold as
installed; or claim native hook support for a host without fixture evidence.

**Stop condition.** Stop and return the unresolved evidence if the accepted
Native floor/effect map, cross-lane identity contract, or NCM state ownership is
missing; if a change would require writing a shared file outside the named
owner; if provider output could displace canonical host evidence; or if a real
Claude/Codex journey cannot distinguish hook observation from startup import,
selected-provider recall, and canonical Native facts.
