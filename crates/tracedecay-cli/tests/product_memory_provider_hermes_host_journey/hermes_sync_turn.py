"""Drive the installed TraceDecay Hermes plugin through its host boundary.

The repository does not carry a stock Hermes checkout. When that runtime is
absent, the supported plugin boundary is still testable: ``register(ctx)``
receives a small, faithful ``PluginContext`` implementation, and the fixture
drives the registered provider, post-tool hook, and context engine exactly as
the host would. The sentinel says ``register_ctx_fixture`` so a passing test
cannot be mistaken for a stock-loader run.
"""

import importlib.machinery
import importlib.util
import json
import os
import pathlib
import sys
import time


plugin_dir = pathlib.Path(sys.argv[1])
project_root = pathlib.Path(sys.argv[2]).resolve()
trace_decay_bin = sys.argv[3]
mode = sys.argv[4] if len(sys.argv) > 4 else "original"
session_id = "hermes-cli-project-journey"
fixed_timestamp_ns = int(
    os.environ.get("TRACEDECAY_HERMES_REPLAY_TIMESTAMP_NS", time.time_ns())
)
fixed_timestamp = fixed_timestamp_ns / 1_000_000_000

# ``tools.py`` captures this override while the generated package is imported.
# It keeps the fixture independent of the binary path baked into the install.
os.environ["TRACEDECAY_BIN"] = trace_decay_bin


class PluginContext:
    """The subset of Hermes' PluginContext used by the generated plugin.

    Stock Hermes currently does not advertise
    ``context_engine_tool_handlers_receive_messages``. Keeping that
    capability false exercises the provider's lifecycle sync and still lets
    this fixture invoke the registered ContextEngine callback directly.
    """

    context_engine_tool_handlers_receive_messages = False

    def __init__(self, root):
        self.config = {
            "memory": {"provider": "tracedecay"},
            "project_root": str(root),
        }
        self.hermes_home = os.path.join(os.environ["HOME"], ".hermes")
        self.hooks = {}
        self.tools = {}
        self.memory_providers = []
        self.context_engines = []
        self.skills = {}
        self.commands = {}

    def register_hook(self, name, handler):
        self.hooks[name] = handler

    def register_tool(self, name=None, toolset=None, schema=None, handler=None, **kwargs):
        self.tools[name] = {
            "toolset": toolset,
            "schema": schema,
            "handler": handler,
            **kwargs,
        }

    def register_memory_provider(self, provider):
        self.memory_providers.append(provider)

    def register_context_engine(self, engine):
        self.context_engines.append(engine)

    def register_skill(self, name, path):
        self.skills[name] = pathlib.Path(path)

    def register_config_defaults(self, defaults):
        self.config_defaults = defaults

    def register_command(self, name, handler, description=""):
        self.commands[name] = {"handler": handler, "description": description}


# The generated plugin is a package (__init__.py imports sibling modules), so
# load it with a synthetic package parent exactly as a plugin manager does.
package_name = "_tracedecay_hermes_cli_journey"
package_spec = importlib.machinery.ModuleSpec(package_name, None, is_package=True)
package_spec.submodule_search_locations = []
package = importlib.util.module_from_spec(package_spec)
sys.modules[package_name] = package

module_name = f"{package_name}.tracedecay"
spec = importlib.util.spec_from_file_location(
    module_name,
    plugin_dir / "__init__.py",
    submodule_search_locations=[str(plugin_dir)],
)
plugin = importlib.util.module_from_spec(spec)
sys.modules[module_name] = plugin
spec.loader.exec_module(plugin)


def _sync_turn(provider):
    """Run one deterministic turn so a fresh provider can replay it exactly."""

    original_time_ns = plugin.time.time_ns
    original_time = plugin.time.time
    plugin.time.time_ns = lambda: fixed_timestamp_ns
    plugin.time.time = lambda: fixed_timestamp
    try:
        provider.sync_turn(
            "Hermes captured a quartz crystal workspace observation from the user.",
            "Hermes recorded the quartz crystal project decision for the assistant.",
            session_id=session_id,
            messages=[
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "name": "terminal",
                            "arguments": {"workdir": str(project_root)},
                        }
                    ],
                }
            ],
        )
    finally:
        plugin.time.time_ns = original_time_ns
        plugin.time.time = original_time


def _decode_tool_result(raw):
    """Decode the generated engine's MCP envelope without hiding failures."""

    outer = raw if isinstance(raw, dict) else json.loads(raw)
    if not isinstance(outer, dict):
        raise AssertionError(f"context engine returned non-object: {outer!r}")
    if outer.get("error") or outer.get("isError") is True:
        raise AssertionError(f"context engine callback failed: {outer!r}")
    blocks = outer.get("content")
    if not isinstance(blocks, list):
        return outer
    for block in blocks:
        text = block.get("text") if isinstance(block, dict) else None
        if isinstance(text, str):
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and not payload.get("error"):
                return payload
    return outer


ctx = PluginContext(project_root)
plugin.register(ctx)
assert "post_tool_call" in ctx.hooks, sorted(ctx.hooks)
assert len(ctx.memory_providers) == 1, len(ctx.memory_providers)
assert len(ctx.context_engines) == 1, len(ctx.context_engines)

provider = ctx.memory_providers[0]
provider.initialize(
    session_id=session_id,
    hermes_home=ctx.hermes_home,
    project_root=str(project_root),
)
assert provider.project_root == str(project_root), provider.project_root
if mode == "original":
    _sync_turn(provider)
    plugin._join_host_receipts()
elif mode == "replay":
    # The replay uses a new host process and a new provider object. The shared
    # timestamp supplied by Rust makes its generated message IDs byte-for-byte
    # identical to the original process's IDs.
    pass
else:
    raise AssertionError(f"unknown fixture mode: {mode!r}")

# A real Hermes post-tool callback is content-free and carries the project
# route plus receipt identity. Exercise the registered wrapper, then join its
# worker so the fixture's successful exit includes the callback's side effect.
ctx.hooks["post_tool_call"](
    {
        "name": "terminal",
        "project_root": str(project_root),
        "session_id": session_id,
        "turn_id": "hermes_sync_1",
        "tool_call_id": "terminal_1",
        "status": "success",
        "duration_ms": 17,
        "args": {"workdir": str(project_root)},
    }
)
plugin._join_host_receipts()

fresh_provider = None
if mode == "replay":
    # Re-run the same turn with a fresh provider object and the same timestamp.
    # sync_turn therefore emits the exact original message IDs and calls the
    # daemon's admission boundary again; no new observation row may result.
    fresh_provider = plugin.TracedecayMemoryProvider()
    fresh_provider.initialize(
        session_id=session_id,
        hermes_home=ctx.hermes_home,
        project_root=str(project_root),
    )
    assert fresh_provider.project_root == provider.project_root
    _sync_turn(fresh_provider)
    plugin._join_host_receipts()

# Hermes selects this engine after register(ctx). Invoke a read through the
# public callback with a paraphrased query, proving the installed context
# engine reaches the live daemon rather than merely being registered.
engine = ctx.context_engines[0]
engine.on_session_start(
    session_id=session_id,
    hermes_home=ctx.hermes_home,
    project_root=str(project_root),
)
status = _decode_tool_result(
    engine.handle_tool_call(
        "lcm_status",
        {},
        session_id=session_id,
        project_root=str(project_root),
        messages=[],
    )
)
grep = _decode_tool_result(
    engine.handle_tool_call(
        "lcm_grep",
        {"query": "crystal workspace note", "limit": 10, "session_scope": "current"},
        session_id=session_id,
        project_root=str(project_root),
        messages=[],
    )
)
assert isinstance(status, dict), status
assert isinstance(grep, dict), grep
engine.on_session_end(session_id=session_id)

print(
    json.dumps(
        {
            "session_id": session_id,
            "project_root": provider.project_root,
            "sync": "complete",
            "installed_provider_id": provider.provider_id,
            # Hermes' transcript admission is live and project-scoped. Its
            # canonical source provider is deliberately reported separately
            # from the installed TraceDecay memory-provider ID so a later
            # history/control assertion cannot silently conflate the two.
            "canonical_provider_id": "hermes",
            # The current daemon history reader has no Hermes host-origin
            # mapping. Report that boundary as a typed capability state rather
            # than making an absent source look like an empty history binding.
            "history_control": {
                "capability": "provider_history",
                "canonical_provider_id": "hermes",
                "state": "unsupported",
                "source_resolution": "hook_origin_reader_no_hermes_mapping",
            },
            "host_boundary": "register_ctx_fixture",
            "context_engine_callback": "complete",
            "replay": {
                "mode": "exact" if mode == "replay" else "original",
                "fresh_provider": mode == "replay",
                "message_ids": [
                    f"tracedecay_sync_1_{fixed_timestamp_ns}_0_user",
                    f"tracedecay_sync_1_{fixed_timestamp_ns}_1_assistant",
                ],
            },
        }
    )
)
