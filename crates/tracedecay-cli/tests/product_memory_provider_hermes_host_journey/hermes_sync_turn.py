"""Drive the installed TraceDecay Hermes provider through its live CLI seam."""

import importlib.machinery
import importlib.util
import json
import os
import pathlib
import sys


plugin_dir = pathlib.Path(sys.argv[1])
project_root = pathlib.Path(sys.argv[2]).resolve()
trace_decay_bin = sys.argv[3]
session_id = "hermes-cli-project-journey"

# `tools.py` captures this override while the generated package is imported.
# It keeps the fixture independent of the binary path baked into the install.
os.environ["TRACEDECAY_BIN"] = trace_decay_bin

# The generated plugin is a package (`__init__.py` imports sibling modules),
# so load it with a synthetic package parent exactly as Hermes does. The test
# invokes the installer first; this fixture only drives the shipped artifact.
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

provider = plugin.TracedecayMemoryProvider()
provider.initialize(
    session_id=session_id,
    hermes_home=os.environ["HOME"] + "/.hermes",
    project_root=str(project_root),
)
assert provider.project_root == str(project_root), provider.project_root

provider.sync_turn(
    "hermes project scope quartz user observation",
    "hermes project scope quartz assistant observation",
    session_id=session_id,
    # The project is present in the host-shaped tool history as well as the
    # explicit provider binding. This exercises Hermes' project extraction and
    # the generated project-scoped callback/receipt route together.
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

# Hermes invokes turnCompleted and turnIngested asynchronously. Joining is the
# host lifecycle boundary that makes this fixture's successful exit meaningful.
plugin._join_host_receipts()
print(
    json.dumps(
        {
            "session_id": session_id,
            "project_root": provider.project_root,
            "sync": "complete",
        }
    )
)
