#!/usr/bin/env bash
# Smoke-probe each native Work provider transport against the stock executable
# an operator has installed: launch it with the Work argv, wait for the
# provider's own startup event, interrupt it, and record how the process exited.
#
# Scope: this shows the installed CLIs still accept these launch plans and
# report startup on their native protocol. It does not run the Work runtime, so
# it certifies neither Work admission nor terminal settlement; the Rust tests
# under `work_attempt_exec` own those. A process exit here is not a Work receipt.
#
# Running this uses the current stock-provider login, HOME, and working
# directory, and may begin a model request. The explicit opt-in prevents a
# developer or CI job from consuming a provider request by accident:
#
#   TRACEDECAY_WORK_NATIVE_CONFORMANCE=1 \
#     scripts/work_provider_native_conformance.sh
#
# Overrides:
#   CLAUDE_BIN  absolute stock Claude Code binary (default: `claude` on PATH)
#   CODEX_BIN   absolute stock Codex binary (default: `codex` on PATH)

set -euo pipefail
# Job control gives each provider its own process group and keeps SIGINT at its
# default disposition; without it bash starts background jobs with SIGINT
# ignored, unlike the Work runtime's children.
set -m

if [[ "${TRACEDECAY_WORK_NATIVE_CONFORMANCE:-}" != "1" ]]; then
    echo "error: native Work-provider conformance is opt-in; set TRACEDECAY_WORK_NATIVE_CONFORMANCE=1" >&2
    exit 2
fi

for required in python3 sha256sum; do
    command -v "$required" >/dev/null 2>&1 || {
        echo "error: $required is required for native Work-provider conformance" >&2
        exit 2
    }
done

stage="$(mktemp -d "${TMPDIR:-/tmp}/tracedecay-work-provider.XXXXXX")"
trap 'rm -rf "$stage"' EXIT

resolve_stock_binary() {
    local label="$1"
    local configured="$2"
    local default_name="$3"
    local candidate

    candidate="${configured:-$(command -v "$default_name" || true)}"
    if [[ -z "$candidate" || ! -x "$candidate" ]]; then
        # This mirrors `WorkProviderAvailabilityV1::Unavailable`: there is no
        # configured executable that can be started. It is a diagnostic
        # outcome, never a pass.
        printf '%s\n' "provider=$label availability=unavailable reason=host_binary_absent" >&2
        return 1
    fi
    printf '%s/%s\n' "$(cd "$(dirname "$candidate")" && pwd)" "$(basename "$candidate")"
}

# Prints the session id from the first startup event the Work runtime also
# recognises (`provider_output::session_from_event`).
startup_session() {
    python3 - "$1" "$2" <<'PY'
import json
import sys

protocol, path = sys.argv[1:]
with open(path, errors="replace") as output:
    for line in output:
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict):
            continue
        if protocol == "claude-stream-json" and event.get("type") == "system" and event.get("subtype") == "init":
            session = event.get("session_id")
        elif protocol == "codex-exec-json" and event.get("type") == "thread.started":
            session = event.get("thread_id")
        else:
            continue
        if isinstance(session, str) and session:
            print(session)
            sys.exit(0)
sys.exit(1)
PY
}

wait_for_startup() {
    local pid="$1"
    local label="$2"
    local protocol="$3"
    local output="$4"

    for _ in $(seq 1 300); do
        if startup_session "$protocol" "$output"; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "error: $label exited before reporting startup" >&2
            return 1
        fi
        sleep 0.05
    done
    kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
    echo "error: $label did not report startup within 15s" >&2
    return 1
}

# Sets `exit_status`; it must run in this shell, the provider's parent.
interrupt_and_wait() {
    local pid="$1"
    local label="$2"

    kill -INT -- "-$pid" 2>/dev/null || kill -INT "$pid"
    for _ in $(seq 1 100); do
        if ! kill -0 "$pid" 2>/dev/null; then
            set +e
            wait "$pid"
            exit_status=$?
            set -e
            return 0
        fi
        sleep 0.05
    done

    # A provider still running after the bounded interrupt window fails the
    # probe; kill its owned process group.
    kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    echo "error: $label did not exit after SIGINT" >&2
    return 1
}

run_stdio_provider() {
    local label="$1"
    local protocol="$2"
    local executable="$3"
    shift 3
    local input="$stage/$label.input"
    local output="$stage/$label.output"
    local pid session

    printf '%s\n' 'Return only the word READY. Do not use tools or modify files.' > "$input"
    echo "== $label: $executable ($("$executable" --version 2>/dev/null | head -n 1 || echo unknown))"
    env -i HOME="$HOME" PATH="$PATH" "$executable" "$@" < "$input" > "$output" 2>&1 &
    pid=$!
    session="$(wait_for_startup "$pid" "$label" "$protocol" "$output")"
    interrupt_and_wait "$pid" "$label"

    if grep -Eqi '"subtype"[[:space:]]*:[[:space:]]*"success"|"turn.completed"' "$output"; then
        echo "error: $label completed successfully before the interrupt; no interrupt evidence" >&2
        return 1
    fi
    local digest
    digest="sha256:$(sha256sum "$output" | awk '{print $1}')"
    printf '%s\n' "provider=$label availability=available start=observed session_id=$session stop=sigint exit_status=$exit_status stdout_digest=$digest"
}

run_codex_app_server() {
    local executable="$1"

    echo "== codex-app-server: $executable ($("$executable" --version 2>/dev/null | head -n 1 || echo unknown))"
    # Python is used solely for framed JSON-RPC I/O against the stock
    # `codex app-server`, sending initialize → thread/start → turn/start.
    CODEX_CONFORMANCE_BIN="$executable" HOME="$HOME" PATH="$PATH" python3 - <<'PY'
import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time

binary = os.environ["CODEX_CONFORMANCE_BIN"]
child_env = {"HOME": os.environ["HOME"], "PATH": os.environ["PATH"]}
proc = subprocess.Popen(
    [binary, "app-server"],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    bufsize=1,
    env=child_env,
    start_new_session=True,
)

def send(message):
    proc.stdin.write(json.dumps(message) + "\n")
    proc.stdin.flush()

# A reader thread, not select(): one pipe read can buffer several frames.
lines = queue.Queue()

def pump():
    for line in proc.stdout:
        lines.put(line)
    lines.put(None)

threading.Thread(target=pump, daemon=True).start()

def response(request_id, label):
    deadline = time.monotonic() + 15
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            line = lines.get(timeout=remaining)
        except queue.Empty:
            break
        if line is None:
            proc.wait(timeout=10)
            raise RuntimeError(f"codex app-server exited while waiting for {label}: {proc.stderr.read()}")
        message = json.loads(line)
        if not isinstance(message, dict) or message.get("id") != request_id:
            continue
        if "error" in message:
            raise RuntimeError(f"codex app-server {label} failed: {message['error']}")
        result = message.get("result")
        if not isinstance(result, dict):
            raise RuntimeError(f"codex app-server {label} lacked a result: {message}")
        return result
    raise RuntimeError(f"timed out waiting for {label}")

try:
    send({"method": "initialize", "id": 0, "params": {"clientInfo": {"name": "tracedecay_work_provider_conformance", "version": "1"}}})
    response(0, "initialize")
    send({"method": "initialized", "params": {}})
    send({"method": "thread/start", "id": 1, "params": {"ephemeral": True, "threadSource": "tracedecay_work_provider_conformance"}})
    thread = response(1, "thread/start")
    thread_id = (thread.get("thread") or {}).get("id") or thread.get("id")
    if not isinstance(thread_id, str) or not thread_id:
        raise RuntimeError(f"stock thread/start omitted a thread id: {thread}")
    send({"method": "turn/start", "id": 2, "params": {"threadId": thread_id, "input": [{"type": "text", "text": "Return only READY. Do not use tools or modify files."}], "cwd": os.getcwd(), "effort": "low", "summary": "concise"}})
    turn = response(2, "turn/start").get("turn")
    turn_id = turn.get("id") if isinstance(turn, dict) else None
    if not isinstance(turn_id, str) or not turn_id:
        raise RuntimeError(f"turn/start response lacked a turn id: {turn}")
    if turn.get("status") == "failed":
        raise RuntimeError(f"turn/start reported a failed turn: {turn}")
    os.killpg(proc.pid, signal.SIGKILL)
    proc.wait(timeout=10)
    print(f"provider=codex-app-server availability=available start=observed turn_id={turn_id} stop=sigkill exit_status={proc.returncode}")
except Exception as error:
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)
    print(f"error: codex app-server conformance failed: {error}", file=sys.stderr)
    raise
PY
}

claude_bin="$(resolve_stock_binary claude-code-cli "${CLAUDE_BIN:-}" claude)" || exit 1
codex_bin="$(resolve_stock_binary codex "${CODEX_BIN:-}" codex)" || exit 1

# These argv are copies of `provider_arguments` and `codex_app_server_command`.
# Nothing ties them together: a production launch-plan change needs a matching
# edit here.
run_stdio_provider claude-code-cli claude-stream-json "$claude_bin" --print --output-format stream-json --verbose
run_codex_app_server "$codex_bin"
run_stdio_provider codex-cli codex-exec-json "$codex_bin" exec --json -

echo "native Work-provider launch probe: PASS"
