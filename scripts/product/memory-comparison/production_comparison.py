"""Draft transport for the reviewed real-host comparison test child.

This module supplies no host/provider evidence. It preserves the child's opaque
metadata, action and finish bodies, and acknowledges metadata/actions only after
Capture's durable write returns. The executable is built and approved by the
lead; this module neither runs Cargo nor chooses a different fixture on failure.

Connector configuration is separate from frozen experiment inputs:
TRACEDECAY_HOST_COMPARISON_TEST_EXECUTABLE is the absolute built CLI test target;
TRACEDECAY_HOST_COMPARISON_ARTIFACT_ROOT is an existing caller-owned directory.
All case artifacts survive success and failure. Only the owned socket is removed.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import secrets
import signal
import socket
import struct
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from capture import ProcessIdentity, ProcessTreeCapture, capture_directories, read_process_identity


PROTOCOL = "tracedecay.host-comparison.v1"
MAX_FRAME_BYTES = 64 * 1024 * 1024
TEST_ARGUMENTS = ("--ignored", "--exact", "comparison_fixture::host_comparison_fixture_entry", "--nocapture")
EXECUTABLE_ENV = "TRACEDECAY_HOST_COMPARISON_TEST_EXECUTABLE"
ARTIFACT_ROOT_ENV = "TRACEDECAY_HOST_COMPARISON_ARTIFACT_ROOT"


class ProtocolError(ValueError):
    pass


@dataclass(frozen=True)
class Timeouts:
    """Connector cutoffs only; none are host/provider latency measurements."""
    connect: float = 30.0
    frame: float = 240.0
    case: float = 1800.0
    close: float = 30.0
    terminate: float = 5.0
    kill: float = 5.0

    def __post_init__(self):
        if any(not math.isfinite(value) or value <= 0 for value in asdict(self).values()):
            raise ValueError("transport timeouts must be positive and finite")


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("comparison transport deadline exceeded")
    return remaining


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def _same_input(left, right):
    """JSON types matter: Python's True == 1 cannot hide input substitution."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(_same_input(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(_same_input(a, b) for a, b in zip(left, right))
    return left == right


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ProtocolError(f"duplicate JSON member: {key}")
        value[key] = item
    return value


def _invalid_constant(value):
    raise ProtocolError(f"nonfinite JSON number: {value}")


def _finite_float(value):
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ProtocolError("nonfinite JSON number")
    return parsed


def _sync(stream):
    if stream is not None:
        stream.flush()
        os.fsync(stream.fileno())


class FramedChannel:
    """Bounded framing retaining the exact bytes used for acknowledgments."""

    def __init__(self, stream, nonce, *, incoming=None, outgoing=None):
        if len(nonce) != 64 or any(char not in "0123456789abcdef" for char in nonce):
            raise ValueError("comparison nonce must be 64 lowercase hexadecimal characters")
        self.stream, self.nonce = stream, nonce
        self.incoming, self.outgoing = incoming, outgoing

    def _read(self, count, deadline):
        parts = bytearray()
        while len(parts) < count:
            self.stream.settimeout(_remaining(deadline))
            chunk = self.stream.recv(min(count - len(parts), 65536))
            if not chunk:
                raise EOFError("comparison child closed a partial or missing frame")
            if self.incoming is not None:
                self.incoming.write(chunk)
            parts.extend(chunk)
        return bytes(parts)

    def receive(self, deadline):
        try:
            declared = struct.unpack(">Q", self._read(8, deadline))[0]
            if not 0 < declared <= MAX_FRAME_BYTES:
                raise ProtocolError(f"comparison frame length {declared} outside 1..={MAX_FRAME_BYTES}")
            payload = self._read(declared, deadline)
            value = json.loads(payload.decode("utf-8", errors="strict"),
                               object_pairs_hook=_unique_object, parse_constant=_invalid_constant,
                               parse_float=_finite_float)
            if not isinstance(value, dict) or value.get("protocol") != PROTOCOL or value.get("nonce") != self.nonce:
                raise ProtocolError("comparison protocol or nonce mismatch")
            if not isinstance(value.get("kind"), str) or "action_index" not in value or "body" not in value:
                raise ProtocolError("comparison child frame lacks kind, action_index or body")
            return value, payload
        finally:
            # Preserve complete frames and partial failed reads alike. The raw
            # payload is never reserialized when computing its acknowledgment.
            _sync(self.incoming)

    def send(self, kind, deadline, **fields):
        payload = _json_bytes({"protocol": PROTOCOL, "nonce": self.nonce, "kind": kind, **fields})
        if not 0 < len(payload) <= MAX_FRAME_BYTES:
            raise ProtocolError("outgoing comparison frame exceeds transport bound")
        framed = struct.pack(">Q", len(payload)) + payload
        # The outgoing artifact is the exact attempted frame. A send failure
        # does not establish how many of these bytes the child consumed.
        if self.outgoing is not None:
            self.outgoing.write(framed)
            _sync(self.outgoing)
        self.stream.settimeout(_remaining(deadline))
        self.stream.sendall(framed)


class SocketSession:
    """Ordered case transport, independent of process creation and test peers."""

    def __init__(self, channel, invocation, timeouts):
        self.channel, self.invocation, self.timeouts = channel, copy.deepcopy(invocation), timeouts
        self.started = self.finished = False
        self.broken = False

    def _receive(self, deadline):
        return self.channel.receive(min(deadline, time.monotonic() + self.timeouts.frame))

    def _ack(self, frame, payload, deadline):
        self.channel.send("ack", deadline, ack_kind=frame["kind"],
                          action_index=frame["action_index"],
                          frame_sha256=hashlib.sha256(payload).hexdigest())

    def replay(self, invocation, capture):
        if self.started or not _same_input(invocation, self.invocation):
            raise ProtocolError("case invocation changed or replay was requested twice")
        self.started = True
        deadline = time.monotonic() + self.timeouts.case
        try:
            self.channel.send("hello", deadline)
            ready, _ = self._receive(deadline)
            if ready["kind"] != "ready" or ready["action_index"] is not None or ready["body"] is not None:
                raise ProtocolError("expected empty ready frame before invocation")
            self.channel.send("create", deadline, body=self.invocation)
            frame, payload = self._receive(deadline)
            if frame["kind"] != "metadata" or frame["action_index"] is not None or not isinstance(frame["body"], dict):
                raise ProtocolError("actual case metadata must precede action evidence")
            capture.record_metadata(frame["body"])
            self._ack(frame, payload, deadline)
            next_index = 0
            while True:
                frame, payload = self._receive(deadline)
                if frame["kind"] == "finish":
                    body = frame["body"]
                    if frame["action_index"] is not None or not isinstance(body, dict):
                        raise ProtocolError("invalid finish envelope")
                    if body.get("case_trial_id") != self.invocation["case_trial_id"]:
                        raise ProtocolError("finish changed case identity")
                    if next_index < len(self.invocation["actions"]):
                        reason = body.get("reason")
                        if body.get("status") not in ("unexecuted", "censored") or not isinstance(reason, str) or not reason.strip():
                            raise ProtocolError("early finish requires explicit unexecuted/censored status and reason")
                    self.finished = True
                    return body
                if frame["kind"] != "action" or type(frame["action_index"]) is not int or frame["action_index"] != next_index:
                    raise ProtocolError("expected next scheduled action index or finish")
                if next_index >= len(self.invocation["actions"]):
                    raise ProtocolError("child emitted an unscheduled action")
                body = frame["body"]
                if not isinstance(body, dict) or not _same_input(body.get("input"), self.invocation["actions"][next_index]):
                    raise ProtocolError("child changed the scheduled action input")
                capture.record_action(body)
                self._ack(frame, payload, deadline)
                next_index += 1
        except BaseException:
            self.broken = True
            raise

    def close(self):
        if not self.finished or self.broken:
            raise ProtocolError("no verified finish; abort transport without a skipped acknowledgment")
        deadline = time.monotonic() + self.timeouts.close
        self.channel.send("close", deadline)
        frame, _ = self.channel.receive(deadline)
        if frame["kind"] != "closed" or frame["action_index"] is not None or not isinstance(frame["body"], dict):
            raise ProtocolError("expected actual cleanup evidence")
        return frame["body"]


def _artifact(path):
    hasher, size = hashlib.sha256(), 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            hasher.update(chunk)
            size += len(chunk)
    return {"path": str(path), "bytes": size, "sha256": hasher.hexdigest()}


def _write_json(path, body):
    with path.open("xb") as stream:
        stream.write(_json_bytes(body))
        _sync(stream)
    return _artifact(path)


class ProductionFixture:
    def __init__(self, invocation, executable, artifact_parent, timeouts):
        self.invocation, self.timeouts = copy.deepcopy(invocation), timeouts
        self.artifacts = Path(tempfile.mkdtemp(prefix="case-", dir=artifact_parent))
        self.child = self.identity = self.resources = self.session = None
        self.listener = self.connection = self.socket_root = None
        self.streams, self.signals = {}, []
        self.cleanup = None
        self._launch(executable)

    def _launch(self, executable):
        try:
            _write_json(self.artifacts / "connector-invocation.json", self.invocation)
            # Unix paths are length limited, including on macOS. This directory
            # contains only the owned socket; all evidence stays at artifacts.
            self.socket_root = Path(tempfile.mkdtemp(prefix="tdhc-", dir="/tmp"))
            socket_path = self.socket_root / "control.sock"
            self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.listener.bind(str(socket_path))
            self.listener.listen(1)
            nonce = secrets.token_hex(32)
            for name in ("stdout", "stderr", "received", "sent"):
                self.streams[name] = (self.artifacts / f"connector-{name}.bin").open("xb")
            environment = {**os.environ, "TRACEDECAY_HOST_COMPARISON_SOCKET": str(socket_path),
                           "TRACEDECAY_HOST_COMPARISON_NONCE": nonce,
                           ARTIFACT_ROOT_ENV: str(self.artifacts)}
            self.child = subprocess.Popen([str(executable), *TEST_ARGUMENTS],
                                          stdin=subprocess.DEVNULL, stdout=self.streams["stdout"],
                                          stderr=self.streams["stderr"], env=environment,
                                          start_new_session=True)
            self.identity = read_process_identity(self.child.pid)
            if os.getpgid(self.child.pid) != self.child.pid:
                raise RuntimeError("comparison test child does not own its process group")
            self.resources = ProcessTreeCapture(self.identity, "comparison_fixture_process")
            self.resources.start()
            _write_json(self.artifacts / "connector-launch.json", {
                "argv": [str(executable), *TEST_ARGUMENTS], "root": asdict(self.identity),
                "process_group_id": self.child.pid, "timeouts_seconds": asdict(self.timeouts),
                "phase": "comparison_fixture_process", "semantic_latency": "unmeasured",
            })
            deadline = time.monotonic() + self.timeouts.connect
            while True:
                self.listener.settimeout(min(0.1, _remaining(deadline)))
                try:
                    self.connection, _ = self.listener.accept()
                    break
                except socket.timeout:
                    if self.child.poll() is not None:
                        raise RuntimeError(f"comparison test child exited before connection: {self.child.returncode}")
            self.listener.close()
            self.listener = None
            channel = FramedChannel(self.connection, nonce, incoming=self.streams["received"], outgoing=self.streams["sent"])
            self.session = SocketSession(channel, self.invocation, self.timeouts)
        except BaseException as error:
            cleanup = self.close()
            raise RuntimeError(f"comparison fixture creation failed; artifacts: {self.artifacts}; cleanup: {cleanup['status']}") from error

    def replay(self, invocation, capture):
        if getattr(capture, "sink", None) is None or getattr(capture, "case_trial_id", None) != self.invocation["case_trial_id"]:
            raise ProtocolError("production connection requires the case's durable Capture sink")
        return self.session.replay(invocation, capture)

    def _signal_owned(self, sig):
        if self.child is None:
            return False
        # An unreaped Popen child cannot be replaced by an unrelated process.
        # Without a readable start identity, signal only that direct child.
        if self.identity is None and self.child.poll() is None:
            self.child.send_signal(sig)
            self.signals.append({"signal": sig.name, "status": "sent", "pid": self.child.pid,
                                 "scope": "direct_owned_child", "identity": None})
            return True
        identities = [self.identity] if self.identity else []
        if self.resources is not None:
            self.resources.sample()
            identities.extend(ProcessIdentity(**row["identity"]) for row in self.resources.report()["processes"]
                              if row["identity"] != asdict(self.identity))
        # A still-observed member anchors the original group after reparenting.
        # Escaped descendants remain owned by their recorded PID/start identity;
        # signal only that PID, never its new group or session.
        sent = group_sent = False
        for expected in identities:
            try:
                actual = read_process_identity(expected.pid)
                if actual != expected:
                    continue
                group = os.getpgid(actual.pid)
                if group == self.child.pid:
                    if group_sent:
                        continue
                    os.killpg(self.child.pid, sig)
                    self.signals.append({"signal": sig.name, "status": "sent", "anchor": asdict(actual),
                                         "process_group_id": self.child.pid})
                    group_sent = True
                elif actual != self.identity:
                    os.kill(actual.pid, sig)
                    self.signals.append({"signal": sig.name, "status": "sent", "identity": asdict(actual),
                                         "scope": "known_owned_descendant", "observed_process_group_id": group})
                else:
                    continue
                sent = True
            except (OSError, subprocess.SubprocessError):
                continue
        if not sent:
            self.signals.append({"signal": sig.name, "status": "unexecuted",
                                 "reason": "no matching observed owned identity remains available"})
        return sent

    def _observed_survivors(self):
        if self.resources is None:
            return []
        self.resources.sample()
        return [row for row in self.resources.report()["processes"]
                if row["identity"] != asdict(self.identity) and row["present_in_last_sample"]]

    def _abort_observed_descendants(self):
        for sig, duration in ((signal.SIGTERM, self.timeouts.terminate), (signal.SIGKILL, self.timeouts.kill)):
            if not self._observed_survivors():
                return
            self._signal_owned(sig)
            deadline = time.monotonic() + duration
            while self._observed_survivors() and time.monotonic() < deadline:
                time.sleep(min(0.02, max(0, deadline - time.monotonic())))

    def _join(self):
        if self.child is None:
            return {"status": "unexecuted", "reason": "no test child was spawned"}
        for duration, sig in ((self.timeouts.close, None), (self.timeouts.terminate, signal.SIGTERM), (self.timeouts.kill, signal.SIGKILL)):
            if sig is not None:
                self._signal_owned(sig)
            try:
                code = self.child.wait(timeout=duration)
                if self.resources is not None:
                    self.resources.record_exit(self.identity, code)
                return {"status": "completed", "pid": self.child.pid,
                        "identity": asdict(self.identity) if self.identity else None,
                        "joined": True, "returncode": code}
            except subprocess.TimeoutExpired:
                continue
        return {"status": "unmeasured", "pid": self.child.pid, "joined": False,
                "reason": "owned test child did not exit within bounded cleanup"}

    def close(self):
        if self.cleanup is not None:
            return copy.deepcopy(self.cleanup)
        host_cleanup, errors = None, []
        try:
            if self.session is not None:
                host_cleanup = self.session.close()
        except (OSError, EOFError, ValueError) as error:
            errors.append(f"host cleanup unavailable: {error}")
        finally:
            for stream in (self.connection, self.listener):
                if stream is not None:
                    stream.close()
            self.connection = self.listener = None
        joined = self._join()
        if host_cleanup is not None and host_cleanup.get("root") != (asdict(self.identity) if self.identity else None):
            errors.append("host cleanup root differs from the owned test child identity")
        survivors = self._observed_survivors()
        if survivors:
            errors.append("owned descendants remained observable after the test child joined")
        if host_cleanup is None or host_cleanup.get("status") != "completed" or host_cleanup.get("remaining_owned_children") != 0 or survivors:
            self._abort_observed_descendants()
        refs = {}
        if self.resources is not None:
            report = self.resources.stop()
            try:
                refs["process_capture"] = _write_json(self.artifacts / "connector-process-capture.json", report)
            except OSError as error:
                errors.append(f"process capture artifact failed: {error}")
        for name, stream in self.streams.items():
            try:
                _sync(stream)
                stream.close()
                refs[name] = _artifact(self.artifacts / f"connector-{name}.bin")
            except OSError as error:
                errors.append(f"{name} artifact failed: {error}")
            finally:
                stream.close()
        if self.socket_root is not None:
            try:
                (self.socket_root / "control.sock").unlink(missing_ok=True)
                self.socket_root.rmdir()
            except OSError as error:
                errors.append(f"owned socket removal failed: {error}")
        # The helper must prove its own descendants ended. Reaping the Python-
        # owned test child, or a ps sample with no children, cannot supply that.
        cleanup = copy.deepcopy(host_cleanup) if host_cleanup is not None else {
            "status": "unmeasured", "remaining_owned_children": None,
            "reason": "no completed host cleanup frame",
        }
        if joined.get("status") != "completed" or joined.get("returncode") != 0 or errors:
            cleanup = {"status": "unmeasured", "remaining_owned_children": None,
                       "reason": "connector cleanup or test child exit was incomplete",
                       "host_cleanup": host_cleanup}
        cleanup["connector"] = {"test_child": joined, "signals": self.signals,
                                "artifact_directory": str(self.artifacts), "artifacts": refs, "errors": errors}
        # This snapshot accounts only for connector artifacts, not provider DB,
        # snapshots or model/cache storage, whose paths belong to the host owner.
        cleanup["connector"]["artifact_storage"] = capture_directories({
            "connector_artifacts": sorted(self.artifacts.glob("connector-*"))})
        self.cleanup = cleanup
        try:
            _write_json(self.artifacts / "connector-cleanup.json", cleanup)
        except OSError as error:
            cleanup["status"], cleanup["remaining_owned_children"] = "unmeasured", None
            errors.append(f"cleanup artifact failed: {error}")
        return copy.deepcopy(cleanup)


class ProductionFactory:
    def __init__(self, executable, artifact_root, *, timeouts=Timeouts()):
        self.executable, self.artifact_root = Path(executable), Path(artifact_root)
        self.timeouts = timeouts
        if not self.executable.is_absolute() or not self.executable.is_file() or not os.access(self.executable, os.X_OK):
            raise ValueError("comparison test executable must be an existing absolute executable file")
        if not self.artifact_root.is_absolute() or not self.artifact_root.is_dir():
            raise ValueError("comparison artifact root must be an existing absolute directory")

    def create(self, invocation):
        return ProductionFixture(invocation, self.executable, self.artifact_root, self.timeouts)


def factory(metadata):
    """Bind explicit reviewed executable and artifact ownership; never choose pins."""
    del metadata  # Actual selection and build evidence must come from the child.
    try:
        return ProductionFactory(os.environ[EXECUTABLE_ENV], os.environ[ARTIFACT_ROOT_ENV])
    except KeyError as error:
        raise ValueError(f"missing connector configuration: {error.args[0]}") from error
