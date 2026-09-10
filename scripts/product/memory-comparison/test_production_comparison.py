"""Independent socket peers only; never execute the real host/model fixture."""

import contextlib
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

# Draft mirrors use the reviewed capture module from the active checkout.
HERE = Path(__file__).resolve().parent
REPO = next(parent for parent in HERE.parents if (parent / "scripts/product/memory-comparison/capture.py").is_file())
sys.path.insert(0, str(REPO / "scripts/product/memory-comparison"))
sys.path.insert(0, str(HERE))

from event_log import JsonlEventSink
from runner import Capture
from production_comparison import (FramedChannel, MAX_FRAME_BYTES, PROTOCOL, ProcessIdentity,
                                   ProductionFactory, ProductionFixture, ProtocolError, SocketSession,
                                   Timeouts, read_process_identity)


NONCE = "9a" * 32
INVOCATION = {"case_trial_id": "independent/transport", "case": {"id": "independent"},
              "actions": [{"action_id": "independent/0", "step": {"action": "recall", "query_id": "development"},
                           "query": {"id": "development", "text": "independent development question", "limit": 1}, "source": None}]}
METADATA = {"mode": "controlled_preparation", "selected_provider": None,
            "reason": "independent socket test has no host", "opaque": "é\n  retained"}
ACTION = {"input": INVOCATION["actions"][0], "status": "unexecuted", "terminal": None,
          "delivery": None, "provider_contacted": None, "reason": "no provider in transport test"}
FAST = Timeouts(connect=0.4, frame=0.4, case=1.5, close=0.15, terminate=0.1, kill=0.4)


def wire(kind, body=None, index=None, *, nonce=NONCE):
    # Deliberately different JSON spacing/key ordering from the adapter encoder.
    value = {"body": body, "kind": kind, "action_index": index, "nonce": nonce, "protocol": PROTOCOL}
    payload = (json.dumps(value, ensure_ascii=True, indent=2) + " \n").encode()
    return struct.pack(">Q", len(payload)) + payload, payload


def exact(stream, count):
    result = b""
    while len(result) < count:
        piece = stream.recv(count - len(result))
        if not piece:
            raise EOFError("independent peer reached EOF")
        result += piece
    return result


def receive(stream):
    size = struct.unpack(">Q", exact(stream, 8))[0]
    return json.loads(exact(stream, size))


def emit(stream, kind, body=None, index=None, *, nonce=NONCE):
    framed, payload = wire(kind, body, index, nonce=nonce)
    stream.sendall(framed)
    return payload


@contextlib.contextmanager
def peer_pair(callback):
    parent, child = socket.socketpair()
    child.settimeout(2)
    failures = []

    def run():
        try:
            callback(child)
        except BaseException as error:
            failures.append(error)
        finally:
            child.close()

    worker = threading.Thread(target=run)
    worker.start()
    try:
        yield parent
    finally:
        parent.close()
        worker.join(timeout=3)
        if worker.is_alive():
            raise AssertionError("independent socket peer did not join")
        if failures:
            raise failures[0]


class FramingTests(unittest.TestCase):
    def test_fragmented_utf8_keeps_exact_payload_and_raw_frame(self):
        framed, payload = wire("metadata", METADATA)

        def peer(stream):
            for index in range(0, len(framed), 7):
                stream.sendall(framed[index:index + 7])

        with tempfile.TemporaryDirectory() as directory, peer_pair(peer) as stream:
            path = Path(directory) / "wire.bin"
            with path.open("xb") as incoming:
                frame, actual = FramedChannel(stream, NONCE, incoming=incoming).receive(time.monotonic() + 1)
            self.assertEqual(actual, payload)
            self.assertEqual(frame["body"], METADATA)
            self.assertEqual(path.read_bytes(), framed)

    def test_oversize_and_zero_declarations_fail_before_reading_payload(self):
        for declared in (0, MAX_FRAME_BYTES + 1, 2**64 - 1):
            with self.subTest(declared=declared):
                parent, child = socket.socketpair()
                try:
                    child.sendall(struct.pack(">Q", declared))
                    with self.assertRaisesRegex(ProtocolError, "frame length"):
                        FramedChannel(parent, NONCE).receive(time.monotonic() + 0.1)
                finally:
                    parent.close()
                    child.close()

    def test_truncated_payload_keeps_received_bytes(self):
        def peer(stream):
            stream.sendall(struct.pack(">Q", 99) + b"partial")

        with tempfile.TemporaryDirectory() as directory, peer_pair(peer) as stream:
            path = Path(directory) / "partial.bin"
            with path.open("xb") as incoming:
                with self.assertRaises(EOFError):
                    FramedChannel(stream, NONCE, incoming=incoming).receive(time.monotonic() + 1)
            self.assertEqual(path.read_bytes(), struct.pack(">Q", 99) + b"partial")

    def test_rejects_wrong_nonce_duplicate_members_nonfinite_and_invalid_utf8(self):
        _, valid = wire("ready")
        invalids = [valid.replace(NONCE.encode(), b"0" * 64), b'{"kind":"a","kind":"b"}',
                    b'{"n":NaN}', b'{"n":1e999}', b'"\xff"']
        for payload in invalids:
            with self.subTest(payload=payload[:40]):
                def peer(stream):
                    stream.sendall(struct.pack(">Q", len(payload)) + payload)
                with peer_pair(peer) as stream:
                    with self.assertRaises((ProtocolError, UnicodeDecodeError)):
                        FramedChannel(stream, NONCE).receive(time.monotonic() + 1)

    def test_partial_frame_does_not_reset_absolute_deadline(self):
        def peer(stream):
            try:
                for byte in struct.pack(">Q", 99) + b"slow":
                    stream.sendall(bytes([byte]))
                    time.sleep(0.015)
            except (BrokenPipeError, ConnectionResetError):
                pass
        with peer_pair(peer) as stream:
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                FramedChannel(stream, NONCE).receive(started + 0.04)
            self.assertLess(time.monotonic() - started, 0.15)


class SessionTests(unittest.TestCase):
    def handshake(self, stream):
        hello = receive(stream)
        self.assertEqual(hello, {"protocol": PROTOCOL, "nonce": NONCE, "kind": "hello"})
        emit(stream, "ready")
        request = receive(stream)
        self.assertEqual(request["kind"], "create")
        self.assertEqual(request["body"], INVOCATION)

    def ack(self, stream, kind, payload, index=None):
        self.assertEqual(receive(stream), {"protocol": PROTOCOL, "nonce": NONCE, "kind": "ack",
                                          "ack_kind": kind, "action_index": index,
                                          "frame_sha256": hashlib.sha256(payload).hexdigest()})

    def test_durable_writes_precede_exact_byte_acks_and_explicit_close(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"

            def peer(stream):
                self.handshake(stream)
                metadata_bytes = emit(stream, "metadata", METADATA)
                self.ack(stream, "metadata", metadata_bytes)
                self.assertEqual(json.loads(path.read_text().splitlines()[0])["metadata"], METADATA)
                action_bytes = emit(stream, "action", ACTION, 0)
                self.ack(stream, "action", action_bytes, 0)
                self.assertEqual(json.loads(path.read_text().splitlines()[1])["result"], ACTION)
                emit(stream, "finish", {"case_trial_id": INVOCATION["case_trial_id"], "status": "unexecuted", "opaque": "retained"})
                self.assertEqual(receive(stream)["kind"], "close")
                emit(stream, "closed", {"status": "unmeasured", "remaining_owned_children": None, "opaque": ["actual", 3]})

            with path.open("x") as events, peer_pair(peer) as stream:
                capture = Capture(JsonlEventSink(events), INVOCATION["case_trial_id"])
                session = SocketSession(FramedChannel(stream, NONCE), INVOCATION, FAST)
                with mock.patch("event_log.os.fsync", wraps=os.fsync) as synced:
                    result = session.replay(INVOCATION, capture)
                    self.assertEqual(synced.call_count, 2)
                self.assertEqual(result["opaque"], "retained")
                self.assertEqual(capture.action_results, [ACTION])
                self.assertEqual(session.close()["opaque"], ["actual", 3])

    def test_action_before_metadata_has_no_ack(self):
        def peer(stream):
            self.handshake(stream)
            emit(stream, "action", ACTION, 0)
            self.assertEqual(stream.recv(1), b"")
        with peer_pair(peer) as stream:
            capture = Capture()
            session = SocketSession(FramedChannel(stream, NONCE), INVOCATION, FAST)
            with self.assertRaisesRegex(ProtocolError, "metadata"):
                session.replay(INVOCATION, capture)
            self.assertEqual(capture.action_results, [])
            with self.assertRaisesRegex(ProtocolError, "acknowledgment"):
                session.close()

    def test_skipped_repeated_boolean_indexes_and_input_drift_have_no_ack(self):
        changed_type = {**ACTION, "input": {**ACTION["input"], "query": {**ACTION["input"]["query"], "limit": True}}}
        for index, body, repeat in ((1, ACTION, False), (True, ACTION, False),
                                    (0, {**ACTION, "input": {"action_id": "changed"}}, False),
                                    (0, changed_type, False), (0, ACTION, True)):
            with self.subTest(index=index, repeat=repeat):
                def peer(stream):
                    self.handshake(stream)
                    self.ack(stream, "metadata", emit(stream, "metadata", METADATA))
                    if repeat:
                        self.ack(stream, "action", emit(stream, "action", ACTION, 0), 0)
                    emit(stream, "action", body, index)
                    self.assertEqual(stream.recv(1), b"")
                with peer_pair(peer) as stream:
                    capture = Capture()
                    session = SocketSession(FramedChannel(stream, NONCE), INVOCATION, FAST)
                    with self.assertRaises(ProtocolError):
                        session.replay(INVOCATION, capture)
                    self.assertEqual(len(capture.action_results), int(repeat))

    def test_durable_event_limit_is_not_widened_to_transport_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sent = wire("metadata", METADATA)[0]

            def peer(stream):
                self.handshake(stream)
                stream.sendall(sent)
                self.assertEqual(stream.recv(1), b"")

            with (root / "events").open("x") as events, (root / "wire").open("xb") as raw, peer_pair(peer) as stream:
                capture = Capture(JsonlEventSink(events), INVOCATION["case_trial_id"])
                session = SocketSession(FramedChannel(stream, NONCE, incoming=raw), INVOCATION, FAST)
                with mock.patch("event_log.MAX_EVENT_LINE_BYTES", 64):
                    with self.assertRaisesRegex(ValueError, "bounded line size"):
                        session.replay(INVOCATION, capture)
                self.assertTrue((root / "wire").read_bytes().endswith(sent))
                self.assertEqual((root / "events").read_bytes(), b"")

    def test_capture_write_error_does_not_ack_or_retry(self):
        def peer(stream):
            self.handshake(stream)
            self.ack(stream, "metadata", emit(stream, "metadata", METADATA))
            emit(stream, "action", ACTION, 0)
            self.assertEqual(stream.recv(1), b"")
        with peer_pair(peer) as stream:
            capture = Capture()
            session = SocketSession(FramedChannel(stream, NONCE), INVOCATION, FAST)
            with mock.patch.object(capture, "record_action", side_effect=OSError("independent disk failure")):
                with self.assertRaisesRegex(OSError, "disk failure"):
                    session.replay(INVOCATION, capture)
            self.assertEqual(capture.action_results, [])
            with self.assertRaisesRegex(ProtocolError, "twice"):
                session.replay(INVOCATION, capture)

    def test_premature_finish_preserves_missing_actions_without_fabrication(self):
        for status in ("unexecuted", "censored"):
            with self.subTest(status=status):
                def peer(stream):
                    self.handshake(stream)
                    self.ack(stream, "metadata", emit(stream, "metadata", METADATA))
                    emit(stream, "finish", {"case_trial_id": INVOCATION["case_trial_id"], "status": status, "reason": "actual unavailable"})
                with peer_pair(peer) as stream:
                    capture = Capture()
                    result = SocketSession(FramedChannel(stream, NONCE), INVOCATION, FAST).replay(INVOCATION, capture)
                    self.assertEqual(capture.action_results, [])
                    self.assertEqual(result["status"], status)
                    self.assertNotIn("actions", result)

    def test_early_completed_or_unexplained_finish_is_rejected(self):
        for status, reason in (("completed", "unexpected stop"), ("unexecuted", None),
                               ("censored", " \n"), ("unknown", "unexpected stop")):
            with self.subTest(status=status, reason=reason):
                def peer(stream):
                    self.handshake(stream)
                    self.ack(stream, "metadata", emit(stream, "metadata", METADATA))
                    emit(stream, "finish", {"case_trial_id": INVOCATION["case_trial_id"], "status": status, "reason": reason})
                    self.assertEqual(stream.recv(1), b"")
                with peer_pair(peer) as stream:
                    session = SocketSession(FramedChannel(stream, NONCE), INVOCATION, FAST)
                    with self.assertRaisesRegex(ProtocolError, "early finish"):
                        session.replay(INVOCATION, Capture())
                    self.assertFalse(session.finished)
                    self.assertTrue(session.broken)


FAKE_EXECUTABLE = r'''
import hashlib, json, os, signal, socket, struct, subprocess, sys, time
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(os.environ["TRACEDECAY_HOST_COMPARISON_SOCKET"])
n = os.environ["TRACEDECAY_HOST_COMPARISON_NONCE"]
p = "tracedecay.host-comparison.v1"
def take(count):
    value = b""
    while len(value) < count:
        part = s.recv(count - len(value))
        if not part: raise SystemExit(4)
        value += part
    return value
def receive():
    return json.loads(take(struct.unpack(">Q", take(8))[0]))
def send(kind, body=None):
    raw = json.dumps(dict(protocol=p, nonce=n, kind=kind, action_index=None, body=body), indent=1).encode()
    s.sendall(struct.pack(">Q", len(raw)) + raw)
    return hashlib.sha256(raw).hexdigest()
assert receive()["kind"] == "hello"
if os.environ.get("INDEPENDENT_SOCKET_TEST_MODE") == "detached":
    child = subprocess.Popen([sys.executable, "-S", "-c", "import time; time.sleep(60)"],
                             start_new_session=True, stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    root = os.environ["TRACEDECAY_HOST_COMPARISON_ARTIFACT_ROOT"]
    with open(os.path.join(root, "independent-detached-pid"), "x") as out: out.write(str(child.pid))
    code = child.wait()
    with open(os.path.join(root, "independent-detached-exit"), "x") as out: out.write(str(code))
    raise SystemExit(4)
if os.environ.get("INDEPENDENT_SOCKET_TEST_MODE") == "hang":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True: time.sleep(1)
send("ready")
invocation = receive()["body"]
digest = send("metadata", dict(mode="controlled_preparation", selected_provider=None, reason="independent peer has no host"))
assert receive()["frame_sha256"] == digest
send("finish", dict(case_trial_id=invocation["case_trial_id"], status="unexecuted", reason="no host"))
assert receive()["kind"] == "close"
start = subprocess.check_output(["/bin/ps", "-p", str(os.getpid()), "-o", "lstart="], env=dict(os.environ, LC_ALL="C"), text=True).strip()
root = dict(pid=os.getpid(), start_identity=" ".join(start.split()))
if os.environ.get("INDEPENDENT_SOCKET_TEST_MODE") == "wrong_root": root["pid"] += 1
send("closed", dict(status="completed", remaining_owned_children=0, root=root, direct_child_exits=[]))
os.write(1, b"independent stdout\x00\n")
os.write(2, b"independent stderr\xff\n")
s.close()
'''


class OwnedTestChildTests(unittest.TestCase):
    def make_factory(self, root):
        executable = root / "independent-peer"
        executable.write_text(f"#!{sys.executable} -S\n" + FAKE_EXECUTABLE)
        executable.chmod(0o700)
        return ProductionFactory(executable, root, timeouts=FAST)

    @contextlib.contextmanager
    def capture(self, root):
        with (root / "independent-events.jsonl").open("x") as stream:
            yield Capture(JsonlEventSink(stream), INVOCATION["case_trial_id"])

    def test_owned_child_join_and_binary_artifacts_survive_close(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = self.make_factory(root).create(INVOCATION)
            try:
                with self.capture(root) as capture:
                    result = fixture.replay(INVOCATION, capture)
                    self.assertEqual(result["reason"], "no host")
            finally:
                cleanup = fixture.close()
            self.assertEqual(cleanup["status"], "completed")
            self.assertTrue(cleanup["connector"]["test_child"]["joined"])
            self.assertEqual(cleanup["connector"]["test_child"]["returncode"], 0)
            self.assertEqual((fixture.artifacts / "connector-stdout.bin").read_bytes(), b"independent stdout\x00\n")
            self.assertEqual((fixture.artifacts / "connector-stderr.bin").read_bytes(), b"independent stderr\xff\n")
            self.assertEqual(fixture.close(), cleanup)
            self.assertFalse(fixture.socket_root.exists())
            self.assertTrue((fixture.artifacts / "connector-process-capture.json").is_file())
            self.assertEqual(json.loads((fixture.artifacts / "connector-invocation.json").read_text()), INVOCATION)

    def test_wrong_cleanup_root_cannot_claim_success(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ, {"INDEPENDENT_SOCKET_TEST_MODE": "wrong_root"}):
            root = Path(directory)
            fixture = self.make_factory(root).create(INVOCATION)
            try:
                with self.capture(root) as capture:
                    fixture.replay(INVOCATION, capture)
            finally:
                cleanup = fixture.close()
            self.assertEqual(cleanup["status"], "unmeasured")
            self.assertEqual(cleanup["host_cleanup"]["status"], "completed")
            self.assertIn("root differs", " ".join(cleanup["connector"]["errors"]))

    def test_timeout_retains_unavailable_host_cleanup_and_reaps_owned_child(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ, {"INDEPENDENT_SOCKET_TEST_MODE": "hang"}):
            root = Path(directory)
            fixture = self.make_factory(root).create(INVOCATION)
            try:
                with self.capture(root) as capture:
                    with self.assertRaises(TimeoutError):
                        fixture.replay(INVOCATION, capture)
            finally:
                cleanup = fixture.close()
            self.assertEqual(cleanup["status"], "unmeasured")
            self.assertIsNone(cleanup["remaining_owned_children"])
            self.assertTrue(cleanup["connector"]["test_child"]["joined"])
            self.assertIn("SIGKILL", [item["signal"] for item in cleanup["connector"]["signals"]])
            self.assertFalse(fixture.socket_root.exists())
            self.assertTrue((fixture.artifacts / "connector-cleanup.json").is_file())

    def test_creation_failure_retains_launch_output_and_joins_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "failing-independent-peer"
            executable.write_text(f"#!{sys.executable} -S\nimport os\nos.write(2, b'early failure')\nraise SystemExit(7)\n")
            executable.chmod(0o700)
            factory = ProductionFactory(executable, root, timeouts=FAST)
            with self.assertRaisesRegex(RuntimeError, "creation failed; artifacts:"):
                factory.create(INVOCATION)
            artifact_root, = root.glob("case-*")
            self.assertEqual((artifact_root / "connector-stderr.bin").read_bytes(), b"early failure")
            cleanup = json.loads((artifact_root / "connector-cleanup.json").read_text())
            self.assertEqual(cleanup["status"], "unmeasured")
            self.assertTrue(cleanup["connector"]["test_child"]["joined"])
            self.assertEqual(cleanup["connector"]["test_child"]["returncode"], 7)

    def test_production_wrapper_refuses_non_durable_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.make_factory(Path(directory)).create(INVOCATION)
            try:
                with self.assertRaisesRegex(ProtocolError, "durable Capture"):
                    fixture.replay(INVOCATION, Capture())
            finally:
                cleanup = fixture.close()
            self.assertEqual(cleanup["status"], "unmeasured")
            self.assertTrue(cleanup["connector"]["test_child"]["joined"])

    def test_detached_owned_descendant_is_terminated_without_signaling_unrelated_child(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ, {"INDEPENDENT_SOCKET_TEST_MODE": "detached"}):
            root = Path(directory)
            unrelated = subprocess.Popen([sys.executable, "-S", "-c", "import time; time.sleep(60)"],
                                         start_new_session=True, stdin=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            fixture = detached = None
            try:
                fixture = self.make_factory(root).create(INVOCATION)
                with self.capture(root) as capture:
                    with self.assertRaises(TimeoutError):
                        fixture.replay(INVOCATION, capture)
                pid = int((fixture.artifacts / "independent-detached-pid").read_text())
                detached = read_process_identity(pid)
                self.assertEqual(os.getpgid(pid), pid)
                self.assertNotEqual(os.getpgid(pid), fixture.child.pid)
                cleanup = fixture.close()
                self.assertEqual(cleanup["status"], "unmeasured")
                self.assertTrue(cleanup["connector"]["test_child"]["joined"])
                self.assertEqual((fixture.artifacts / "independent-detached-exit").read_text(), str(-signal.SIGTERM))
                self.assertTrue(any(item.get("scope") == "known_owned_descendant" and
                                    item.get("identity") == {"pid": detached.pid, "start_identity": detached.start_identity}
                                    for item in cleanup["connector"]["signals"]))
                self.assertIsNone(unrelated.poll())
                with self.assertRaises(OSError):
                    read_process_identity(detached.pid)
            finally:
                if fixture is not None:
                    fixture.close()
                if detached is not None:
                    try:
                        if read_process_identity(detached.pid) == detached:
                            os.kill(detached.pid, signal.SIGKILL)
                    except OSError:
                        pass
                unrelated.terminate()
                unrelated.wait(timeout=2)

    def test_replaced_known_identities_are_never_signaled(self):
        fixture = object.__new__(ProductionFixture)
        fixture.child = mock.Mock(pid=123)
        fixture.identity = ProcessIdentity(123, "original root")
        fixture.resources = mock.Mock()
        fixture.resources.report.return_value = {"processes": [
            {"identity": {"pid": 123, "start_identity": "original root"}},
            {"identity": {"pid": 456, "start_identity": "original descendant"}}]}
        fixture.signals = []
        with mock.patch("production_comparison.read_process_identity", side_effect=lambda pid: ProcessIdentity(pid, "replacement")), \
                mock.patch("production_comparison.os.kill") as kill, \
                mock.patch("production_comparison.os.killpg") as killpg:
            self.assertFalse(fixture._signal_owned(signal.SIGTERM))
            kill.assert_not_called()
            killpg.assert_not_called()

    def test_factory_rejects_missing_or_relative_ownership_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                ProductionFactory("relative-executable", directory)
            with self.assertRaises(ValueError):
                ProductionFactory(sys.executable, "relative-artifacts")
        with self.assertRaises(ValueError):
            Timeouts(frame=0)


if __name__ == "__main__":
    unittest.main()
