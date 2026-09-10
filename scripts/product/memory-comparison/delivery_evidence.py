"""Versioned, lossless ToolResult evidence. No host decisions are reconstructed.

Offsets name original UTF-8 bytes in one decoded text block. JSON member spans
include the original key/colon/value, never bytes from a reserialization. The
Rust evaluator independently recounts all recorded o200k observations.
Integer lexemes have exact [-2**63, 2**64-1] semantics; bare -0 normalizes to
integer 0. Fraction/exponent lexemes use finite IEEE-754 binary64 semantics.
Original numeric lexemes remain in the retained byte spans.
"""

import hashlib
import json
import re

TOKENIZER = {"identity": "tiktoken.o200k_base", "revision": "tiktoken-rs-0.12"}
NUMBER = re.compile(rb"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")
PRESENTATION_REVIEW = {"candidate_metadata": True, "shared_text": True, "source_attribution": True, "prohibited_claims": True}
STAGES = {"denied", "normalization_unavailable", "selection_unavailable", "deduplicated",
          "budget_excluded", "host_withheld", "selected", "pack_excluded", "injected"}


def check(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(text):
    return hashlib.sha256(text.encode("utf-8", errors="strict")).hexdigest()


def same(left, right):
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(same(left[k], right[k]) for k in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(same(a, b) for a, b in zip(left, right))
    return left == right


class LexicalJson:
    """Strict JSON parser retaining value and member spans by RFC 6901 pointer."""
    def __init__(self, text):
        self.data = text.encode("utf-8", errors="strict")
        check(len(self.data) <= 16 * 1024 * 1024, "JSON evidence exceeds byte bound")
        self.at, self.nodes, self.members = 0, {}, {}
        self.value = self._value("", 0)
        self._space()
        check(self.at == len(self.data), "trailing JSON bytes")

    def _space(self):
        while self.at < len(self.data) and self.data[self.at] in b" \t\r\n":
            self.at += 1

    def _string(self):
        start = self.at
        check(self.data[self.at:self.at + 1] == b'"', "JSON key/string expected")
        self.at += 1
        while self.at < len(self.data):
            byte = self.data[self.at]
            self.at += 1
            if byte == 92:
                self.at += 1
            elif byte == 34:
                value = json.loads(self.data[start:self.at])
                value.encode("utf-8", errors="strict")
                return value
        raise ValueError("unterminated JSON string")

    def _value(self, pointer, depth):
        check(depth <= 128, "JSON evidence exceeds depth bound")
        self._space()
        start, tag = self.at, self.data[self.at:self.at + 1]
        if tag == b'{':
            self.at += 1
            value = {}
            self._space()
            if self.data[self.at:self.at + 1] != b'}':
                while True:
                    self._space()
                    member_start = self.at
                    key = self._string()
                    check(key not in value, "duplicate JSON key")
                    child = pointer + "/" + key.replace("~", "~0").replace("/", "~1")
                    self._space()
                    check(self.data[self.at:self.at + 1] == b':', "JSON colon expected")
                    self.at += 1
                    value[key] = self._value(child, depth + 1)
                    self.members[child] = (member_start, self.at)
                    self._space()
                    if self.data[self.at:self.at + 1] != b',':
                        break
                    self.at += 1
            check(self.data[self.at:self.at + 1] == b'}', "JSON object end expected")
            self.at += 1
        elif tag == b'[':
            self.at += 1
            value = []
            self._space()
            if self.data[self.at:self.at + 1] != b']':
                while True:
                    value.append(self._value(pointer + "/" + str(len(value)), depth + 1))
                    self._space()
                    if self.data[self.at:self.at + 1] != b',':
                        break
                    self.at += 1
            check(self.data[self.at:self.at + 1] == b']', "JSON array end expected")
            self.at += 1
        elif tag == b'"':
            value = self._string()
        else:
            literal = next((word for word in (b'true', b'false', b'null')
                            if self.data.startswith(word, self.at)), None)
            if literal is not None:
                self.at += len(literal)
            else:
                match = NUMBER.match(self.data, self.at)
                check(match is not None, "invalid JSON value")
                self.at = match.end()
            value = json.loads(self.data[start:self.at])
            check(type(value) is not int or -(2**63) <= value <= 2**64 - 1,
                  "integer JSON lexeme outside shared exact domain")
            check(not isinstance(value, float) or abs(value) != float("inf"), "nonfinite JSON number")
        self.nodes[pointer] = (start, self.at, value)
        return value

    def span(self, pointer, member=False):
        start, end = self.members[pointer] if member else self.nodes[pointer][:2]
        return {"pointer": pointer, "start": start, "end": end,
                "sha256": sha(self.data[start:end].decode("utf-8")), "value": self.nodes[pointer][2]}

    def verify(self, span, member=False):
        expected = self.span(span["pointer"], member)
        check(all(type(span.get(k)) is type(v) and same(span[k], v) for k, v in expected.items()),
              "original JSON span, digest, or semantic pointer mismatch")
        return self.data[span["start"]:span["end"]].decode("utf-8")


def artifact(value):
    text = value["utf8"]
    check(isinstance(value.get("artifact_path"), str) and bool(value["artifact_path"]), "missing raw artifact reference")
    check(type(value.get("byte_length")) is int and value["byte_length"] == len(text.encode("utf-8"))
          and value.get("sha256") == sha(text), "raw artifact byte/digest mismatch")
    # Embedded exact bytes make metric bundles portable; the retained path is an
    # attribution reference. This verifier cannot authenticate the fixture.
    return LexicalJson(text)


def count(value):
    check(type(value) is int and value >= 0, "missing exact nonnegative token observation")
    return value


def is_tool_result(delivery):
    return delivery.get("representation") == "tool_result_v1"


def finally_emitted(candidate, delivery):
    return True if is_tool_result(delivery) else candidate["delivered"]


def canonical_sections(delivery):
    if not is_tool_result(delivery):
        return [s for s in delivery["sections"] if s["kind"] == "canonical"]
    # Exclude output offsets and advisory-dependent block positions from pairing;
    # compare each exact original canonical member plus source classification.
    return [{"pointer": s["pointer"], "sha256": s["sha256"], "text": s["text"],
             "source_refs": s["source_refs"], "authority": s["authority"], "section": s["section"]}
            for s in delivery["canonical_spans"]]


def validate_tool_result(delivery, lane):
    check(delivery["representation"] == "tool_result_v1", "unknown delivery representation")
    check(delivery["tokenizer"] == TOKENIZER, "tokenizer mismatch")
    check(not any(k in delivery for k in ("final_text", "final_sha256", "sections")), "mixed delivery representations")
    check(delivery["model_input_tokens"]["status"] == "unmeasured" and
          bool(delivery["model_input_tokens"]["reason"]), "model input boundary is unmeasured")
    carrier = artifact(delivery["raw_carrier"])
    projection = delivery["projection"]
    check(same(carrier.value, projection["result"]), "raw ToolResult/projection disagreement")
    content = carrier.value["content"]
    check(isinstance(content, list), "ToolResult content is not an array")
    expected = [{"content_index": i, "text": block["text"]} for i, block in enumerate(content)
                if block.get("type") == "text"]
    blocks = delivery["text_blocks"]
    check(same(expected, [{"content_index": b["content_index"], "text": b["text"]} for b in blocks])
          and same(expected, projection["text_blocks"]), "missing, reordered, or rewritten decoded text block")
    for block in blocks:
        check(block["sha256"] == sha(block["text"]) and type(block["utf8_bytes"]) is int
              and block["utf8_bytes"] == len(block["text"].encode()), "decoded text block bytes mismatch")
        count(block["tokens"])
    check(count(delivery["final_tokens"]) == sum(b["tokens"] for b in blocks) <= 128000,
          "full decoded block token sum exceeds quota or disagrees")
    index = projection["payload_content_index"]
    check(type(index) is int, "invalid payload content index")
    payload = next((b for b in blocks if b["content_index"] == index), None)
    check(payload is not None, "payload block missing")
    parser = LexicalJson(payload["text"])
    check(isinstance(parser.value, dict), "ToolResult v1 requires an object JSON payload")
    canonical = delivery["canonical_spans"]
    observed = projection["host_evidence"]
    check(len(canonical) == len(observed), "canonical projection coverage mismatch")
    expected_pointers = ["/" + k.replace("~", "~0").replace("/", "~1") for k in parser.value
                         if k != "advisory_provider_memory"]
    check([s["pointer"] for s in canonical] == expected_pointers, "canonical original member order/coverage mismatch")
    by_pointer = {s["pointer"]: s for s in observed}
    check(len(by_pointer) == len(observed), "duplicate canonical projection")
    canonical_text = ""
    for span in canonical:
        projected = by_pointer[span["pointer"]]
        text = parser.verify(span, member=True)
        check(span["text"] == text and span["content_index"] == index and
              projected["kind"] == "json_member" and same(projected["value"], span["value"])
              and span["authority"] == projected["authority"] and span["section"] == projected["section"]
              and isinstance(span["source_refs"], list), "canonical source/projection mismatch")
        canonical_text += text
    check(sha(canonical_text) == delivery["canonical_sha256"], "canonical original bytes digest mismatch")
    count(delivery["canonical_tokens"])
    # Every byte outside a complete top-level member is original JSON syntax.
    # Retain these gaps explicitly; full-block accounting charges them once.
    syntax, cursor = [], 0
    top = [parser.members["/" + k.replace("~", "~0").replace("/", "~1")] for k in parser.value]
    for start, end in [*top, (len(parser.data), len(parser.data))]:
        if start > cursor:
            text = parser.data[cursor:start].decode("utf-8")
            check(all(c in " \t\r\n{}," for c in text), "unattributed substantive payload bytes")
            syntax.append({"content_index": index, "start": cursor, "end": start, "text": text, "sha256": sha(text)})
        cursor = end
    check(same(syntax, delivery["payload_syntax_spans"]), "payload delimiter partition mismatch")
    advisory = parser.value.get("advisory_provider_memory")
    check("advisory_provider_memory" not in parser.value or isinstance(advisory, dict), "invalid advisory object")
    expected_advisory = []
    if advisory is not None:
        projected = projection["advisory"]
        check(projected["kind"] == "json_member" and projected["pointer"] == "/advisory_provider_memory"
              and same(projected["value"], advisory), "advisory projection mismatch")
        expected_advisory.append({"content_index": index, "attribution": "observed_advisory_member", **parser.span("/advisory_provider_memory", member=True)})
    else:
        check(projection["advisory"] is None, "missing observed advisory projection")
    # Every extra block is unclassified text, conservatively advisory-charged.
    # This cost classification does not assert provider authorship.
    for block in blocks:
        if block["content_index"] != index:
            expected_advisory.append({"content_index": block["content_index"], "attribution": "unclassified_text", "pointer": None, "start": 0,
                                      "end": block["utf8_bytes"], "sha256": block["sha256"], "value": block["text"]})
    expected_advisory.sort(key=lambda s: (s["content_index"], s["start"]))
    check(same(expected_advisory, delivery["advisory_spans"]), "advisory original span coverage mismatch")
    per_block = []
    for block in blocks:
        raw = block["text"].encode()
        text = b"".join(raw[s["start"]:s["end"]] for s in expected_advisory
                        if s["content_index"] == block["content_index"]).decode()
        per_block.append({"content_index": block["content_index"], "text": text, "sha256": sha(text)})
    check(same(per_block, [{k: b[k] for k in ("content_index", "text", "sha256")}
                          for b in delivery["advisory_blocks"]]), "advisory per-block projection mismatch")
    check(count(delivery["advisory_tokens"]) == sum(count(b["tokens"]) for b in delivery["advisory_blocks"]) <= 8192,
          "advisory per-block token sum exceeds quota or disagrees")
    # Length-prefixing binds the ordered block boundaries of a review document.
    review = b"".join(len(b["text"].encode()).to_bytes(8, "big") + b["text"].encode() for b in per_block)
    check(hashlib.sha256(review).hexdigest() == delivery["advisory_review_sha256"], "advisory review digest mismatch")
    emitted = advisory.get("candidates", []) if isinstance(advisory, dict) else []
    check(isinstance(emitted, list) and len(emitted) == len(delivery["candidates"]), "emitted candidate coverage mismatch")
    check(len({c["candidate_ref"] for c in delivery["candidates"]}) == len(emitted), "duplicate candidate ledger entry")
    for position, candidate in enumerate(delivery["candidates"]):
        pointer = f"/advisory_provider_memory/candidates/{position}"
        presentation = candidate["presentation_span"]
        check(presentation["pointer"] == pointer, "candidate order/presentation pointer mismatch")
        parser.verify(presentation)
        check(candidate["presentation_sha256"] == presentation["sha256"], "full candidate presentation digest mismatch")
        check(candidate["content"] == emitted[position]["content"] and sha(candidate["content"]) == candidate["content_sha256"],
              "finally emitted body mismatch")
        check(isinstance(candidate["sources"], list), "missing source attribution state")
        join = candidate["final_join"]
        check(join["status"] in ("bound", "unresolved"), "invalid final-output join")
        if join["status"] == "unresolved":
            check(bool(join.get("reason")), "unresolved final-output join lacks reason")
        else:
            # A host-authenticated retained-item binding is required. This draft
            # deliberately cannot derive it from a hardened candidate ID.
            validate_bound_join(delivery, candidate, emitted[position])
    count(delivery["candidate_body_tokens"])
    if lane == "no_memory":
        check(not emitted and advisory is None, "no-memory lane delivered provider advisory")
    if lane == "explicit_documentation":
        check(advisory is None, "documentation ToolResult assembly is not bound by provider JSON evidence")
    return canonical_sections(delivery)


def validate_bound_join(delivery, candidate, emitted):
    observed = emitted.get("provenance_evidence", {}).get("recall")
    check(isinstance(observed, dict), "missing observed candidate control locator")
    retained = delivery["retained_trace"]
    check(retained["status"] == "observed", "retained trace unavailable")
    trace = artifact(retained["artifact"]).value
    advisory = delivery["projection"]["advisory"]["value"]
    correlation = advisory["recall_trace"]
    check(same(retained["correlation"], correlation) and observed["trace_ref"] == correlation["trace_ref"],
          "candidate/retained trace correlation mismatch")
    check(trace["request_id"] == correlation["request_id"] and
          trace["provider_id"] == retained["provider_id"] == advisory["provider_id"] and
          type(trace["registration_revision"]) is int and
          same(trace["registration_revision"], retained["registration_revision"]) and
          same(trace["registration_revision"], advisory["registration_revision"]) and
          same(retained["delivery_scope"], advisory["canonical_history_replay"]["delivery_scope"]),
          "retained request/provider/revision/scope mismatch")
    check(retained.get("store_evidence") and retained.get("daemon_identity"), "missing retained host-read attribution")
    match = re.fullmatch(r"recall-trace-v1:([0-9a-f]{64}):([0-9a-f]{64})", observed["trace_ref"])
    check(match is not None and match.group(2) == trace["trace_id"], "retained trace identity mismatch")
    item = re.fullmatch(r"recall-item-v1:(0|[1-9][0-9]*)", observed["item_ref"])
    check(item is not None, "noncanonical retained item locator")
    rank = int(item.group(1))
    check(rank < len(trace["items"]) and type(trace["items"][rank]["provider_rank"]) is int
          and trace["items"][rank]["provider_rank"] == rank, "retained item rank mismatch")
    check(type(trace["requested_count"]) is int and trace["requested_count"] == len(trace["items"])
          and [i["provider_rank"] for i in trace["items"]] == list(range(len(trace["items"])))
          and all(i["stage"] in STAGES for i in trace["items"]), "retained trace partition mismatch")
    row = trace["items"][rank]
    join = candidate["final_join"]
    check(join["trace_ref"] == observed["trace_ref"] and join["item_ref"] == observed["item_ref"] and
          same(join["provider_rank"], rank) and join["retained_candidate_id"] == row["candidate_id"] and
          candidate["candidate_ref"] == row["candidate_id"] and join["compiled_stage"] == row["stage"] == "injected",
          "final output/compiled retained item join mismatch")
