"""Durable append-only comparison events and bounded interrupted-tail recovery."""

import copy
import json
import os

EVENT_FORMAT = "tracedecay.host-comparison.events.v1"
MAX_EVENT_LINE_BYTES = 16 * 1024 * 1024


class JsonlEventSink:
    """A completed append is flushed and fsynced before the next action starts."""

    def __init__(self, stream):
        self.stream = stream
        self.failed = False

    def __call__(self, event):
        if self.failed:
            raise OSError("comparison event sink previously failed")
        encoded = json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n"
        if len(encoded.encode("utf-8")) > MAX_EVENT_LINE_BYTES:
            raise ValueError("comparison event exceeds bounded line size")
        try:
            self.stream.write(encoded)
            self.stream.flush()
            os.fsync(self.stream.fileno())
        except (OSError, ValueError):
            self.failed = True
            raise


def event(kind, case_trial_id, **payload):
    return {"format": EVENT_FORMAT, "event": kind, "case_trial_id": case_trial_id, **payload}


def _merge_actions(recorded, final):
    """A final case cannot erase or rewrite a durably observed action."""
    by_id = {row["input"]["action_id"]: row for row in final}
    if len(by_id) != len(final):
        raise ValueError("duplicate action in final case")
    for row in recorded:
        action_id = row["input"]["action_id"]
        if action_id in by_id and by_id[action_id] != row:
            raise ValueError("final case rewrote a durable action")
        if action_id not in by_id:
            final.append(copy.deepcopy(row))
            by_id[action_id] = row
    return final


def read_captures(path):
    """Read legacy case rows or event streams; only an unfinished tail is ignored.

    Corrupt complete lines and oversized tails fail explicitly. Unfinished cases
    retain every durable action with its original terminal, status and timing;
    only actions without a durable result remain absent for schedule recovery.
    """
    states, order, legacy, tail_bytes = {}, [], [], 0
    with path.open("rb") as stream:
        while True:
            line = stream.readline(MAX_EVENT_LINE_BYTES + 1)
            if not line:
                break
            if len(line) > MAX_EVENT_LINE_BYTES:
                raise ValueError("comparison event or interrupted tail exceeds bounded line size")
            if not line.endswith(b"\n"):
                tail_bytes = len(line)
                break
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("format") != EVENT_FORMAT:
                if states:
                    raise ValueError("cannot mix case rows with comparison events")
                legacy.append(row)
                continue
            if legacy:
                raise ValueError("cannot mix comparison events with case rows")
            case_id, kind = row["case_trial_id"], row["event"]
            if case_id not in states:
                states[case_id] = {"actions": [], "metadata": {}, "begin": None, "finish": None}
                order.append(case_id)
            state = states[case_id]
            if state["finish"] is not None:
                raise ValueError("event follows finished case")
            if kind == "case_begin":
                if state["begin"] is not None:
                    raise ValueError("duplicate case begin/retry")
                state["begin"] = row["invocation"]
            elif kind == "case_metadata":
                if state["metadata"] or state["actions"]:
                    raise ValueError("case metadata changed after recording began")
                state["metadata"] = row["metadata"]
            elif kind == "action":
                result = row["result"]
                if any(a["input"]["action_id"] == result["input"]["action_id"] for a in state["actions"]):
                    raise ValueError("duplicate durable action/retry")
                state["actions"].append(result)
            elif kind == "case_finish":
                final = row["result"]
                if final["case_trial_id"] != case_id:
                    raise ValueError("finished case identity mismatch")
                for field, recorded in state["metadata"].items():
                    if field in final and final[field] != recorded:
                        raise ValueError(f"final case rewrote durable metadata field: {field}")
                    final[field] = copy.deepcopy(recorded)
                if state["begin"] is not None:
                    if "requested_invocation" in final and final["requested_invocation"] != state["begin"]:
                        raise ValueError("final case rewrote durable requested invocation")
                    final["requested_invocation"] = copy.deepcopy(state["begin"])
                final["actions"] = _merge_actions(state["actions"], final.get("actions", []))
                state["finish"] = final
            else:
                raise ValueError("unknown comparison event")
    captures = list(legacy)
    for case_id in order:
        state = states[case_id]
        if state["finish"] is not None:
            captures.append(state["finish"])
        else:
            captures.append({**state["metadata"], "case_trial_id": case_id, "status": "unexecuted",
                             "reason": "capture process ended before durable case finish/cleanup",
                             "actions": state["actions"], "requested_invocation": state["begin"],
                             "cleanup": {"status": "unmeasured", "reason": "no durable case finish"},
                             "stream_recovery": {"durable_actions": len(state["actions"]),
                                                 "unterminated_final_line_bytes": tail_bytes}})
    if tail_bytes and captures:
        captures[-1]["stream_recovery"] = {
            **captures[-1].get("stream_recovery", {}), "unterminated_final_line_bytes": tail_bytes,
            "reason": "bounded incomplete final line excluded; complete preceding events retained"}
    return captures
