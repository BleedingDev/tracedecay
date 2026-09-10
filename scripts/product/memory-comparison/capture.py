"""Stdlib measurement primitives for caller-owned host comparison attempts.

The runner owns process creation, readiness, deadlines, semantic outcomes and
reaping. Create an AttemptTiming for each directly instrumented phase. Obtain a
ProcessIdentity immediately after spawning an owned child, then sample a
ProcessTreeCapture manually or use start()/stop() around that phase. stop() only
stops collection; it never signals the workload. All reports are JSON values.

ps RSS is KiB; macOS time -l maximum RSS is bytes, matching ncm/benchmark/run.py.
That script's parser is embedded in a benchmark launcher, so its small regex is
reused here without importing or invoking its launcher. time -l reports command
rusage, not a simultaneous tree peak or an automatically attributable worker peak.
ps lstart identity has one-second resolution: reuse within the same second is
indistinguishable. Short-lived children can escape sampling; sampled peaks are
lower bounds, and shared pages may be counted in multiple processes' RSS.
"""

from __future__ import annotations

import math
import os
import re
import stat
import subprocess
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping


class AttemptTiming:
    """One span, including failed terminals, unexecuted rows and right censoring.

    Extra trial/host/lane/budget/effect fields belong to the runner's measurement
    row. finish() means an observed terminal; censor() means no terminal observed.
    unmeasured() preserves a previously recorded semantic terminal on clock failure.
    """

    def __init__(self, phase: str, *, clock: Callable[[], int] = time.monotonic_ns):
        self.phase = phase
        self.clock = clock
        self.start_ns = self.end_ns = self.cutoff_ns = None
        self.terminal = None
        self.status = "unmeasured"
        self.reason = "not_started"

    def start(self, *, at_ns: int | None = None) -> None:
        if self.start_ns is not None or self.terminal is not None:
            raise ValueError("attempt already started or completed")
        self.start_ns = self.clock() if at_ns is None else at_ns
        self.status, self.reason = "recording", None

    def _boundary(self, at_ns: int | None) -> int:
        if self.status != "recording":
            raise ValueError("attempt is not recording")
        boundary = self.clock() if at_ns is None else at_ns
        if boundary < self.start_ns:
            raise ValueError("monotonic time moved backwards")
        return boundary

    def finish(self, terminal: str, *, at_ns: int | None = None) -> None:
        if not terminal:
            raise ValueError("an observed terminal is required")
        self.end_ns = self._boundary(at_ns)
        self.terminal, self.status, self.reason = terminal, "measured", None

    def censor(self, reason: str, *, at_ns: int | None = None) -> None:
        if not reason:
            raise ValueError("a censoring reason is required")
        self.cutoff_ns = self._boundary(at_ns)
        self.status, self.reason = "censored", reason

    def unmeasured(self, reason: str, *, terminal: str | None = None) -> None:
        if not reason:
            raise ValueError("a missing-measurement reason is required")
        self.status, self.reason = "unmeasured", reason
        if terminal is not None:
            self.terminal = terminal

    def report(self) -> dict:
        return {
            "phase": self.phase, "status": self.status, "reason": self.reason,
            "start_monotonic_ns": self.start_ns, "end_monotonic_ns": self.end_ns,
            "cutoff_monotonic_ns": self.cutoff_ns, "terminal": self.terminal,
            "elapsed_ns": (self.end_ns - self.start_ns) if self.status == "measured" else None,
            "elapsed_at_cutoff_ns": (self.cutoff_ns - self.start_ns) if self.status == "censored" else None,
        }


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    start_identity: str

    def __post_init__(self) -> None:
        if self.pid <= 0 or not self.start_identity:
            raise ValueError("positive PID and process start identity are required")


@dataclass(frozen=True)
class ProcessReading:
    identity: ProcessIdentity
    parent_pid: int
    rss_bytes: int | None
    cpu_seconds: float | None


def _cpu_seconds(value: str) -> float:
    days, _, rest = value.rpartition("-")
    parts = rest.split(":")
    if len(parts) not in (2, 3):
        raise ValueError("invalid ps CPU time")
    result = float(days or 0) * 86400
    for power, part in enumerate(reversed(parts)):
        result += float(part) * 60 ** power
    if not math.isfinite(result) or result < 0:
        raise ValueError("invalid ps CPU time")
    return result


def parse_ps(output: str) -> tuple[dict[int, ProcessReading], list[str]]:
    """Parse headerless `pid,ppid,lstart,rss,time` and report ancestry errors.

    Missing metric columns are None on otherwise usable process identities.
    """
    rows, errors, duplicates = {}, [], set()
    for number, line in enumerate(output.splitlines(), 1):
        if not line.strip():
            continue
        fields = line.split()
        try:
            if len(fields) != 9:
                raise ValueError("expected pid ppid and five lstart fields, rss, time")
            pid, parent = int(fields[0]), int(fields[1])
            identity = ProcessIdentity(pid, " ".join(fields[2:7]))
            if parent < 0:
                raise ValueError("negative parent PID")
        except ValueError as error:
            errors.append(f"ps line {number}: {error}")
            continue
        rss = cpu = None
        try:
            rss = int(fields[7]) * 1024
            if rss < 0:
                raise ValueError("negative RSS")
        except ValueError:
            rss = None
        try:
            cpu = _cpu_seconds(fields[8])
        except ValueError:
            cpu = None
        if pid in rows or pid in duplicates:
            duplicates.add(pid)
            rows.pop(pid, None)
            errors.append(f"ps PID {pid}: duplicate identity rows")
        else:
            rows[pid] = ProcessReading(identity, parent, rss, cpu)
    return rows, errors


def _ps(runner: Callable, *, pid: int | None = None, timeout: float = 1.0) -> str:
    selection = ["-p", str(pid)] if pid is not None else ["-A"]
    result = runner(
        ["/bin/ps", *selection, "-o", "pid=,ppid=,lstart=,rss=,time="],
        capture_output=True, text=True, check=False, timeout=timeout,
        env={**os.environ, "LC_ALL": "C"},
    )
    if result.returncode:
        raise OSError(f"ps exited {result.returncode}: {result.stderr.strip()}")
    return result.stdout


def read_process_identity(pid: int, *, runner: Callable = subprocess.run) -> ProcessIdentity:
    """Read only the PID supplied by its owner; never find workloads by name."""
    if pid <= 0:
        raise ValueError("positive owned PID required")
    rows, errors = parse_ps(_ps(runner, pid=pid))
    if pid not in rows:
        raise OSError("owned process identity unavailable: " + "; ".join(errors))
    return rows[pid].identity


def parse_time_l(stderr: str) -> dict:
    """macOS /usr/bin/time -l output only; absent fields stay unmeasured."""
    peak = re.search(r"^\s*(\d+)\s+maximum resident set size\s*$", stderr, re.MULTILINE)
    number = r"(\d+(?:\.\d+)?)"
    cpu = re.search(r"^\s*" + number + r"\s+real\s+" + number + r"\s+user\s+" + number + r"\s+sys\s*$", stderr, re.MULTILINE)
    return {
        "scope": "timed_command_rusage", "source": "macos_time_l",
        "peak_rss_bytes": int(peak.group(1)) if peak else None,
        "user_cpu_seconds": float(cpu.group(2)) if cpu else None,
        "system_cpu_seconds": float(cpu.group(3)) if cpu else None,
        "status": "measured" if peak and cpu else "partial" if peak or cpu else "unmeasured",
    }


class ProcessTreeCapture:
    """Observe an explicitly owned root and descendants, without process control.

    Numeric ps ancestry is enumerated, but only matching owned identities and
    their descendants enter reports. Previously observed children remain owned
    after reparenting. A replaced root PID cannot enroll another process tree.
    Each instance covers one phase; the caller controls its lifetime. sample()
    accepts fixture output, while runner injection exercises command failures.
    """

    def __init__(self, root: ProcessIdentity, phase: str, *, interval_seconds: float = 0.020,
                 runner: Callable = subprocess.run, clock: Callable[[], int] = time.monotonic_ns,
                 cpu_clock: Callable[[], float] = time.thread_time):
        if not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError("positive finite sample interval required")
        self.root, self.phase = root, phase
        self.interval_seconds = interval_seconds
        self.runner, self.clock, self.cpu_clock = runner, clock, cpu_clock
        self.samples: list[dict] = []
        self._known = {root}
        self._high_water: dict[ProcessIdentity, dict] = {}
        self._exits: dict[ProcessIdentity, dict] = {}
        self._lock, self._stop = threading.RLock(), threading.Event()
        self._thread = None

    def sample(self, *, ps_output: str | None = None) -> dict:
        with self._lock:
            started, cpu_started = self.clock(), self.cpu_clock()
            errors = []
            try:
                rows, errors = parse_ps(_ps(self.runner) if ps_output is None else ps_output)
            except (OSError, subprocess.SubprocessError) as error:
                rows, errors = {}, [f"ps collection failed: {error}"]
            owned = {pid: row for pid, row in rows.items() if row.identity in self._known}
            while True:
                children = {pid: row for pid, row in rows.items()
                            if pid not in owned and row.parent_pid in owned and row.identity != self.root}
                if not children:
                    break
                owned.update(children)
            self._known.update(row.identity for row in owned.values())
            root_row = rows.get(self.root.pid)
            root_status = ("present" if root_row and root_row.identity == self.root else
                           "replaced" if root_row else "unavailable")
            if root_status != "present":
                errors.append(f"owned root {root_status}")
            ancestry_complete = not errors and bool(owned)
            readings = [asdict(row) for _, row in sorted(owned.items())]
            rss = [row.rss_bytes for row in owned.values() if row.rss_bytes is not None]
            cpu = [row.cpu_seconds for row in owned.values() if row.cpu_seconds is not None]
            for pid, row in owned.items():
                if row.rss_bytes is None:
                    errors.append(f"ps PID {pid}: unavailable RSS")
                if row.cpu_seconds is None:
                    errors.append(f"ps PID {pid}: unavailable CPU time")
            complete = ancestry_complete and not errors
            finished = self.clock()
            previous = self.samples[-1]["start_monotonic_ns"] if self.samples else None
            gap = started - previous if previous is not None else None
            record = {
                "phase": self.phase, "start_monotonic_ns": started, "end_monotonic_ns": finished,
                "gap_ns": gap, "collection_duration_ns": finished - started,
                "overrun_ns": max(0, gap - round(self.interval_seconds * 1e9)) if gap is not None else None,
                "instrumentation_thread_cpu_seconds": self.cpu_clock() - cpu_started,
                "status": "measured" if complete else "partial" if owned else "unmeasured",
                "errors": errors, "root_status": root_status, "processes": readings,
                "tree_rss_bytes": sum(rss) if ancestry_complete and len(rss) == len(owned) else None,
                "observed_tree_rss_bytes": sum(rss) if rss else None,
                "tree_cpu_seconds": sum(cpu) if ancestry_complete and len(cpu) == len(owned) else None,
                "process_count": len(owned) if ancestry_complete else None,
                "child_count": len(owned) - 1 if ancestry_complete else None,
                "observed_process_count": len(owned),
                "absent_identities": [asdict(identity) for identity in sorted(
                    self._known - {row.identity for row in owned.values()},
                    key=lambda identity: (identity.pid, identity.start_identity))],
            }
            self.samples.append(record)
            return record

    def start(self) -> None:
        if self._thread is not None or self._stop.is_set():
            raise ValueError("collector already started or stopped")
        self.sample()
        self._thread = threading.Thread(target=self._poll, name="memory-resource-capture", daemon=True)
        self._thread.start()

    def _poll(self) -> None:
        while True:
            elapsed = (self.clock() - self.samples[-1]["start_monotonic_ns"]) / 1e9
            if self._stop.wait(max(0, self.interval_seconds - elapsed)):
                return
            self.sample()

    def stop(self) -> dict:
        """Stop only sampling, take a final sample, and return the phase report."""
        if self._stop.is_set():
            return self.report()
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self.sample()
        return self.report()

    def record_exit(self, identity: ProcessIdentity, returncode: int, *, at_ns: int | None = None) -> None:
        """Record an exit observed by the owner (ps disappearance is insufficient)."""
        with self._lock:
            if identity not in self._known:
                raise ValueError("exit identity was not observed in the owned tree")
            self._exits[identity] = {"returncode": returncode,
                                     "observed_monotonic_ns": self.clock() if at_ns is None else at_ns}

    def record_os_high_water(self, identity: ProcessIdentity, rss_bytes: int, *, source: str) -> None:
        """Attach an explicitly process-attributed OS counter, never a tree estimate.

        Command-wide time -l output is separate unless the caller can establish
        that its rusage belongs to this exact process identity.
        """
        with self._lock:
            if identity not in self._known or rss_bytes < 0 or not source:
                raise ValueError("observed identity, nonnegative RSS and counter source required")
            previous = self._high_water.get(identity)
            if previous is None or rss_bytes > previous["rss_bytes"]:
                self._high_water[identity] = {"rss_bytes": rss_bytes, "source": source}

    def report(self) -> dict:
        with self._lock:
            samples = list(self.samples)
            processes = []
            for identity in sorted(self._known, key=lambda item: (item.pid, item.start_identity)):
                series = [next((row for row in sample["processes"] if row["identity"] == asdict(identity)), None)
                          for sample in samples]
                rss = [row["rss_bytes"] for row in series if row and row["rss_bytes"] is not None]
                first, last = (series[0], series[-1]) if series else (None, None)
                processes.append({
                    "identity": asdict(identity), "role": "root" if identity == self.root else "descendant",
                    "start_rss_bytes": first["rss_bytes"] if first else None,
                    "end_rss_bytes": last["rss_bytes"] if last else None,
                    "sampled_peak_rss_bytes": max(rss) if rss else None,
                    "cpu_delta_seconds": paired_delta(last["cpu_seconds"], first["cpu_seconds"]) if first and last else None,
                    "observed_sample_count": sum(row is not None for row in series),
                    "os_high_water": self._high_water.get(identity), "exit": self._exits.get(identity),
                    "present_in_last_sample": last is not None if samples else None,
                })
            tree = [sample["tree_rss_bytes"] for sample in samples if sample["tree_rss_bytes"] is not None]
            observed = [sample["observed_tree_rss_bytes"] for sample in samples if sample["observed_tree_rss_bytes"] is not None]
            children = [sample["child_count"] for sample in samples if sample["child_count"] is not None]
            gaps = [sample["gap_ns"] for sample in samples if sample["gap_ns"] is not None]
            return {
                "phase": self.phase, "root": asdict(self.root), "nominal_interval_seconds": self.interval_seconds,
                "status": "unmeasured" if not samples or not observed else "measured" if all(
                    sample["status"] == "measured" for sample in samples) else "partial",
                "samples": samples, "sample_count": len(samples), "processes": processes,
                "start_tree_rss_bytes": samples[0]["tree_rss_bytes"] if samples else None,
                "end_tree_rss_bytes": samples[-1]["tree_rss_bytes"] if samples else None,
                "sampled_peak_tree_rss_bytes": max(tree) if tree else None,
                "sampled_peak_observed_tree_rss_bytes": max(observed) if observed else None,
                "sampled_tree_peak_is_lower_bound": True,
                "peak_child_count": max(children) if children else None,
                "end_child_count": samples[-1]["child_count"] if samples else None,
                "maximum_gap_ns": max(gaps) if gaps else None,
                "instrumentation_thread_cpu_seconds": sum(sample["instrumentation_thread_cpu_seconds"] for sample in samples) if samples else None,
                "instrumentation_ps_cpu_seconds": None,
                "limitations": ["ps start identity has one-second resolution",
                                "short-lived descendants may escape sampling",
                                "RSS may count shared pages in multiple processes",
                                "instrumentation CPU excludes ps subprocess CPU"],
            }


def paired_delta(value: int | float | None, baseline: int | float | None) -> int | float | None:
    """Difference of matched raw observations; never use on population percentiles."""
    return None if value is None or baseline is None else value - baseline


def capture_directories(categories: Mapping[str, Iterable[Path]], *,
                        clock: Callable[[], int] = time.monotonic_ns) -> dict:
    """Snapshot caller-supplied paths, without following symlinks.

    Count regular files and symlink entries, excluding directory metadata. Sparse
    allocation uses st_blocks * 512 when supplied by the OS. Each inode is counted
    once across the snapshot; the first category owns it, and later aliases name
    that category. Supply model/cache once as its own category. Missing roots or
    scan failures leave totals null and retain observed subtotals and reasons.
    The start/end timestamps bound a live traversal, not an atomic disk snapshot.
    """
    started = clock()
    seen: dict[tuple[int, int], str] = {}
    results = {}
    for category, roots in categories.items():
        logical = allocated = count = 0
        allocation_missing = False
        errors, shared = [], []
        pending = [Path(path) for path in roots]
        paths = [str(path) for path in pending]
        if not paths:
            errors.append("no paths supplied")
        while pending:
            path = pending.pop()
            try:
                info = path.lstat()
                key = (info.st_dev, info.st_ino)
                if key in seen:
                    shared.append({"path": str(path), "counted_in": seen[key]})
                    continue
                seen[key] = category
                if stat.S_ISDIR(info.st_mode):
                    with os.scandir(path) as entries:
                        pending.extend(Path(entry.path) for entry in entries)
                elif stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                    logical += info.st_size
                    blocks = getattr(info, "st_blocks", None)
                    if blocks is None:
                        allocation_missing = True
                    else:
                        allocated += blocks * 512
                    count += 1
                else:
                    errors.append(f"{path}: unsupported file type")
            except OSError as error:
                errors.append(f"{path}: {error}")
        results[category] = {
            "paths": paths, "status": "partial" if errors or allocation_missing else "measured",
            "logical_bytes": logical if not errors else None,
            "allocated_bytes": allocated if not errors and not allocation_missing else None,
            "observed_logical_bytes": logical, "observed_allocated_bytes": allocated,
            "file_count": count, "shared_entries": shared, "errors": errors,
            "allocation_missing": allocation_missing,
        }
    return {"start_monotonic_ns": started, "end_monotonic_ns": clock(), "categories": results}
