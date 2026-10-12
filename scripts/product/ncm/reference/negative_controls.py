#!/usr/bin/env python3
"""Replay real baselines, then detect four required mutants and five defect controls.

Exit nonzero if a baseline disagrees OR any mutant survives its numeric assertion.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import unittest

import torch

import export_fixtures as oracle

REFERENCE = Path.home()/"workspace/bleedingdev/projects/biomem/code/biomem"
sys.path.insert(0, str(REFERENCE/"src"))
from memory_module.memory_centers import MemoryCenters
from memory_module.projections import ProjectionBundle
from memory_module.terrain_3d import Terrain3D
from memory_module.consolidation import SleepConsolidator

DEFAULT_ORACLE = oracle.ROOT/"product/ncm/reference/oracle"


def load(name, root=DEFAULT_ORACLE):
    return json.loads((root/f"{name}.json").read_text())


def restore_bank(state):
    # Copy explicit buffers rather than from_state_dict, whose legacy-identity
    # migration changes metadata when provenance is absent.
    obj = MemoryCenters(state["n_centers"], state["d_key"], state["d_value"])
    for key, value in state.items():
        if key in obj._buffers:
            obj._buffers[key].copy_(torch.tensor(value, dtype=obj._buffers[key].dtype))
        elif hasattr(obj, key):
            setattr(obj, key, value)
    return obj


def restore_terrain(state):
    obj = Terrain3D(state["resolution"], state["n_emotions"], state["alpha_h"], state["alpha_e"], state["leak"])
    obj.H.copy_(torch.tensor(state["H"]))
    obj.E.copy_(torch.tensor(state["E"]))
    return obj


def restore_projections(fixture):
    obj = ProjectionBundle().eval()
    with torch.no_grad():
        for name, module in obj.named_children():
            values = fixture["parameters"][name]
            module.proj.weight.copy_(torch.tensor(values["weight"]))
            if module.proj.bias is not None:
                module.proj.bias.copy_(torch.tensor(values["bias"]))
    return obj


def write_inputs(raw):
    tensors = {"keys", "values", "emotions", "intensities", "context_keys", "terrain_positions", "ages"}
    return {k: torch.tensor(v, dtype=torch.int64 if k == "ages" else torch.float32) if k in tensors else v
            for k, v in raw.items()}


def close(actual, expected, tolerance):
    a = torch.as_tensor(actual)
    e = torch.as_tensor(expected, dtype=a.dtype)
    torch.testing.assert_close(a, e, **tolerance)


class NegativeControls(unittest.TestCase):
    """Each test requires both a green real baseline and a red mutated execution."""

    root = DEFAULT_ORACLE

    def detected(self, actual, expected, tol, field):
        try:
            close(actual, expected, tol)
        except AssertionError:
            print(f"DETECTED {field}")
        else:
            self.fail(f"UNDETECTED mutation: {field}")

    def test_noop_blur(self):
        f = load("terrain", self.root)
        t = restore_terrain(f["after_step"])
        for field in ("H", "E"):
            expected = f["blur_corrected"][field]
            close(oracle.corrected_blur(getattr(t, field)), expected, f["tolerance"])
            # Executed reference no-op is the mutant, not another blur implementation.
            mutant = t.blur(2.)[0 if field == "H" else 1]
            self.detected(mutant, expected, f["tolerance"], f"terrain.json/blur_corrected/{field}")

    def test_constant_recall(self):
        f = load("rbf_read", self.root)
        case = next(c for c in f["cases"] if c["name"] == "five")
        b = restore_bank(case["bank"])
        q = torch.tensor(case["queries"])
        baseline = b.read(q, case["top_k"], increment_stats=False)[0]
        close(baseline, case["read"]["r_V"], f["tolerance"])
        # Even a nonzero constant (the correct first query's output) fails query two.
        def constant_read(queries):
            return baseline[:1].expand(queries.shape[0], -1, -1).clone()
        self.detected(constant_read(q), case["read"]["r_V"], f["tolerance"], "rbf_read.json/cases/five/read/r_V")

    def test_disabled_consolidation(self):
        f = load("consolidation", self.root)
        c = SleepConsolidator(**{k: v for k, v in f["config"].items() if k != "training"})
        c.stm_to_ltm.load_state_dict({k: torch.tensor(v) for k, v in f["projection"].items()})
        c.fatigue.fill_(f["before"]["fatigue"])
        stm, ltm = restore_bank(f["before"]["stm"]), restore_bank(f["before"]["ltm"])
        st, lt = restore_terrain(f["before"]["stm_terrain"]), restore_terrain(f["before"]["ltm_terrain"])
        c.consolidate(stm, ltm, st, lt)
        close(ltm.h, f["after"]["ltm"]["h"], f["tolerance"])
        close(c.fatigue, f["after"]["fatigue"], f["tolerance"])
        # Disabled execution is the untouched, restored pre-state.
        mutant_ltm = restore_bank(f["before"]["ltm"])
        def disabled_consolidate(*args):
            return None
        disabled_consolidate(stm, mutant_ltm, st, lt)
        self.detected(mutant_ltm.h, f["after"]["ltm"]["h"], f["tolerance"], "consolidation.json/after/ltm/h")

    def test_write_sigma_substitution(self):
        f = load("write", self.root)
        # Operation 1 is deliberately between read-width and write-width admission.
        op = f["operations"][1]
        b = restore_bank(f["operations"][0]["state"])
        inp = write_inputs(op["inputs"])
        baseline = b.write(**inp)[1]
        self.assertEqual(baseline, op["write_results"])
        mutant = restore_bank(f["operations"][0]["state"])
        mutant.sigma_read = mutant.sigma_write
        result = mutant.write(**inp)[1]
        self.assertNotEqual(result[0]["status"], op["write_results"][0]["status"])
        print("DETECTED write.json/operations/1/write_results/status (sigma_write: created; reference: reinforced)")

    def test_terrain_axis_swap(self):
        f = load("terrain", self.root)
        probe = f["axis_probe"]
        t = Terrain3D(16)
        t.H.copy_(torch.tensor(probe["H"]))
        q = torch.tensor(probe["sample_positions"])
        close(t.sample(q.flip(-1))[0], probe["corrected_H"], f["tolerance"])
        self.detected(t.sample(q)[0], probe["corrected_H"], f["tolerance"], "terrain.json/axis_probe/corrected_H (D09)")

    def test_stm_ltm_key_alignment(self):
        f = load("projections", self.root)
        p = restore_projections(f)
        x = torch.tensor(f["input_embeddings"])
        canonical = p.to_ltm_key(x)
        close(canonical, f["outputs"]["to_ltm_key"], f["tolerance"])
        self.detected(p.stm_to_ltm(p.to_stm_key(x)), f["outputs"]["to_ltm_key"], f["tolerance"],
                      "projections.json/outputs/to_ltm_key (D08 U is not canonical LTM key)")

    def test_missing_snapshot_cadence(self):
        from memory_module.text_memory import TextMemory
        from memory_module.config import MemoryConfig
        from memory_module.consolidation import AutomaticConsolidator
        f = load("consolidation", self.root)
        row = f["automatic_boundaries"][1]  # incoming 99 -> threshold 100
        m = TextMemory.__new__(TextMemory)
        m.config, m.device = MemoryConfig(), "cpu"
        m.stm_centers, m.ltm_centers = restore_bank(row["before"]["stm"]), restore_bank(row["before"]["ltm"])
        grids = f["automatic_terrain_states"]["empty"]
        m.stm_terrain, m.ltm_terrain = restore_terrain(grids["stm_terrain"]), restore_terrain(grids["ltm_terrain"])
        m.projections = ProjectionBundle()
        m.consolidator = SleepConsolidator(consolidation_top_m=8)
        m.consolidator.fatigue.fill_(3.)
        m.automatic_consolidator = AutomaticConsolidator(m.consolidator)
        m.automatic_consolidator.steps_since_consolidation = 99
        m.write_count = m.read_count = m.step_count = m.consolidation_count = 0
        snapshot = m._build_state_dict()
        # An actual reference reopen leaves a fresh scheduler at zero.
        m.automatic_consolidator.steps_since_consolidation = 0
        m._apply_state(snapshot)
        result = m.automatic_consolidator.step(0., m.stm_centers, m.ltm_centers, m.stm_terrain, m.ltm_terrain)
        self.assertIsNotNone(row["stats"])
        self.assertIsNone(result)
        self.assertNotEqual(m.automatic_consolidator.steps_since_consolidation, row["after"]["steps_since_consolidation"])
        print("DETECTED consolidation.json/automatic_boundaries/1/stats (D07 restored cadence misses sleep)")

    def test_swallowed_load_failure(self):
        from unittest.mock import patch
        from memory_module.text_memory import TextMemory
        m = TextMemory.__new__(TextMemory)
        m.device = "cpu"
        # Fault injection isolates the catch-all behavior; no files or pickle input.
        with patch("memory_module.text_memory.os.path.exists", return_value=True), \
             patch("memory_module.text_memory.load_bdbm", side_effect=ValueError("oracle corruption")):
            with self.assertLogs("bdbm.text_memory", level="ERROR"):
                result = m._load_impl("oracle-corrupt.bdbm")
        self.assertIsNone(result)
        # The reference returning normally violates fail-closed, regardless of logging.
        print("DETECTED D06 TextMemory._load_impl returned None instead of propagating injected corruption")

    def test_random_projection(self):
        f = load("projections", self.root)
        x = torch.tensor(f["input_embeddings"])
        p = restore_projections(f)
        close(p.to_ltm_key(x), f["outputs"]["to_ltm_key"], f["tolerance"])
        torch.manual_seed(9901)
        replacement = ProjectionBundle().eval()
        self.detected(replacement.to_ltm_key(x), f["outputs"]["to_ltm_key"], f["tolerance"],
                      "projections.json/outputs/to_ltm_key")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle", type=Path, default=DEFAULT_ORACLE)
    parser.add_argument("--list", action="store_true", help="List actual unittest IDs without executing")
    args = parser.parse_args()
    NegativeControls.root = args.oracle
    torch.set_num_threads(1)
    torch.set_default_dtype(torch.float32)
    torch.manual_seed(901)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(NegativeControls)
    if args.list:
        for test in suite:
            print(test.id())
        return
    with torch.no_grad():
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())


if __name__ == "__main__":
    main()
