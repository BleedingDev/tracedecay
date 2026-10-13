#!/usr/bin/env python3
"""Replay the exported explicit inputs, checking every kernel family and long trace."""
import argparse
import hashlib
import json
import sys
import unittest

import torch

import export_fixtures as oracle
from negative_controls import (load, close, restore_bank, restore_terrain, restore_projections,
                               DEFAULT_ORACLE, SleepConsolidator, write_inputs)
from memory_module.config import MemoryConfig
from memory_module.consolidation import AutomaticConsolidator
from memory_module.text_memory import TextMemory
from memory_module.embedder import EmotionExtractor


def recursive(actual, expected, tol):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys(), (actual.keys(), expected.keys())
        for k in expected:
            recursive(actual[k], expected[k], tol)
    elif isinstance(expected, list):
        assert len(actual) == len(expected)
        for a,e in zip(actual, expected):
            recursive(a,e,tol)
    elif isinstance(expected, float):
        close(actual,expected,tol)
    else:
        assert actual == expected, (actual,expected)


def check_bank(bank, expected, tol):
    recursive(oracle.state(bank), expected, tol)


class ReferenceOracle(unittest.TestCase):
    def test_projection_outputs(self):
        f = load("projections")
        p = restore_projections(f)
        outputs = oracle.project(p,torch.tensor(f["input_embeddings"]))
        for name,value in outputs.items():
            close(value,f["outputs"][name],f["tolerance"])

    def test_rbf_cases(self):
        f = load("rbf_read")
        for case in f["cases"]:
            b = restore_bank(case["bank"])
            q = torch.tensor(case["queries"])
            for normalized,label in ((False,"unnormalized"),(True,"normalized")):
                w,i = b.compute_rbf_weights(q,case["top_k"],normalized)
                close(w,case[label]["weights"],f["tolerance"])
                self.assertEqual(i.tolist(),case[label]["indices"])
            for name,value in zip(("r_V","r_E","weights","indices"),b.read(q,case["top_k"],False)):
                close(value,case["read"][name],f["tolerance"])
            check_bank(b,case["bank"],f["tolerance"])

    def test_compound_cases(self):
        f = load("compound_read")
        for c in f["cases"]:
            b = restore_bank(f["bank"])
            args = [None if c[k] is None else torch.tensor(c[k]) for k in ("queries","context_queries","terrain_queries")]
            for name,value in zip(("r_V","r_E","weights","indices"),b.read_compound(*args,top_k=c["top_k"],increment_stats=False)):
                close(value,c[name],f["tolerance"])

    def test_write_sequence(self):
        f = load("write")
        b = restore_bank(f["initial"])
        for op in f["operations"]:
            count,results = b.write(**write_inputs(op["inputs"]))
            self.assertEqual(count,op["new_centers"])
            self.assertEqual(results,op["write_results"])
            check_bank(b,op["state"],f["tolerance"])

    def test_write_strength_and_affect(self):
        f = load("write_strength")
        for c in f["cases"]:
            m = TextMemory.__new__(TextMemory)
            m.config,m.device,m.ltm_centers = MemoryConfig(),"cpu",restore_bank(c["bank"])
            strength = m._compute_write_strength(torch.tensor(c["k_ltm"]),torch.tensor(c["emotions"]),
                                                  None if c["surprise"] is None else torch.tensor(c["surprise"]),c["intensity"])
            close(strength,c["omega"],f["tolerance"])
        for c in f["extract"]:
            close(EmotionExtractor.extract(c["text"]),c["output"],f["tolerance"])
        for c in f["from_name"]:
            close(EmotionExtractor.from_name(c["name"]),c["output"],f["tolerance"])
        for c in f["from_dict"]:
            close(EmotionExtractor.from_dict(c["input"]),c["output"],f["tolerance"])
        for c in f["sanitize"]:
            v = torch.tensor(c["input"]) if c["type"] == "Tensor" else c["input"]
            close(m._sanitize_emotion(v),c["output"],f["tolerance"])

    def test_terrain_operations(self):
        f = load("terrain")
        t = restore_terrain(f["initial"])
        inp = {k:torch.tensor(v) if isinstance(v,list) else v for k,v in f["splat_inputs"].items()}
        t.splat(**inp)
        close(t.H,f["after_splat"]["H"],f["tolerance"])
        close(t.E,f["after_splat"]["E"],f["tolerance"])
        t.step()
        close(t.H,f["after_step"]["H"],f["tolerance"])
        close(t.E,f["after_step"]["E"],f["tolerance"])
        for actual,expected in zip(t.sample(torch.tensor(f["sample_positions"])),f["sample_after_step"]):
            close(actual,expected,f["tolerance"])
        dst = restore_terrain(f["merge_initial"])
        dst.merge_from(t,**f["merge_inputs"])
        close(dst.H,f["after_merge"]["H"],f["tolerance"])
        close(dst.E,f["after_merge"]["E"],f["tolerance"])

    def test_maintenance_operations(self):
        f = load("merge_prune_normalize")
        for op in ("merge","prune","normalize","homeostasis"):
            c = f[op]; b = restore_bank(c["before"])
            if op == "merge": self.assertEqual(b.merge_similar(c["threshold"]),c["count"])
            elif op == "prune": self.assertEqual(b.prune_weak(c["intensity_threshold"],c["min_age"]),c["count"])
            elif op == "normalize": b.apply_normalization(c_v=c["c_v"])
            else: b.homeostasis_step()
            check_bank(b,c["after"],f["tolerance"])

    def test_consolidation_and_cadence(self):
        f = load("consolidation")
        c = SleepConsolidator(**{k:v for k,v in f["config"].items() if k != "training"})
        c.stm_to_ltm.load_state_dict({k:torch.tensor(v) for k,v in f["projection"].items()})
        c.fatigue.fill_(f["before"]["fatigue"])
        s,l = restore_bank(f["before"]["stm"]),restore_bank(f["before"]["ltm"])
        st,lt = restore_terrain(f["before"]["stm_terrain"]),restore_terrain(f["before"]["ltm_terrain"])
        stats = c.consolidate(s,l,st,lt)
        recursive(stats,f["stats"],f["tolerance"])
        check_bank(s,f["after"]["stm"],f["tolerance"])
        check_bank(l,f["after"]["ltm"],f["tolerance"])
        close(st.H,f["after"]["stm_terrain"]["H"],f["tolerance"])
        close(lt.E,f["after"]["ltm_terrain"]["E"],f["tolerance"])
        for row in f["automatic_boundaries"]:
            before = row["before"]
            s,l = restore_bank(before["stm"]),restore_bank(before["ltm"])
            grids = f["automatic_terrain_states"][before["terrain_state_ref"].split(".")[-1]]
            st,lt = restore_terrain(grids["stm_terrain"]),restore_terrain(grids["ltm_terrain"])
            c = SleepConsolidator(consolidation_top_m=8)
            c.stm_to_ltm.load_state_dict({k:torch.tensor(v) for k,v in row["projection"].items()})
            c.fatigue.fill_(before["fatigue"])
            a = AutomaticConsolidator(c,row["min_interval"])
            a.steps_since_consolidation = before["steps_since_consolidation"]
            result = a.step(row["write_strength"],s,l,st,lt)
            recursive(result,row["stats"],f["tolerance"])
            self.assertEqual(a.steps_since_consolidation,row["after"]["steps_since_consolidation"])
            check_bank(l,row["after"]["ltm"],f["tolerance"])

    def test_long_trace_replay(self):
        f = load("trace_long")
        projection_bytes = (DEFAULT_ORACLE/"projections.json").read_bytes()
        self.assertEqual(hashlib.sha256(projection_bytes).hexdigest(),f["projection_fixture"]["sha256"])
        p = restore_projections(json.loads(projection_bytes))
        initial = f["snapshots"]["0"]
        s,l = restore_bank(initial["stm"]),restore_bank(initial["ltm"])
        st,lt = restore_terrain(initial["stm_terrain"]),restore_terrain(initial["ltm_terrain"])
        c = SleepConsolidator(**{k:v for k,v in f["consolidator_config"].items() if k != "training"})
        c.stm_to_ltm = p.stm_to_ltm
        c.fatigue.fill_(initial["fatigue"])
        auto = AutomaticConsolidator(c, f["automatic_min_interval"])
        m = TextMemory.__new__(TextMemory)
        m.config, m.device, m.ltm_centers = MemoryConfig(**f["config"]), "cpu", l
        for row in f["operations"]:
            if row["op"] == "store":
                x = torch.tensor([f["synthetic_embeddings"][row["embedding_index"]]])
                projected = oracle.project(p, x)
                for key, value in projected.items():
                    close(value, row["projections"][key], f["tolerance"])
                strength = m._compute_write_strength(projected["to_ltm_key"], torch.tensor(row["emotion"]),
                                                      torch.tensor([row["surprise"]]), row["intensity"])
                close(strength, row["write_inputs"]["intensities"], f["tolerance"])
                inp = write_inputs(row["write_inputs"])
                result = s.write(**inp)
                recursive(oracle.plain(result),row["write_results"],f["tolerance"])
                st.splat(inp["terrain_positions"],inp["intensities"],inp["emotions"],eta=.02,sigma=.1)
                auto.step(1.,s,l,st,lt)
            elif row["op"] == "step":
                s.homeostasis_step(); l.homeostasis_step(); st.step(); lt.step()
            else:
                recursive(c.consolidate(s,l,st,lt),row["result"],f["tolerance"])
            self.assertEqual(oracle.tensor_digests(s,l,st,lt,c),row["digests"],f"operation {row['index']}")
            self.assertEqual(auto.steps_since_consolidation,row["steps_since_consolidation"])
            if str(row["index"]) in f["snapshots"]:
                recursive(oracle.world_state(s,l,st,lt,c,auto),f["snapshots"][str(row["index"])] ,f["tolerance"])
        self.assertEqual(len(f["operations"]),60)

    def test_real_encoder(self):
        from huggingface_hub import snapshot_download
        from sentence_transformers import SentenceTransformer
        f = load("real_embeddings")
        path = snapshot_download(f["model"]["id"],revision=f["model"]["revision"],local_files_only=True)
        model = SentenceTransformer(path,device="cpu",local_files_only=True).eval()
        self.assertEqual(model.max_seq_length,f["model"]["max_seq_length"])
        for c in f["cases"]:
            t = model.tokenize([c["text"]])
            self.assertEqual(t["input_ids"][0].tolist(),c["input_ids"])
            self.assertEqual(t["attention_mask"][0].tolist(),c["attention_mask"])
            embeddings = model.encode([c["text"]],convert_to_tensor=True,show_progress_bar=False,normalize_embeddings=True)
            close(embeddings[0],c["embedding"],f["tolerance"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list",action="store_true")
    args = parser.parse_args()
    torch.set_num_threads(1)
    torch.set_default_dtype(torch.float32)
    torch.manual_seed(1001)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ReferenceOracle)
    if args.list:
        for test in suite: print(test.id())
        return
    with torch.no_grad():
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())


if __name__ == "__main__":
    main()
