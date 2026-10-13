#!/usr/bin/env python3
"""Export bounded, deterministic CPU/f32 fixtures from the unmodified Biomem source.

No RNG-parity assumption: initial state and operation inputs are serialized explicitly.
Corrected results are separately labeled and never substituted for reference output.
"""
from __future__ import annotations

import argparse
import dataclasses
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
import torch
import torch.nn.functional as F

REFERENCE_COMMIT = "500847ff65b5d9548b3826fa29bf3ccf8d221147"
MODEL_ID = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
# Immutable upstream revision, checked against the resolved snapshot at export time.
MODEL_REVISION = "e8f8c211226b894fcb81acc59f3b34ba3efd5f42"
ROOT = Path(__file__).resolve().parents[4]
ENVIRONMENT = {"python": "3.12.11", "torch": "2.8.0", "sentence-transformers": "5.1.0",
               "transformers": "4.57.6", "tokenizers": "0.22.2", "huggingface-hub": "0.36.2", "numpy": "2.5.2"}
MODEL_ARTIFACTS = {
    "1_Pooling/config.json": "4be450dde3b0273bb9787637cfbd28fe04a7ba6ab9d36ac48e92b11e350ffc23",
    "config.json": "6300193cb75e01cf80c96decef7187dfb33094d97cc1490b7ead6ff134476e4e",
    "config_sentence_transformers.json": "b8c64b5cece00d8424b4896ea75b512b6008576088497609dfeb6bd63e6d36b8",
    "model.safetensors": "eaa086f0ffee582aeb45b36e34cdd1fe2d6de2bef61f8a559a1bbc9bd955917b",
    "modules.json": "8f4b264b80206c830bebbdcae377e137925650a433b689343a63bdc9b3145460",
    "sentence_bert_config.json": "70f4448f31320443fe3557cacea5abf2dcc4915dda8c80646bec9f3bb0aa5a1f",
    "sentencepiece.bpe.model": "cfc8146abe2a0488e9e2a0c56de7952f7c11ab059eca145a0a727afce0db2865",
    "special_tokens_map.json": "378eb3bf733eb16e65792d7e3fda5b8a4631387ca04d2015199c4d4f22ae554d",
    "tokenizer.json": "2c3387be76557bd40970cec13153b3bbf80407865484b209e655e5e4729076b8",
    "tokenizer_config.json": "5036ea374ffedd706e3bef33e2e0d6953cb868ef8a490e76e32ba0faa37a6b9b",
    "unigram.json": "71b44701d7efd054205115acfa6ef126c5d2f84bd3affe0c59e48163674d19a6",
}
TOLERANCE = {"atol": 1e-6, "rtol": 1e-5}
MULTISTEP_TOLERANCE = {"atol": 2e-5, "rtol": 2e-4}
SEEDS = {name: 200 + i for i, name in enumerate((
    "projections", "rbf_read", "compound_read", "write", "write_strength",
    "terrain", "merge_prune_normalize", "consolidation", "trace_long", "real_embeddings"))}


def plain(value):
    """Snapshot live tensors immediately, avoiding mutation through aliased views."""
    if isinstance(value, torch.Tensor):
        return value.detach().cpu().tolist()
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    return value


def begin(name, description, multi=False):
    # Called before constructing or executing each fixture; tolerances never fit outputs.
    torch.manual_seed(SEEDS[name])
    return {"schema_version": 1, "description": description,
            "tolerance": dict(MULTISTEP_TOLERANCE if multi else TOLERANCE),
            "seed": SEEDS[name], "dtype": "float32", "device": "cpu", "threads": 1,
            "reference_commit": REFERENCE_COMMIT}


def save(out, name, fixture):
    data = (json.dumps(plain(fixture), ensure_ascii=False, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")
    if len(data) >= 2 * 1024 * 1024:
        raise ValueError(f"{name}: {len(data)} bytes exceeds fixture bound")
    (out / f"{name}.json").write_bytes(data)
    print(f"{name}.json {len(data)} bytes sha256={hashlib.sha256(data).hexdigest()}")


def unit(n, d):
    return F.normalize(torch.randn(n, d), dim=-1)


def bank(n=8, active=0, layer="ltm"):
    c = MemoryConfig()
    b = MemoryCenters(n_centers=n, d_key=getattr(c, f"d_{layer}_key"), d_value=128,
                      **{k: getattr(c, f"{layer}_{k}") for k in
                         ("sigma_read", "sigma_write", "leak", "leak_emotion", "leak_value",
                          "alpha_value", "alpha_emotion")})
    b.active[:active] = True
    b.h[:active] = torch.linspace(0.2, 2.4, active)
    b.V[:active] = torch.randn(active, 128) * 0.2
    b.e[:active] = torch.rand(active, 4) + 0.5
    b.K_context[:active] = unit(active, 16)
    b.K_terrain[:active] = torch.rand(active, 3) * 2 - 1
    b.age[:active] = torch.arange(active)
    b.usage[:active] = torch.arange(active) % 7
    for i in range(active):
        b.memory_ids[i] = f"{layer}-{i}"
        b.key_texts[i], b.value_texts[i] = f"key-{i}", f"value-{i}"
    return b


def state(b):
    return plain(b.state_dict_custom())


def terrain(layer="stm"):
    c = MemoryConfig()
    return Terrain3D(resolution=16, alpha_h=getattr(c, f"terrain_{layer}_alpha_h"),
                     alpha_e=getattr(c, f"terrain_{layer}_alpha_e"),
                     leak=getattr(c, f"terrain_{layer}_lambda"))


def read_result(result):
    return plain(dict(zip(("r_V", "r_E", "weights", "indices"), result)))


def project(p, x):
    ltm, stm = p.to_ltm_key(x), p.to_stm_key(x)
    return {"to_ltm_key": ltm, "to_stm_key": stm, "to_value": p.to_value(x),
            "to_context": p.to_context(x), "ltm_to_3d": p.ltm_to_3d(ltm),
            "stm_to_3d": p.stm_to_3d(stm), "stm_to_ltm": p.stm_to_ltm(stm)}


def projection_parameters(p):
    return {name: {"weight": mod.proj.weight, "bias": mod.proj.bias}
            for name, mod in p.named_children()}


def embeddings_fixture(out):
    f = begin("real_embeddings", "Real multilingual MiniLM; one-string batches, masked mean pooling then L2 normalization.")
    from huggingface_hub import snapshot_download
    from sentence_transformers import SentenceTransformer
    snapshot = Path(snapshot_download(MODEL_ID, revision=MODEL_REVISION, local_files_only=True))
    hashes = {name: hashlib.sha256((snapshot/name).read_bytes()).hexdigest() for name in MODEL_ARTIFACTS}
    if hashes != MODEL_ARTIFACTS:
        raise RuntimeError("Model artifact digest mismatch")
    model = SentenceTransformer(str(snapshot), device="cpu", local_files_only=True).eval()
    assert snapshot.name == MODEL_REVISION
    assert model.max_seq_length == 128
    f["model"] = {"id": MODEL_ID, "revision": snapshot.name,
                  "max_seq_length": model.max_seq_length, "batch_size": 1,
                  "artifacts_sha256": hashes}
    f["environment"] = {"python": platform.python_version(), "torch": torch.__version__,
                         **{name: importlib.metadata.version(name) for name in
                            ("sentence-transformers", "transformers", "tokenizers", "huggingface-hub", "numpy")}}
    if f["environment"] != ENVIRONMENT:
        raise RuntimeError(f"Environment pin mismatch: {f['environment']}")
    texts = ["The capital of France is Paris.", "Hlavní město České republiky je Praha.",
             "Memory recalls a stored fact.", "Paměť si vybaví uloženou informaci.",
             "Příliš žluťoučký kůň úpěl ďábelské ódy.", "fn compute_rbf_weights", "",
             " ".join(["memory"] * 200), "🙂🚀🧠", "Hello world!", "Děkuji za pomoc.",
             "The quick brown fox jumps over the lazy dog.", "Chyba: soubor nebyl nalezen.",
             "SELECT key FROM memory WHERE active = true;", "calm peace and friendship",
             "Strach, úzkost a panika."]
    rows = []
    for text in texts:
        tokens = model.tokenize([text])
        features = model[0]({k: v.clone() for k, v in tokens.items()})
        mask = features["attention_mask"].unsqueeze(-1).expand(features["token_embeddings"].size()).float()
        mean = (features["token_embeddings"] * mask).sum(1) / mask.sum(1).clamp(min=1e-9)
        pooled = model[1](features)["sentence_embedding"]
        encoded = model.encode([text], convert_to_tensor=True, normalize_embeddings=True,
                               show_progress_bar=False, batch_size=1)
        torch.testing.assert_close(mean, pooled, **TOLERANCE)
        torch.testing.assert_close(F.normalize(mean, dim=-1), encoded, **TOLERANCE)
        rows.append(plain({"text": text, "input_ids": tokens["input_ids"][0],
                           "attention_mask": tokens["attention_mask"][0],
                           "untruncated_token_count": len(model.tokenizer(text, verbose=False)["input_ids"]),
                           "masked_mean": mean[0], "embedding": encoded[0]}))
    f["cases"] = rows
    assert len(rows[7]["input_ids"]) == 128 and rows[7]["untruncated_token_count"] > 128
    save(out, "real_embeddings", f)
    return torch.tensor([r["embedding"] for r in rows[:2]])


def projections_fixture(out, real):
    f = begin("projections", "Actual initialized seven projections; two real-model strings and two synthetic basis vectors.")
    p = ProjectionBundle().eval()
    x = torch.cat((real, torch.eye(384)[:2]))
    f.update(plain({"input_embeddings": x, "input_sources": ["real_embeddings.cases[0]", "real_embeddings.cases[1]", "basis[0]", "basis[1]"],
                    "parameters": projection_parameters(p), "outputs": project(p, x)}))
    f["alignment_cosine"] = plain(F.cosine_similarity(p.to_ltm_key(x), p.stm_to_ltm(p.to_stm_key(x))))
    save(out, "projections", f)


def rbf_fixture(out):
    f = begin("rbf_read", "Cosine RBF, log intensity normalization, 64/65 hybrid boundary, distant and imbalanced reads.")
    cases = []
    for label, n in [("empty", 0), ("singleton", 1), ("five", 5), ("64", 64), ("65", 65), ("100", 100),
                     ("distant", 5), ("intensity_imbalance", 5)]:
        b = bank(max(8, n), n)
        q = unit(2, 64).unsqueeze(1)
        if n:
            q[0, 0] = b.K[0]
        if label == "distant":
            b.K[:n] = q[0, 0]
            q = -q[:1]
        if label == "intensity_imbalance":
            b.h[:n] = torch.tensor([1e-8, 1e-4, 1., 100., 1e5])
        row = {"name": label, "bank": state(b), "queries": plain(q), "sigma": b.sigma_read, "top_k": 4,
               "hybrid_active": n > 64}
        for normalize in (False, True):
            w, i = b.compute_rbf_weights(q, top_k=4, normalize=normalize)
            row["normalized" if normalize else "unnormalized"] = plain({"weights": w, "indices": i})
        row["read"] = read_result(b.read(q, top_k=4, increment_stats=False))
        row["post_read_usage"] = plain(b.usage)
        cases.append(row)
    f["cases"] = cases
    # Export exact ties without sorting or assuming torch.topk's portability.
    # This pinned CPU run returns ascending indices, matching the frozen policy.
    b = bank(8, 8)
    b.K[:] = torch.eye(64)[0]
    q = b.K[:1].unsqueeze(0)
    f["ties"] = {"bank": state(b), "queries": plain(q), "top_k": 4,
                 "reference": read_result(b.read(q, top_k=4, increment_stats=False)),
                 "v1_expected_indices": [[[0, 1, 2, 3]]]}
    save(out, "rbf_read", f)


def compound_fixture(out):
    f = begin("compound_read", "Compound scores captured from semantic/context/terrain components; None contributes exactly zero.")
    b = bank(8, 5)
    q, ctx, terr = unit(2, 64).unsqueeze(1), unit(2, 16).unsqueeze(1), unit(2, 3).unsqueeze(1)
    f["bank"] = state(b)
    f["cases"] = []
    for tq in (None, terr):
        parts = [(q.unsqueeze(2) * b.K[b.active]).sum(-1).clamp(-1, 1),
                 (ctx.unsqueeze(2) * b.K_context[b.active]).sum(-1).clamp(-1, 1)]
        parts.append(torch.zeros_like(parts[0]) if tq is None else
                     (tq.unsqueeze(2) * b.K_terrain[b.active]).sum(-1).clamp(-1, 1))
        score = .6 * parts[0] + .25 * parts[1] + .15 * parts[2]
        f["cases"].append(plain({"queries": q, "context_queries": ctx, "terrain_queries": tq,
                                   "top_k": 4, "sigma": b.sigma_read,
                                   "semantic_part": parts[0], "context_part": parts[1], "terrain_part": parts[2],
                                   "combined_score": score, "combined_weights": torch.exp(-(2 - 2 * score) / (2 * b.sigma_read ** 2)),
                                   **read_result(b.read_compound(q, ctx, tq, top_k=4, increment_stats=False))}))
    save(out, "compound_read", f)


def write_fixture(out):
    f = begin("write", "Sequential allocation/reinforcement/ignore/full-capacity; executed candidate width sigma_read (D02).", True)
    b = bank(8, layer="stm")
    f["initial"] = state(b)
    f["sigma_used"] = b.sigma_read
    f["sigma_write_unused"] = b.sigma_write
    f["operations"] = []
    keys = [torch.eye(16)[0], F.normalize(torch.tensor([.92, (1-.92**2)**.5] + [0.] * 14), dim=0),
            torch.eye(16)[1]] + list(torch.eye(16)[1:10])
    for i, k in enumerate(keys):
        inp = {"keys": k.unsqueeze(0), "values": torch.randn(1, 128), "emotions": torch.tensor([[1.4, .7, 1.2, .9]]),
               "intensities": torch.tensor([5e-7 if i == 2 else 1.2]), "top_k": 8, "new_center_threshold": .5,
               "context_keys": unit(1, 16), "terrain_positions": torch.tensor([[.2, -.4, .6]]),
               "key_texts": [f"write-key-{i}"], "value_texts": [f"write-value-{i}"], "memory_ids": [f"write-{i}"],
               "ages": [i], "return_results": True}
        candidates = {str(sigma): plain(dict(zip(("weights", "indices"), b.compute_rbf_weights(k.view(1, 1, 16), 8, False, sigma))))
                      for sigma in (b.sigma_read, b.sigma_write)}
        explicit = plain(inp)
        created, result = b.write(**inp)
        f["operations"].append({"inputs": explicit, "candidate_width_comparison": candidates,
                                "new_centers": created, "write_results": result, "state": state(b)})
    statuses = {r["write_results"][0]["status"] for r in f["operations"]}
    assert statuses == {"created", "reinforced", "ignored_zero_intensity", "capacity_exhausted"}
    assert f["operations"][1]["write_results"][0]["status"] == "reinforced"
    save(out, "write", f)


def text_shell(b):
    # Avoid TextMemory.__init__: it creates directories and an embedder. These two
    # reference methods only need config/device/LTM; no method body is replaced.
    m = TextMemory.__new__(TextMemory)
    m.config, m.device, m.ltm_centers = MemoryConfig(), "cpu", b
    return m


def strength_fixture(out):
    f = begin("write_strength", "Executed write strength and optional keyword heuristic; caller affect is never inferred from text.")
    f["config"] = dataclasses.asdict(MemoryConfig())
    f["cases"] = []
    for n in (0, 1, 5):
        b = bank(8, n)
        m = text_shell(b)
        q = b.K[:1].clone()
        for emotion, surprise, intensity in [([1.]*4, None, 1.), ([1.8,.7,1.2,1.], 2., .5), ([.5]*4, -1., 2.), ([1.]*4, 0., 0.)]:
            for near in (True, False):
                key = q if near else -q
                e = torch.tensor(emotion)
                s = None if surprise is None else torch.tensor([surprise])
                w, idx = b.compute_rbf_weights(key.unsqueeze(1), 1, False)
                novelty = torch.ones(1) if not n else 1 - w.squeeze(0).squeeze(-1)
                salience = (e - 1).abs().max(-1).values
                logit = 2 * novelty + .3 * (0 if s is None else s) + .3 * salience - 1
                f["cases"].append(plain({"bank": state(b), "k_ltm": key, "emotions": e, "surprise": s,
                                           "intensity": intensity, "rbf_weights": w, "indices": idx, "novelty": novelty,
                                           "salience": salience, "logit": logit,
                                           "omega": m._compute_write_strength(key, e, s, intensity)}))
    m = text_shell(bank())
    texts = ["", "ordinary factual statement", "radost úspěch skvělé výborně super hurá", "joy success great excellent amazing win",
             "klid pohoda spokojenost mír harmonie", "calm peace satisfied content balanced", "strach úzkost panika",
             "fear anxiety panic", "chyba problém nemoc špatně nebezpečí", "error problem illness bad danger",
             "vztah člověk pomoc děkuji přátelství láska", "relation help thank friendship love together",
             "GREAT great great", "window contentment"]
    f["extract"] = [{"text": t, "output": plain(EmotionExtractor.extract(t))} for t in texts]
    names = ["neutral", "positive", "negative", "curious", "social", "stressed", "dopamin", "serotonin", "kortizol", "oxytocin", "unknown", "Positive"]
    f["from_name"] = [{"name": n, "output": plain(EmotionExtractor.from_name(n))} for n in names]
    dictionaries = [{}, {"dopamin": 1.8, "kortizol": .6}, {"dopamine": 1.8, "cortisol": .6}, {"serotonin": 2., "oxytocin": .2}]
    f["from_dict"] = [{"input": d, "output": plain(EmotionExtractor.from_dict(d))} for d in dictionaries]
    inputs = [None, "positive", "Positive", *dictionaries, torch.tensor(1.7), torch.tensor([.2,.4,.6,.8,1.]),
              torch.tensor([[1.,2.,3.,4.]]), torch.tensor([.2,.3]), 42]
    f["sanitize"] = [{"type": type(v).__name__, "input": plain(v), "output": plain(m._sanitize_emotion(v))} for v in inputs]
    # JSON never encodes NaN/Inf: represent their input and observed output symbolically.
    bad = m._sanitize_emotion(torch.tensor([float("nan"), float("inf"), 1., 1.]))
    f["sanitize_nonfinite"] = {"input": ["NaN", "+Inf", 1., 1.], "isfinite": plain(torch.isfinite(bad)),
                               "v1_expected": "typed NonFinite error (contract section 10)"}
    save(out, "write_strength", f)


def corrected_blur(grid, sigma=2.):
    """D01 analytic separable Gaussian, replicate padding, f32 convolution."""
    size = max(3, int(6 * sigma) | 1)
    x = torch.arange(size, dtype=torch.float32) - size // 2
    kernel = torch.exp(-x.square() / (2 * sigma**2))
    kernel /= kernel.sum()
    result = grid.clone()
    channels = grid.shape[1]
    for axis in range(3):
        shape = [1, 1, 1]
        shape[axis] = size
        weights = kernel.reshape(1, 1, *shape).expand(channels, 1, *shape)
        padding = [0] * 6
        padding[2 * (2 - axis):2 * (2 - axis) + 2] = [size // 2] * 2
        result = F.conv3d(F.pad(result, padding, mode="replicate"), weights, groups=channels)
    return result


def terrain_fixture(out):
    f = begin("terrain", "16-cubed asymmetric splat, diffusion, border samples, no-op reference blur and corrected Gaussian.", True)
    t, dst = terrain(), terrain("ltm")
    f["index_order"] = "H[0,0,x,y,z]; E[0,channel,x,y,z]; flattened ((x*G)+y)*G+z"
    f["initial"] = state(t)
    positions = torch.tensor([[-.6, -.2, .6], [.4, .7, -.8]])
    inp = {"positions": positions, "intensities": torch.tensor([2., .7]),
           "emotions": torch.tensor([[1.2,.6,1.7,1.], [.5,1.4,.8,1.9]]), "sigma": .1, "eta": .02}
    f["splat_inputs"] = plain(inp)
    t.splat(**inp)
    f["after_splat"] = state(t)
    samples = torch.tensor([[[-1.,-1.,-1.], [1.,1.,1.], [-1.,0.,0.], [0.,1.,0.], [0.,0.,-1.],
                             [.23,-.17,.61], [-.6,-.2,.6], [.6,-.2,-.6], [2.,-.3,-2.]]])
    f["sample_positions"] = plain(samples)
    f["sample_before_step"] = plain(t.sample(samples))
    f["laplacian_before_step"] = plain({"H": t._compute_laplacian(t.H), "E": t._compute_laplacian(t.E)})
    t.step()
    f["after_step"] = state(t)
    f["sample_after_step"] = plain(t.sample(samples))
    hb, eb = t.blur(2.)
    assert torch.equal(hb, t.H) and torch.equal(eb, t.E)
    f["blur_reference"] = plain({"sigma": 2., "H": hb, "E": eb, "H_blurred_equals_H": True, "E_blurred_equals_E": True})
    f["blur_corrected"] = plain({"sigma": 2., "kernel_size": 13, "padding": "replicate",
                                  "H": corrected_blur(t.H), "E": corrected_blur(t.E)})
    f["merge_initial"] = state(dst)
    dst.merge_from(t, xi_h=.005, xi_e=.003, blur_sigma=2.)
    f["merge_inputs"] = {"xi_h": .005, "xi_e": .003, "blur_sigma": 2.}
    f["after_merge"] = state(dst)
    impulse = terrain()
    pos = torch.tensor([[-.6,-.2,.6]])  # exactly grid [3,6,12] at G=16
    impulse.splat(pos, torch.tensor([1.]), eta=1., sigma=.1)
    probe = torch.cat([pos, pos.flip(-1)]).unsqueeze(0)
    h, e = impulse.sample(probe)
    f["axis_probe"] = plain({"splat_position": pos, "grid_peak_index_xyz": [3,6,12], "H": impulse.H,
                              "sample_positions": probe, "reference_H": h, "reference_E": e,
                              "corrected_H": impulse.sample(probe.flip(-1))[0],
                              "observed": "grid_sample position components map to tensor z,y,x; splat maps to x,y,z"})
    assert h[0, 1] > .99 and h[0, 0] == 0
    torch.testing.assert_close(corrected_blur(torch.ones_like(t.H)), torch.ones_like(t.H), **TOLERANCE)
    save(out, "terrain", f)


def maintenance_fixture(out):
    f = begin("merge_prune_normalize", "Greedy merge, explicit 299/300/301 age and 4/5 usage boundaries, normalization and homeostasis.", True)
    b = bank(8, 6)
    b.K[1] = F.normalize(b.K[0] + .005 * b.K[2], dim=0)
    b.h[:2] = torch.tensor([2., 3.])
    b.V[0].fill_(1.); b.V[1].fill_(4.)
    before = state(b)
    count = b.merge_similar(.95)
    f["merge"] = {"before": before, "threshold": .95, "count": count, "after": state(b),
                   "analytic_nonaliased_V0": [2.8] * 128,
                   "note": "Reference h_i is a tensor view: assigning total_h mutates h_i before weighted V/e/K arithmetic (D12)."}
    assert count == 1
    b = bank(8, 8)
    b.age[:] = torch.tensor([299,300,301,299,300,301,301,301])
    b.usage[:] = torch.tensor([4,4,4,5,5,5,4,4])
    b.h[:] = torch.tensor([.01,.01,.01,.01,.01,.01,.011,.012])
    before = state(b)
    count = b.prune_weak(.001, 300)
    f["prune"] = {"before": before, "intensity_threshold": .001, "min_age": 300, "count": count, "after": state(b)}
    assert count == 1 and not b.active[2]
    b = bank(8, 5)
    b.h[:5] = torch.tensor([0., .001, .01, 1., 100.])
    b.e[0] = torch.tensor([-2.,0.,1.,3.])
    before = state(b)
    b.apply_normalization(c_v=2.)
    f["normalize"] = {"before": before, "c_v": 2., "after": state(b)}
    before = state(b)
    b.homeostasis_step()
    f["homeostasis"] = {"before": before, "after": state(b)}
    save(out, "merge_prune_normalize", f)


def world_state(stm, ltm, st, lt, c, auto=None):
    return {"stm": state(stm), "ltm": state(ltm), "stm_terrain": state(st), "ltm_terrain": state(lt),
            "fatigue": plain(c.fatigue), "steps_since_consolidation": None if auto is None else auto.steps_since_consolidation}


def consolidation_fixture(out):
    f = begin("consolidation", "Actual sleep top-8 of 12 and automatic fatigue/cadence boundaries; uncorrected reference blur/U.", True)
    stm, ltm, st, lt = bank(16, 12, "stm"), bank(16, 2), terrain(), terrain("ltm")
    c = SleepConsolidator(consolidation_top_m=8)
    st.splat(stm.K_terrain[:12], stm.h[:12], stm.e[:12], eta=.02)
    f["projection"] = plain(c.stm_to_ltm.state_dict())
    f["config"] = {k: v for k, v in vars(c).items() if isinstance(v, (float,int))}
    sequence = []
    for strength in [0., 10., 10., 10., 0., 0.]:
        before = plain(c.fatigue)
        c.update_fatigue(strength)
        sequence.append({"input": strength, "before": before, "after": plain(c.fatigue), "should_sleep": c.should_sleep()})
    f["fatigue_sequence"] = sequence
    f["before"] = world_state(stm, ltm, st, lt, c)
    selected = torch.where(stm.active)[0][torch.topk(stm.h[stm.active], 8).indices]
    f["selected_indices"] = plain(selected)
    captured = {}
    original = ltm.write
    def capture_write(**kwargs):
        captured["write_inputs"] = plain(kwargs)
        result = original(**kwargs)
        captured["new_centers"] = result
        captured["ltm_after_write_before_normalization"] = state(ltm)
        return result
    ltm.write = capture_write
    f["stats"] = c.consolidate(stm, ltm, st, lt)
    ltm.write = original
    f["intermediates"] = captured
    f["after"] = world_state(stm, ltm, st, lt, c)
    assert f["stats"]["consolidated_centers"] == 8
    f["automatic_boundaries"] = []
    for counter in (98,99,100):
        s, l, a, z = bank(8, 1, "stm"), bank(8), terrain(), terrain("ltm")
        cc = SleepConsolidator(consolidation_top_m=8)
        cc.fatigue.fill_(3.)
        auto = AutomaticConsolidator(cc, min_interval=100)
        auto.steps_since_consolidation = counter
        before = world_state(s,l,a,z,cc,auto)
        result = auto.step(0.,s,l,a,z)
        after = world_state(s,l,a,z,cc,auto)
        # Identical empty terrain inputs are stored once, with explicit references.
        terrain_states = f.setdefault("automatic_terrain_states", {})
        for phase, snapshot_state in (("before", before), ("after", after)):
            key = "empty" if phase == "before" or result is None else "after_success"
            grids = {k: snapshot_state.pop(k) for k in ("stm_terrain", "ltm_terrain")}
            if key in terrain_states:
                assert grids == terrain_states[key]
            else:
                terrain_states[key] = grids
            snapshot_state["terrain_state_ref"] = f"automatic_terrain_states.{key}"
        f["automatic_boundaries"].append({"before": before, "write_strength": 0., "min_interval": 100,
                                           "projection": plain(cc.stm_to_ltm.state_dict()),
                                           "attempted_counter": counter+1, "stats": result,
                                           "after": after})
        assert (result is not None) == (counter >= 99)
    save(out, "consolidation", f)


def tensor_digests(stm, ltm, st, lt, c):
    """Per-component SHA256 of ordered, concatenated tensor bytes; counters included."""
    result = {}
    for name, obj in (("stm",stm),("ltm",ltm),("stm_terrain",st),("ltm_terrain",lt),("consolidator",c)):
        digest = hashlib.sha256()
        for tensor in obj.state_dict().values():
            digest.update(tensor.detach().contiguous().numpy().tobytes())
        result[name] = digest.hexdigest()
    return result


def trace_fixture(out):
    f = begin("trace_long", "60 operations using synthetic 384-D vectors and real kernels; store/step are separate as in Python, not corrected D10 schedule.", True)
    p = ProjectionBundle().eval()
    stm,ltm,st,lt = bank(8,layer="stm"),bank(12),terrain(),terrain("ltm")
    c = SleepConsolidator(consolidation_top_m=8)
    c.stm_to_ltm = p.stm_to_ltm
    auto = AutomaticConsolidator(c)
    m = text_shell(ltm)
    f["digest_format"] = {"encoding": "sha256 of concatenated contiguous little-endian tensor bytes in listed order",
                           "components": {name: [{"name": key, "shape": list(t.shape), "dtype": str(t.dtype)}
                                                  for key, t in obj.state_dict().items()]
                                          for name, obj in (("stm",stm),("ltm",ltm),("stm_terrain",st),
                                                            ("ltm_terrain",lt),("consolidator",c))}}
    # Matrix state is recorded in projections.json for this trace via explicit reuse,
    # avoiding a second ~1.8 MiB matrix block inside the bounded trace.
    projection_fixture = json.loads((out / "projections.json").read_text())
    for name, module in p.named_children():
        values = projection_fixture["parameters"][name]
        module.proj.weight.copy_(torch.tensor(values["weight"]))
        if module.proj.bias is not None:
            module.proj.bias.copy_(torch.tensor(values["bias"]))
    f["projection_fixture"] = {"file": "projections.json", "field": "parameters",
                                "sha256": hashlib.sha256((out / "projections.json").read_bytes()).hexdigest()}
    f["config"] = dataclasses.asdict(m.config)
    f["consolidator_config"] = {k: v for k, v in vars(c).items() if isinstance(v, (float, int))}
    f["automatic_min_interval"] = auto.min_interval
    f["initial"] = world_state(stm,ltm,st,lt,c,auto)
    f["snapshots"] = {"0": f.pop("initial")}
    f["operations"] = []
    embeddings = unit(8,384)
    f["synthetic_embeddings"] = plain(embeddings)
    for i in range(1,61):
        if i % 15 == 0:
            row = {"op": "consolidate", "result": c.consolidate(stm,ltm,st,lt)}
        elif i % 3 == 0:
            stm.homeostasis_step(); ltm.homeostasis_step(); st.step(); lt.step()
            row = {"op": "step"}
        else:
            j = i % 8
            x, value = embeddings[j:j+1], embeddings[(j+1)%8:((j+1)%8)+1]
            projected = project(p,x)
            e = torch.tensor([1.2,.8,1.,1.4])
            omega = m._compute_write_strength(projected["to_ltm_key"], e, torch.tensor([.2]), 1.)
            inputs = {"keys": projected["to_stm_key"], "values": p.to_value(value), "emotions": e.unsqueeze(0),
                      "intensities": omega, "top_k": 8, "new_center_threshold": .5,
                      "context_keys": projected["to_context"], "terrain_positions": projected["stm_to_3d"],
                      "memory_ids": [f"trace-{j}"], "key_texts": [f"key-{j}"], "value_texts": [f"value-{j}"], "return_results": True}
            explicit = plain(inputs)
            result = stm.write(**inputs)
            st.splat(projected["stm_to_3d"], omega, e.unsqueeze(0), eta=.02, sigma=.1)
            automatic = auto.step(1.,stm,ltm,st,lt)
            row = {"op": "store", "embedding_index": j, "value_embedding_index": (j+1)%8,
                   "intensity": 1., "surprise": .2, "emotion": plain(e), "projections": plain(projected),
                   "write_inputs": explicit, "write_results": plain(result), "automatic_result": automatic}
        row.update({"index": i, "digests": tensor_digests(stm,ltm,st,lt,c),
                    "fatigue": plain(c.fatigue), "steps_since_consolidation": auto.steps_since_consolidation,
                    "stm_total_step": stm.total_step, "ltm_total_step": ltm.total_step})
        f["operations"].append(row)
        if i in (30,60):
            f["snapshots"][str(i)] = world_state(stm,ltm,st,lt,c,auto)
    save(out, "trace_long", f)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, default=Path.home()/"workspace/bleedingdev/projects/biomem/code/biomem")
    parser.add_argument("--output", type=Path, default=ROOT/"product/ncm/reference/oracle")
    args = parser.parse_args()
    revision = subprocess.check_output(["git", "-C", str(args.reference), "rev-parse", "HEAD"], text=True).strip()
    if revision != REFERENCE_COMMIT:
        raise RuntimeError(f"Reference revision mismatch: {revision}")
    if subprocess.check_output(["git", "-C", str(args.reference), "status", "--porcelain", "--", "src/memory_module"], text=True).strip():
        raise RuntimeError("Reference source is modified; refusing to export")
    sys.path.insert(0, str(args.reference/"src"))
    global MemoryConfig, MemoryCenters, Terrain3D, ProjectionBundle, SleepConsolidator, AutomaticConsolidator, TextMemory, EmotionExtractor
    from memory_module.config import MemoryConfig
    from memory_module.memory_centers import MemoryCenters
    from memory_module.terrain_3d import Terrain3D
    from memory_module.projections import ProjectionBundle
    from memory_module.consolidation import SleepConsolidator, AutomaticConsolidator
    from memory_module.text_memory import TextMemory
    from memory_module.embedder import EmotionExtractor
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    torch.set_default_dtype(torch.float32)
    torch.use_deterministic_algorithms(True)
    if sys.byteorder != "little":
        raise RuntimeError("Trace digest byte format requires a little-endian host")
    args.output.mkdir(parents=True, exist_ok=True)
    with torch.no_grad():
        real = embeddings_fixture(args.output)
        projections_fixture(args.output,real)
        for fn in (rbf_fixture,compound_fixture,write_fixture,strength_fixture,terrain_fixture,
                   maintenance_fixture,consolidation_fixture,trace_fixture):
            fn(args.output)


if __name__ == "__main__":
    main()
