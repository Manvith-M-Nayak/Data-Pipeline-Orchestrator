#!/usr/bin/env python3
"""
Stage 4 — Compare base vs fine-tuned.

For each held-out prompt (from mlx_data/valid.jsonl, or mlx_data/test.jsonl if
present) we generate TWICE on the identical input:

  1. base       : mlx-community/Qwen2.5-3B-Instruct-4bit, no adapter.
  2. fine-tuned : the same model WITH --adapter-path planner_adapter.

Outputs are printed side by side, then scored on objective, structure-aware
metrics (this task is JSON, so we measure structure, not prose):

  * Valid-JSON rate      — does the output parse as JSON?
  * Contract adherence   — does the parsed output satisfy the 9-key contract?
  * Settings-key match   — recommended_settings has exactly the 4 expected knobs.
  * Token similarity      — avg Jaccard token overlap vs the reference output.

Ends with a summary table: metric | base | fine-tuned | delta.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# Reuse the EXACT system prompt the model was trained with.
from prepare_data import SYSTEM_PROMPT  # noqa: F401  (kept identical at inference)

EXPECTED_SETTINGS = {"diu", "num_workers", "shuffle_partitions", "node_type"}
CONFIG_KEYS = {
    "containers", "containers_to_create", "datasets", "stages", "execution_order",
    "num_containers", "recommended_settings", "editable_settings", "reasoning",
}


# ── Output parsing ──────────────────────────────────────────────────────────────
def extract_json(text: str):
    """Best-effort: parse the first balanced top-level JSON object in `text`."""
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    start = text.find("{")
    if start < 0:
        return None
    depth, in_str, esc = 0, False, False
    for i in range(start, len(text)):
        c = text[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start : i + 1])
                    except json.JSONDecodeError:
                        return None
    return None


# ── Contract scoring (the 9-key wrapper contract) ───────────────────────────────
def check_contract(obj) -> bool:
    """True iff obj == {"config": <valid 9-key config>, "used_fallback": <bool>}."""
    if not isinstance(obj, dict):
        return False
    if set(obj.keys()) != {"config", "used_fallback"}:
        return False
    if not isinstance(obj["used_fallback"], bool):
        return False
    cfg = obj["config"]
    if not isinstance(cfg, dict) or set(cfg.keys()) != CONFIG_KEYS:
        return False

    containers = cfg["containers"]
    create = cfg["containers_to_create"]
    if not isinstance(containers, dict) or not isinstance(create, list) or not create:
        return False
    # num_containers counts containers (one per medallion layer), not stages.
    if cfg["num_containers"] != len(create):
        return False

    # datasets: roles valid, >=1 source & sink, containers known.
    datasets = cfg["datasets"]
    if not isinstance(datasets, list) or not datasets:
        return False
    roles = set()
    for d in datasets:
        if not isinstance(d, dict) or {"name", "container", "role"} - set(d):
            return False
        if d["role"] not in {"source", "intermediate", "sink"}:
            return False
        if d["container"] not in create:
            return False
        roles.add(d["role"])
    if not {"source", "sink"} <= roles:
        return False

    # stages: stage0 is the ADF copy ingest, every later stage a Databricks notebook.
    stages = cfg["stages"]
    if not isinstance(stages, list) or not stages:
        return False
    for i, st in enumerate(stages):
        if not isinstance(st, dict):
            return False
        want = "copy" if i == 0 else "notebook"
        if st.get("type") != want:
            return False

    # execution_order == stage names in order.
    names = [st.get("name") for st in stages]
    if cfg["execution_order"] != names:
        return False

    # recommended_settings exactly the 4 knobs; editable_settings covers them.
    rs = cfg["recommended_settings"]
    if not isinstance(rs, dict) or set(rs.keys()) != EXPECTED_SETTINGS:
        return False
    es = cfg["editable_settings"]
    if not isinstance(es, dict) or not EXPECTED_SETTINGS <= set(es.keys()):
        return False
    if not isinstance(cfg["reasoning"], str) or not cfg["reasoning"].strip():
        return False
    return True


def settings_ok(obj) -> bool:
    """recommended_settings has exactly the 4 expected knobs."""
    try:
        return set(obj["config"]["recommended_settings"].keys()) == EXPECTED_SETTINGS
    except (TypeError, KeyError, AttributeError):
        return False


def jaccard(a: str, b: str) -> float:
    ta, tb = set(a.split()), set(b.split())
    if not ta and not tb:
        return 1.0
    return len(ta & tb) / max(1, len(ta | tb))


# ── Generation ──────────────────────────────────────────────────────────────────
def load_samples(data_dir: str, limit: int):
    path = os.path.join(data_dir, "test.jsonl")
    if not os.path.exists(path):
        path = os.path.join(data_dir, "valid.jsonl")
    if not os.path.exists(path):
        sys.exit(f"ERROR: no held-out set found ({data_dir}/test.jsonl or valid.jsonl). "
                 "Run prepare_data.py first.")
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    print(f"==> Held-out set: {path} ({len(rows)} rows, using {min(limit, len(rows))})")
    return rows[:limit]


def gen_all(model_id, adapter, samples, max_tokens):
    """Load one model (optionally with adapter) and generate for every sample."""
    from mlx_lm import generate, load

    tag = "fine-tuned" if adapter else "base"
    print(f"==> Loading {tag} model{' + adapter '+adapter if adapter else ''} ...")
    model, tokenizer = load(model_id, adapter_path=adapter) if adapter else load(model_id)

    outs = []
    for i, rec in enumerate(samples):
        prompt_msgs = rec["messages"][:-1]  # system + user (drop reference assistant)
        prompt = tokenizer.apply_chat_template(
            prompt_msgs, add_generation_prompt=True, tokenize=False
        )
        text = generate(model, tokenizer, prompt=prompt, max_tokens=max_tokens, verbose=False)
        outs.append(text.strip())
        print(f"    [{tag}] {i+1}/{len(samples)} generated", end="\r", flush=True)
    print()
    del model, tokenizer
    return outs


def score(outputs, references):
    n = len(outputs)
    vj = ca = sk = 0
    sim = 0.0
    for out, ref in zip(outputs, references):
        obj = extract_json(out)
        if obj is not None:
            vj += 1
            if check_contract(obj):
                ca += 1
            if settings_ok(obj):
                sk += 1
        sim += jaccard(out, ref)
    return {
        "Valid-JSON rate": vj / n,
        "Contract adherence": ca / n,
        "Settings-key match": sk / n,
        "Token similarity": sim / n,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare base vs fine-tuned planner.")
    ap.add_argument("--model", default="mlx-community/Qwen2.5-3B-Instruct-4bit")
    ap.add_argument("--adapter", default="planner_adapter")
    ap.add_argument("--data-dir", default="mlx_data")
    ap.add_argument("--num-samples", type=int, default=20)
    ap.add_argument("--max-tokens", type=int, default=768)
    ap.add_argument("--show", type=int, default=3, help="how many side-by-side prints")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    os.chdir(here)
    if not os.path.isdir(args.adapter):
        sys.exit(f"ERROR: adapter '{args.adapter}' not found. Run ./train.sh first.")

    samples = load_samples(args.data_dir, args.num_samples)
    references = [r["messages"][-1]["content"] for r in samples]

    # Sequential load keeps peak memory to one 3B model at a time (16 GB-safe).
    base_outs = gen_all(args.model, None, samples, args.max_tokens)
    ft_outs = gen_all(args.model, args.adapter, samples, args.max_tokens)

    # Side-by-side for visual inspection.
    for i in range(min(args.show, len(samples))):
        user_msg = samples[i]["messages"][1]["content"]
        print("\n" + "=" * 100)
        print(f"PROMPT #{i+1} (user message, truncated):\n{user_msg[:600]}")
        print("-" * 100)
        print(f"BASE OUTPUT:\n{base_outs[i][:900]}")
        print("-" * 100)
        print(f"FINE-TUNED OUTPUT:\n{ft_outs[i][:900]}")
    print("\n" + "=" * 100)

    base_m = score(base_outs, references)
    ft_m = score(ft_outs, references)

    # Summary table.
    print("\nSUMMARY  (n = %d held-out prompts)\n" % len(samples))
    print(f"{'metric':<22} | {'base':>8} | {'fine-tuned':>11} | {'delta':>8}")
    print("-" * 60)
    for k in base_m:
        b, f = base_m[k], ft_m[k]
        print(f"{k:<22} | {b:>8.2%} | {f:>11.2%} | {f-b:>+8.2%}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
