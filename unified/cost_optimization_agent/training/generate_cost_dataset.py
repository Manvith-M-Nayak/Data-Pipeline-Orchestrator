"""
Generate the Cost Optimization Agent's supervised training set.

Each row = one pipeline STAGE described by the same 16 features as the
Resource Agent's model, labelled with the *cost-optimal compute configuration*
found by deadline-aware brute-force search over feasible (workers, node, shuffle)
combos.

The label minimizes dollar cost subject to finishing within a per-stage deadline.
Without a deadline constraint, the cheapest node always wins (cost is invariant
to worker count).  Deadlines create realistic diversity: large datasets with
tight SLAs need bigger/faster nodes.

Usage:
    python -m cost_optimization_agent.training.generate_cost_dataset --rows 200000
"""

import argparse
import csv
import math
import os
import random
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from resource_agent.ml.feature_spec import (
    stage_features,
    FEATURE_COLS as SRC_FEATURE_COLS,
)
from cost_optimization_agent.ml.feature_spec import (
    FEATURE_COLS,
    TARGET_COLS,
    NODE_TARGET,
    NODE_TYPES_BY_MEM,
    NODE_HOURLY_RATES,
    BOUNDS,
    SHUFFLE_TIERS,
    snap_shuffle,
)
from resource_agent.resource_agent import NODE_SPECS, DEFAULT_NODE, MAX_WORKERS, MAX_DIU

CSV_COLS = FEATURE_COLS + TARGET_COLS + [NODE_TARGET]

SIZE_TIERS = {
    "small": (0.30, (500, 49_000)),
    "medium": (0.30, (55_000, 980_000)),
    "large": (0.20, (1_100_000, 9_500_000)),
    "xlarge": (0.20, (11_000_000, 120_000_000)),
}
N_STAGES_DIST = {2: 0.20, 3: 0.30, 4: 0.25, 5: 0.15, 6: 0.10}
TRANSFORM_WEIGHTS = {
    0: 0.10,
    1: 0.20,
    2: 0.22,
    3: 0.18,
    4: 0.12,
    5: 0.08,
    6: 0.05,
    7: 0.03,
    8: 0.02,
}
FINAL_AGG_PROB = 0.35
FILTER_PROB = 0.40
JOIN_PROB = 0.08
DISTINCT_PROB = 0.10
SORT_PROB = 0.10
COLUMN_RANGE = (5, 28)
COPY_PROB = 0.25


def _weighted(rng, dist):
    keys, weights = list(dist.keys()), list(dist.values())
    return rng.choices(keys, weights=weights, k=1)[0]


def _make_notebook_stage(rng, is_final):
    transforms = [f"col{i} = expr{i}" for i in range(_weighted(rng, TRANSFORM_WEIGHTS))]
    stage = {"type": "notebook", "transformations": transforms}
    if is_final and rng.random() < FINAL_AGG_PROB:
        k = rng.randint(1, 3)
        stage["aggregations"] = {
            "group_by": ["grp"],
            "agg_exprs": [f"sum(c{i})" for i in range(k)],
        }
    if rng.random() < FILTER_PROB:
        stage["filter_condition"] = "x > 0"
    if rng.random() < JOIN_PROB:
        transforms.append("j = join(other)")
    if rng.random() < DISTINCT_PROB:
        transforms.append("distinct()")
    if rng.random() < SORT_PROB:
        transforms.append("orderBy(x)")
    return stage


def _make_copy_stage(rng):
    return {"type": "copy", "transformations": []}


def _estimate_stage_duration_s(workers, node_type, feat):
    """Estimate stage runtime in seconds for a given config."""
    if feat["stage_is_copy"]:
        csv_mb = max(feat["csv_size_mb"], 0.1)
        mb_per_s = 5.0
        return 30.0 + csv_mb / (max(1, workers) * mb_per_s)

    rows = max(feat["row_count"], 1)
    w = max(workers, 1)
    cores = NODE_SPECS.get(node_type, {}).get("cpu", 4)

    work_mult = 1.0
    work_mult += 0.08 * feat["transform_count"]
    work_mult += 0.05 * feat["has_filter"]
    work_mult += 0.90 * feat["agg_count"]
    work_mult += 2.00 * feat["has_join"]
    work_mult += 1.60 * feat["has_groupby"]
    work_mult += 1.20 * feat["has_distinct"]
    work_mult += 0.80 * feat["has_sort"]

    eff_rows = rows * work_mult
    startup_s = 120.0
    compute_s = eff_rows / (w * cores * 1600.0)
    return startup_s + compute_s


def _estimate_stage_cost_s(workers, node_type, duration_s, is_copy):
    """Estimate dollar cost for a stage config."""
    duration_h = max(duration_s / 3600.0, 1e-6)
    if is_copy:
        return 0.001  # ADF activity cost
    w = max(workers, 1)
    rate = NODE_HOURLY_RATES.get(node_type, 0.28)
    compute = w * rate * duration_h
    dbu = w * 1.5 * 0.55 * duration_h
    return compute + dbu


def _deadline_aware_optimal(feat, deadline_s):
    """Find the cheapest config that finishes within deadline_s.

    Without a deadline, the cheapest node always wins (cost is invariant to
    worker count).  Deadlines force bigger/faster nodes for large datasets.
    """
    is_copy = feat["stage_is_copy"]

    if is_copy:
        best = None
        best_cost = float("inf")
        for diu in range(1, MAX_DIU + 1):
            dur = _estimate_stage_duration_s(0, DEFAULT_NODE, feat)
            cost = _estimate_stage_cost_s(0, DEFAULT_NODE, dur, True)
            if cost < best_cost:
                best_cost = cost
                best = {
                    "opt_workers": 0,
                    "opt_diu": diu,
                    "opt_memory_gb": round(diu * 1.5, 2),
                    "opt_shuffle_partitions": SHUFFLE_TIERS[0],
                    "opt_node_type": DEFAULT_NODE,
                }
        return best or {
            "opt_workers": 0,
            "opt_diu": 2,
            "opt_memory_gb": 3.0,
            "opt_shuffle_partitions": 8,
            "opt_node_type": DEFAULT_NODE,
        }

    best = None
    best_cost = float("inf")

    for node in NODE_TYPES_BY_MEM:
        for workers in range(1, MAX_WORKERS + 1):
            dur = _estimate_stage_duration_s(workers, node, feat)
            if dur > deadline_s:
                continue  # doesn't meet deadline

            cost = _estimate_stage_cost_s(workers, node, dur, False)
            mem_gb = min(
                BOUNDS["opt_memory_gb"][1],
                4.0 + workers * NODE_SPECS[node]["memory_gb"],
            )
            shuffle = snap_shuffle(
                (feat["csv_size_mb"] / 128.0)
                * (1.0 + 0.5 * feat["has_groupby"] + 0.3 * feat["has_join"])
            )

            if cost < best_cost:
                best_cost = cost
                best = {
                    "opt_workers": workers,
                    "opt_diu": 0,
                    "opt_memory_gb": round(mem_gb, 2),
                    "opt_shuffle_partitions": int(shuffle),
                    "opt_node_type": node,
                }

    # Fallback: if no config meets deadline, pick cheapest overall
    if best is None:
        for node in NODE_TYPES_BY_MEM:
            workers = 1
            dur = _estimate_stage_duration_s(workers, node, feat)
            cost = _estimate_stage_cost_s(workers, node, dur, False)
            mem_gb = min(
                BOUNDS["opt_memory_gb"][1],
                4.0 + workers * NODE_SPECS[node]["memory_gb"],
            )
            shuffle = snap_shuffle(
                (feat["csv_size_mb"] / 128.0)
                * (1.0 + 0.5 * feat["has_groupby"] + 0.3 * feat["has_join"])
            )
            if cost < best_cost:
                best_cost = cost
                best = {
                    "opt_workers": workers,
                    "opt_diu": 0,
                    "opt_memory_gb": round(mem_gb, 2),
                    "opt_shuffle_partitions": int(shuffle),
                    "opt_node_type": node,
                }

    return best or {
        "opt_workers": 1,
        "opt_diu": 0,
        "opt_memory_gb": 8.0,
        "opt_shuffle_partitions": 8,
        "opt_node_type": DEFAULT_NODE,
    }


def _emit_pipeline(rng, writer):
    tier = _weighted(rng, {k: v[0] for k, v in SIZE_TIERS.items()})
    row_lo, row_hi = SIZE_TIERS[tier][1]
    row_count = rng.randint(row_lo, row_hi)

    column_count = rng.randint(*COLUMN_RANGE)
    schema = {
        "row_count": row_count,
        "columns": [f"c{i}" for i in range(column_count)],
        "size_hint": tier,
    }

    n_stages = _weighted(rng, N_STAGES_DIST)
    n_copy = 1 if rng.random() < COPY_PROB else 0
    n_notebook = n_stages - n_copy
    if n_notebook < 1:
        n_copy = 0
        n_notebook = n_stages

    stages = []
    for i in range(n_stages):
        is_copy = i < n_copy
        if is_copy:
            stages.append(_make_copy_stage(rng))
        else:
            nb_idx = i - n_copy
            is_final = nb_idx == n_notebook - 1
            stages.append(_make_notebook_stage(rng, is_final))

    for i, stage in enumerate(stages):
        csv_size_bytes = int(row_count * 140.0)
        feat = stage_features(
            stage, schema, csv_size_bytes, stage_index=i, n_stages=n_stages
        )

        # Varying deadlines create diverse node type labels:
        # - Generous deadline (600s)  → cheapest node wins
        # - Moderate deadline (300s)  → mid-range nodes
        # - Tight deadline (120s)     → bigger/faster nodes needed
        # - Very tight (60s)          → largest nodes
        deadline_s = rng.choice([60, 120, 180, 300, 600])

        labels = _deadline_aware_optimal(feat, deadline_s)

        # Add Gaussian noise to numeric targets
        noise_std = {
            "opt_workers": 0.35,
            "opt_diu": 0.45,
            "opt_memory_gb": 0.40,
            "opt_shuffle_partitions": 3.5,
        }
        for key, std in noise_std.items():
            labels[key] = round(max(0, labels[key] + rng.gauss(0, std)), 2)

        feat_row = {c: feat[c] for c in FEATURE_COLS}
        row = {**feat_row, **labels}
        writer.writerow(row)

    return n_stages


def generate(csv_path: str, num_rows: int, seed: int):
    rng = random.Random(seed)
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLS)
        writer.writeheader()

        generated = 0
        while generated < num_rows:
            written = _emit_pipeline(rng, writer)
            generated += written

    print(f"[generate] {num_rows} rows -> {csv_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=200000)
    ap.add_argument(
        "--out", default=os.path.join(os.path.dirname(__file__), "cost_training.csv")
    )
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    generate(args.out, args.rows, args.seed)
