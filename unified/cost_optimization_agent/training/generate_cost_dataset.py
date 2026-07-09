"""
Generate the Cost Optimization Agent's supervised training set.

Each row = one pipeline STAGE described by the same 16 features as the
Resource Agent's model, labelled with the *cost-optimal compute configuration*
found by brute-force search over all feasible (workers, node, shuffle) combos.

The label for each row is the configuration that minimizes estimated dollar
cost while keeping runtime reasonable.

Usage:
    python -m cost_optimization_agent.training.generate_cost_dataset --rows 200000
"""

import argparse
import csv
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
    brute_force_optimal,
)

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
        feat_row = {c: feat[c] for c in FEATURE_COLS}

        labels = brute_force_optimal(feat)
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
