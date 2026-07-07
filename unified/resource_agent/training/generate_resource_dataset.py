#!/usr/bin/env python3
"""
Generate the Resource Agent's supervised-regression training set.

Each row = one pipeline STAGE, described by plan-knowable features (size, rows,
columns, operations, position) and labelled with the *recommended compute
settings* (workers, DIU, peak memory, shuffle partitions, node type). The
labels come from calibration.label_settings(), a demand-driven provisioning
policy anchored to the REAL telemetry in Datasets/Cleaned/ (see calibration.py).

Why not train on the existing synthetic_resource_dataset.json? Its labels are
produced by the Resource Agent's own heuristic, so a model trained on it would
just clone the heuristic. This generator instead grounds labels in real
Databricks/ADF behaviour, so the model learns a realistic demand→settings map.

Grounding in the real datasets:
  * operation COST / intensity / data-size realism → calibration.py (from
    job_runs, pipeline_runs, queries, dbquery_statistics).
  * per-stage trigger INTENSITY spread → sampled from job_runs' trigger mix
    when the CSVs are present (falls back to the measured distribution).
Operation PRESENCE is sampled from a balanced synthetic distribution on purpose:
the raw ingestion workloads in queries_cleaned are ~99% trivial, which would
starve the model of the complex examples it must learn to size for.

Usage:
    # runs as a module (preferred) or as a plain script
    python -m resource_agent.training.generate_resource_dataset --rows 500000
    python resource_agent/training/generate_resource_dataset.py --rows 500000 \
        --out resource_agent/training/resource_training.csv
"""

import argparse
import csv
import os
import random
import sys

# ── Import the shared contract whether run as a module or a flat script ──────
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from resource_agent.ml.feature_spec import (          # noqa: E402
    FEATURE_COLS, TARGET_COLS, NODE_TARGET, stage_features,
)
from resource_agent.ml.calibration import label_settings, BYTES_PER_ROW  # noqa: E402

CSV_COLS = FEATURE_COLS + TARGET_COLS + [NODE_TARGET]

# ── Workload sampling config ─────────────────────────────────────────────────
SIZE_TIERS = {
    # label: (probability, (row_lo, row_hi))
    "small":  (0.30, (500, 49_000)),
    "medium": (0.30, (55_000, 980_000)),
    "large":  (0.20, (1_100_000, 9_500_000)),
    "xlarge": (0.20, (11_000_000, 120_000_000)),
}
N_STAGES_DIST = {2: 0.20, 3: 0.30, 4: 0.25, 5: 0.15, 6: 0.10}
TRANSFORM_WEIGHTS = {0: 0.10, 1: 0.20, 2: 0.22, 3: 0.18, 4: 0.12, 5: 0.08, 6: 0.05, 7: 0.03, 8: 0.02}
FINAL_AGG_PROB = 0.35     # chance the last notebook stage aggregates
FILTER_PROB    = 0.40
JOIN_PROB      = 0.08
DISTINCT_PROB  = 0.10
SORT_PROB      = 0.10
COLUMN_RANGE   = (5, 28)  # real cleaned CSVs span ~15–32 columns

# Real job_runs trigger mix (fallback if the CSVs aren't mounted); used only to
# add realistic heterogeneity to demand via a light intensity multiplier.
DEFAULT_TRIGGER_MIX = {"CONTINUOUS": 0.828, "CRON": 0.160, "ONETIME": 0.009, "PERIODIC": 0.003}
TRIGGER_INTENSITY = {"CONTINUOUS": 1.0, "CRON": 1.6, "ONETIME": 1.4, "PERIODIC": 1.3}


def _weighted(rng, dist):
    keys, weights = list(dist.keys()), list(dist.values())
    return rng.choices(keys, weights=weights, k=1)[0]


def _load_trigger_mix(datasets_dir):
    """Read the real trigger distribution from job_runs if available."""
    path = os.path.join(datasets_dir, "job_runs_cleaned.csv")
    if not os.path.exists(path):
        return DEFAULT_TRIGGER_MIX
    try:
        import pandas as pd
        s = pd.read_csv(path, usecols=["trigger_type"])["trigger_type"].value_counts(normalize=True)
        mix = {k: float(v) for k, v in s.items() if k in TRIGGER_INTENSITY}
        total = sum(mix.values())
        return {k: v / total for k, v in mix.items()} if total > 0 else DEFAULT_TRIGGER_MIX
    except Exception as exc:
        print(f"[gen] could not read trigger mix ({exc}); using measured defaults")
        return DEFAULT_TRIGGER_MIX


def _make_notebook_stage(rng, is_final):
    """Build a notebook stage dict whose ops stage_features() can recover verbatim."""
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


def _emit_pipeline(rng, trigger_mix, writer):
    """Sample one pipeline and write one CSV row per stage. Returns rows written."""
    tier = _weighted(rng, {k: v[0] for k, v in SIZE_TIERS.items()})
    row_lo, row_hi = SIZE_TIERS[tier][1]
    row_count = rng.randint(row_lo, row_hi)
    bytes_per_row = max(40.0, rng.gauss(BYTES_PER_ROW, 40.0))
    csv_size_bytes = int(row_count * bytes_per_row)
    column_count = rng.randint(*COLUMN_RANGE)

    # Real-telemetry-driven demand heterogeneity: heavier trigger classes push
    # a stage toward more resources (mirrors the CRON-vs-CONTINUOUS spread).
    trigger = _weighted(rng, trigger_mix)
    intensity = TRIGGER_INTENSITY[trigger]

    n_stages = int(_weighted(rng, N_STAGES_DIST))
    schema = {
        "row_count": row_count,
        "columns": [f"c{i}" for i in range(column_count)],
        "size_hint": tier,
    }

    written = 0
    for idx in range(n_stages):
        if idx == 0:
            stage = {"type": "copy"}
        else:
            stage = _make_notebook_stage(rng, is_final=(idx == n_stages - 1))

        feat = stage_features(stage, schema, csv_size_bytes, stage_index=idx, n_stages=n_stages)

        # Fold trigger intensity into effective work by nudging the size the
        # label model sees (keeps the recorded feature = plan-knowable size).
        feat_for_label = dict(feat)
        feat_for_label["row_count"] = int(feat["row_count"] * intensity)
        feat_for_label["csv_size_mb"] = feat["csv_size_mb"] * intensity

        label = label_settings(feat_for_label, rng)
        writer.writerow(
            [feat[c] for c in FEATURE_COLS]
            + [label[c] for c in TARGET_COLS]
            + [label[NODE_TARGET]]
        )
        written += 1
    return written


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, default=500_000, help="target number of stage rows")
    ap.add_argument("--seed", type=int, default=20260707)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "resource_training.csv"))
    ap.add_argument("--datasets", default=os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "Datasets", "Datasets", "Cleaned")),
        help="path to the cleaned real-telemetry CSVs (optional; grounds the trigger mix)")
    args = ap.parse_args(argv)

    rng = random.Random(args.seed)
    trigger_mix = _load_trigger_mix(args.datasets)
    print(f"[gen] trigger mix: { {k: round(v,3) for k,v in trigger_mix.items()} }")
    print(f"[gen] writing up to {args.rows} rows to {args.out}")

    written = 0
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLS)
        while written < args.rows:
            written += _emit_pipeline(rng, trigger_mix, writer)
            if written % 50_000 < 6:
                print(f"[gen] {written:,} rows…", flush=True)

    print(f"[gen] done — {written:,} rows written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
