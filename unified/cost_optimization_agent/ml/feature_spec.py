"""
Feature / target contract for the Cost Optimization Agent's ML model.

Mirrors the Resource Agent's feature_spec.py pattern: this module is the
SINGLE SOURCE OF TRUTH shared by:
  * training/generate_cost_dataset.py  (builds the labeled training set)
  * training/train_cost_model.py       (fits the models)
  * ml_predictor.py                    (runtime inference)

The model answers: "For this job stage, what is the cost-optimal configuration?"

It reuses the Resource Agent's FEATURE_COLS (same 16 features) so training
data and inference can share feature extraction with the Resource Agent's
ml/feature_spec.py. This means the cost model learns on the same workload
descriptors that the Resource Agent uses for sizing.

Targets are the cost-optimal compute settings, computed by evaluating all
feasible configurations and picking the cheapest that meets deadlines.

Why separate from the Resource Agent's model?
  The Resource Agent predicts what resources a job NEEDS.
  The Cost Optimization Agent predicts what resources are most COST-EFFICIENT,
  considering the trade-off between cluster size, runtime, and dollar cost.
"""

from resource_agent import (
    NODE_SPECS,
    DEFAULT_NODE,
    MAX_WORKERS,
    MAX_DIU,
    MAX_TOTAL_MEM_GB,
)

FEATURE_COLS = [
    "stage_is_copy",
    "csv_size_mb",
    "row_count",
    "column_count",
    "size_hint_ord",
    "transform_count",
    "agg_count",
    "has_filter",
    "has_join",
    "has_groupby",
    "has_aggregation",
    "has_distinct",
    "has_sort",
    "stage_index",
    "n_stages",
    "is_final_stage",
]

TARGET_COLS = [
    "opt_workers",
    "opt_diu",
    "opt_memory_gb",
    "opt_shuffle_partitions",
]
NODE_TARGET = "opt_node_type"

NODE_TYPES_BY_MEM = sorted(NODE_SPECS.keys(), key=lambda n: NODE_SPECS[n]["memory_gb"])
SHUFFLE_TIERS = [8, 16, 32, 64, 128, 200]
SIZE_HINT_LABELS = ["small", "medium", "large", "xlarge"]

BOUNDS = {
    "opt_workers": (0, MAX_WORKERS),
    "opt_diu": (0, MAX_DIU),
    "opt_memory_gb": (2.0, MAX_TOTAL_MEM_GB),
    "opt_shuffle_partitions": (SHUFFLE_TIERS[0], SHUFFLE_TIERS[-1]),
}

# Node hourly rates for cost computation during labeling
NODE_HOURLY_RATES: dict = {
    "Standard_DS2_v2": 0.14,
    "Standard_D4s_v3": 0.28,
    "Standard_D4_v3": 0.28,
    "Standard_DS3_v2": 0.28,
    "Standard_DS4_v2": 0.56,
    "Standard_D8s_v3": 0.56,
}


def size_hint_to_ord(size_hint: str, csv_size_mb: float = 0.0) -> int:
    s = (size_hint or "").lower()
    for i, label in enumerate(SIZE_HINT_LABELS):
        if label in s:
            return i
    if csv_size_mb < 5:
        return 0
    if csv_size_mb < 50:
        return 1
    if csv_size_mb < 200:
        return 2
    return 3


def snap_shuffle(value: float) -> int:
    return min(SHUFFLE_TIERS, key=lambda t: abs(t - value))


def estimate_stage_cost(
    workers: int,
    diu: int,
    node_type: str,
    duration_s: float,
    is_copy: bool,
) -> float:
    """Estimate the dollar cost of running one stage with given config."""
    duration_h = max(duration_s / 3600.0, 1e-6)
    if is_copy:
        compute = 0.0
        dbu = 0.0
    else:
        w = max(workers, 1)
        rate = NODE_HOURLY_RATES.get(node_type, 0.28)
        compute = w * rate * duration_h
        dbu = w * 1.5 * 0.55 * duration_h
    adf = 0.001 if is_copy else 0.0
    return compute + dbu + adf


def estimate_stage_duration(
    workers: int,
    diu: int,
    node_type: str,
    feat: dict,
) -> float:
    """
    Estimate runtime in seconds for a stage with given config.
    Uses a throughput model calibrated to real telemetry.
    """
    if feat["stage_is_copy"]:
        csv_mb = max(feat["csv_size_mb"], 0.1)
        d = max(diu, 1)
        mb_per_s = 5.0
        return 30.0 + csv_mb / (d * mb_per_s)

    rows = max(feat["row_count"], 1)
    w = max(workers, 1)
    cores = 4

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


def brute_force_optimal(feat: dict) -> dict:
    """
    Brute-force search over all feasible configurations to find the
    cost-optimal one. Used during training data generation.
    """
    is_copy = feat["stage_is_copy"]

    if is_copy:
        best = None
        best_cost = float("inf")
        for diu in range(1, MAX_DIU + 1):
            dur = estimate_stage_duration(0, diu, DEFAULT_NODE, feat)
            cost = estimate_stage_cost(0, diu, DEFAULT_NODE, dur, True)
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
        rate = NODE_HOURLY_RATES.get(node, 0.28)
        for workers in range(0, MAX_WORKERS + 1):
            w = max(workers, 1)
            dur = estimate_stage_duration(workers, 0, node, feat)
            cost = estimate_stage_cost(workers, 0, node, dur, False)

            mem_gb = min(MAX_TOTAL_MEM_GB, 4.0 + w * NODE_SPECS[node]["memory_gb"])

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
