"""
Calibration constants derived from the REAL telemetry in Datasets/Cleaned/.

These numbers anchor the synthetic label generator so the recommended settings
it produces reflect real Databricks/ADF behaviour rather than an arbitrary
formula. They were extracted from:

  job_runs_cleaned.csv (779k Databricks job runs)
    duration p50 = 80s, p95 = 534s. Trigger split is the key signal:
      CONTINUOUS (light/streaming)  p50 =  76s
      CRON       (heavy batch)      p50 = 489s   → ~6.4x heavier
    A "light" notebook on ~500k rows finishes near 76s on one worker, so a
    single 4-core worker sustains ≈ 1600 rows/core/s. Heavy stages (agg/join/
    group-by) are ~5-6x slower per row, matching the CRON tier.

  pipeline_runs_cleaned.csv (ADF copy runs)
    dev p25/p50/p75 = 35 / 50 / 65s → a typical copy targets ~55s.

  queries_cleaned.csv (60k SQL runs) — operation cost ORDERING:
    base query p50 = 0.245s;  has_join → 3.09s (12.6x),
    has_aggregation → 2.43s (9.9x),  has_group_by → 5.58s (22.8x).
    So group_by > join > aggregation in cost. Damped to stage scale below.

  dbquery_statistics_cleaned.csv — CPU-seconds & I/O magnitudes per statement
    (cpu_time p50 ≈ 0.06 CPU-s, logical_reads p50 ≈ 1.4M pages ≈ 11 GB scanned)
    inform the memory/shuffle blow-up factors.

Only the label generator imports this; it is pure data + one pure function.
"""

import math

from .feature_spec import (
    NODE_TYPES_BY_MEM,
    DEFAULT_NODE,
    SHUFFLE_TIERS,
    snap_shuffle,
)
from ..resource_agent import NODE_SPECS, MAX_WORKERS, MAX_DIU, MAX_TOTAL_MEM_GB

# ── Throughput anchors (calibrated to job_runs / pipeline_runs) ──────────────
ROWS_PER_CORE_PER_S = 1600.0     # light-notebook throughput per vCPU
ADF_MB_PER_DIU_PER_S = 5.0       # matches resource_agent's copy constant
TARGET_COPY_S = 55.0             # pipeline_runs dev median
SOFT_TARGET_S = 120.0            # per-stage runtime we size notebooks toward
DRIVER_ONLY_ROWS = 60_000        # below this effective-row count a worker isn't worth it
ROWS_PER_MB = 1100.0             # job_runs/queries rows-per-MB when row_count is unknown
BYTES_PER_ROW = 140.0            # fallback to derive size from row_count

# ── Per-operation work multipliers (queries ordering, damped to stage scale) ─
# effective_work = row_count * (1 + Σ multipliers)
WORK = {
    "transform_per": 0.08,   # +8% work per column transformation
    "filter":        0.05,   # a filter is cheap
    "agg_per":       0.90,   # each aggregation expression is heavy
    "join":          2.00,   # a join more than doubles the work
    "groupby":       1.60,   # group-by drives a wide shuffle (heaviest, per queries)
    "distinct":      1.20,
    "sort":          0.80,
}

# ── Memory / shuffle blow-up (working set relative to raw data) ──────────────
SHUFFLE_BLOWUP = {
    "base":          1.00,
    "transform_per": 0.05,
    "agg_per":       0.25,
    "join":          0.80,
    "groupby":       0.60,
    "distinct":      0.30,
}
DRIVER_OVERHEAD_GB = 4.0
MEM_PARTITION_MB = 128.0          # target bytes per shuffle partition
USABLE_MEM_FRACTION = 0.6         # fraction of a node's RAM available to Spark


def _work_multiplier(feat: dict) -> float:
    return (
        1.0
        + WORK["transform_per"] * feat["transform_count"]
        + WORK["filter"] * feat["has_filter"]
        + WORK["agg_per"] * feat["agg_count"]
        + WORK["join"] * feat["has_join"]
        + WORK["groupby"] * feat["has_groupby"]
        + WORK["distinct"] * feat["has_distinct"]
        + WORK["sort"] * feat["has_sort"]
    )


def _shuffle_blowup(feat: dict) -> float:
    return (
        SHUFFLE_BLOWUP["base"]
        + SHUFFLE_BLOWUP["transform_per"] * feat["transform_count"]
        + SHUFFLE_BLOWUP["agg_per"] * feat["agg_count"]
        + SHUFFLE_BLOWUP["join"] * feat["has_join"]
        + SHUFFLE_BLOWUP["groupby"] * feat["has_groupby"]
        + SHUFFLE_BLOWUP["distinct"] * feat["has_distinct"]
    )


def _data_gb(feat: dict) -> float:
    mb = feat["csv_size_mb"]
    if mb <= 0 and feat["row_count"] > 0:
        mb = feat["row_count"] * BYTES_PER_ROW / (1024 * 1024)
    return mb / 1024.0


def _effective_rows(feat: dict) -> float:
    rows = feat["row_count"]
    if rows <= 0 and feat["csv_size_mb"] > 0:
        rows = feat["csv_size_mb"] * ROWS_PER_MB
    return rows * _work_multiplier(feat)


# Nodes the recommender may pick: at least the default's 4 cores / 16 GB, so the
# worker-sizing math (which assumes a 4-core reference node) stays consistent.
_DEFAULT_MEM = NODE_SPECS[DEFAULT_NODE]["memory_gb"]
NODE_CANDIDATES = [
    n for n in NODE_TYPES_BY_MEM
    if NODE_SPECS[n]["cpu"] >= 4 and NODE_SPECS[n]["memory_gb"] >= _DEFAULT_MEM
]


def _pick_node(per_worker_gb: float) -> str:
    """Smallest ≥default node whose usable RAM covers one worker's share of the working set."""
    for node in NODE_CANDIDATES:
        if NODE_SPECS[node]["memory_gb"] * USABLE_MEM_FRACTION >= per_worker_gb:
            return node
    return NODE_CANDIDATES[-1]   # biggest available


def _noise(rng, sigma: float) -> float:
    return math.exp(rng.gauss(0.0, sigma))


def label_settings(feat: dict, rng) -> dict:
    """
    The ground-truth recommender: map a stage's workload features to the
    smallest feasible compute setting, with realistic noise. This is what the
    500k-row training set uses as its labels, so the learned model imitates a
    demand-driven provisioning policy (not the old duration heuristic).

    Returns a dict with the TARGET_COLS + NODE_TARGET keys, all within the
    student-tier ceilings.
    """
    if feat["stage_is_copy"]:
        raw = feat["csv_size_mb"] / max(TARGET_COPY_S * ADF_MB_PER_DIU_PER_S, 1e-6)
        diu = int(min(MAX_DIU, max(2, math.ceil(raw * _noise(rng, 0.12)))))
        return {
            "rec_workers": 0,
            "rec_diu": diu,
            "rec_memory_gb": round(diu * 1.5, 2),
            "rec_shuffle_partitions": SHUFFLE_TIERS[0],
            "rec_node_type": DEFAULT_NODE,
        }

    # ── notebook stage ──────────────────────────────────────────────────────
    node_cores = NODE_SPECS[DEFAULT_NODE]["cpu"]
    eff_rows = _effective_rows(feat)
    working_gb = _data_gb(feat) * _shuffle_blowup(feat) * _noise(rng, 0.15)

    # Parallelism need: smallest worker count keeping est runtime under target.
    parallel_w = MAX_WORKERS
    for w in range(1, MAX_WORKERS + 1):
        est_s = eff_rows / (w * node_cores * ROWS_PER_CORE_PER_S)
        if est_s <= SOFT_TARGET_S:
            parallel_w = w
            break

    # Memory need: enough workers so each worker's share fits a usable node.
    biggest_usable = NODE_SPECS[NODE_TYPES_BY_MEM[-1]]["memory_gb"] * USABLE_MEM_FRACTION
    memory_w = max(1, math.ceil(working_gb / max(biggest_usable, 1e-6)))

    workers = int(min(MAX_WORKERS, max(parallel_w, memory_w)))

    # Driver-only downgrade: only genuinely tiny, light stages skip workers
    # entirely. Gated on effective rows (not on parallel_w==1) so the 1-worker
    # tier remains populated for the medium band.
    driver_capacity = NODE_SPECS[DEFAULT_NODE]["memory_gb"] * USABLE_MEM_FRACTION
    no_heavy_ops = not (feat["has_aggregation"] or feat["has_join"] or feat["has_groupby"])
    if eff_rows < DRIVER_ONLY_ROWS and working_gb <= driver_capacity and no_heavy_ops:
        workers = 0

    per_worker_gb = working_gb / max(workers, 1)
    node = _pick_node(per_worker_gb)

    mem_gb = min(MAX_TOTAL_MEM_GB, round(DRIVER_OVERHEAD_GB + working_gb, 2))

    shuffle_raw = (working_gb * 1024.0 / MEM_PARTITION_MB) * (
        1.0 + 0.5 * feat["has_groupby"] + 0.3 * feat["has_join"]
    )
    shuffle = snap_shuffle(max(SHUFFLE_TIERS[0], shuffle_raw * _noise(rng, 0.1)))

    return {
        "rec_workers": workers,
        "rec_diu": 0,
        "rec_memory_gb": mem_gb,
        "rec_shuffle_partitions": shuffle,
        "rec_node_type": node,
    }
