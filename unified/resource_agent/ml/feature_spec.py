"""
Feature / target contract for the Resource Agent's resource-sizing model.

This module is the SINGLE SOURCE OF TRUTH shared by three consumers so they can
never drift:
  * training/generate_resource_dataset.py  (builds the 500k-row training set)
  * training/train_resource_model.py / the Kaggle notebook (fits the models)
  * ml_predictor.py                        (runtime inference inside the agent)

It is deliberately dependency-light (pure Python — no numpy/sklearn/pandas) so
that importing it from resource_agent.py never pulls heavy libraries into the
FastAPI process.

The model is a MULTI-TARGET regressor + one small classifier that answers:
  "For this stage on this data, what compute settings should we provision?"

  Regression targets : rec_workers, rec_diu, rec_memory_gb, rec_shuffle_partitions
  Classification tgt  : rec_node_type

Every target is bounded by the student-tier ceilings defined in resource_agent.py
so a recommendation is always feasible by construction.
"""

from ..resource_agent import (
    NODE_SPECS,
    DEFAULT_NODE,
    MAX_WORKERS,
    MAX_DIU,
    MAX_TOTAL_MEM_GB,
)

# ── Feature columns (all numeric → no encoder needed at inference) ────────────
FEATURE_COLS = [
    "stage_is_copy",        # 1 = ADF copy stage, 0 = Databricks notebook stage
    "csv_size_mb",          # input size in MB
    "row_count",            # rows in the source dataset
    "column_count",         # number of columns
    "size_hint_ord",        # 0=small 1=medium 2=large 3=xlarge
    "transform_count",      # number of column transformations
    "agg_count",            # number of aggregation expressions
    "has_filter",           # 1 if the stage has a filter_condition
    "has_join",             # 1 if any transformation performs a join
    "has_groupby",          # 1 if the aggregation groups rows
    "has_aggregation",      # 1 if the stage aggregates at all
    "has_distinct",         # 1 if the stage dedups (distinct)
    "has_sort",             # 1 if the stage sorts / orderBy
    "stage_index",          # position of this stage in the pipeline (0-based)
    "n_stages",             # total stages in the pipeline
    "is_final_stage",       # 1 if this is the last stage (usually the aggregate)
]

# ── Targets the model recommends ─────────────────────────────────────────────
TARGET_COLS = [
    "rec_workers",              # Databricks workers 0..MAX_WORKERS (0 for copy)
    "rec_diu",                  # ADF DIU 0..MAX_DIU (0 for notebook)
    "rec_memory_gb",            # peak memory GB (driver + workers)
    "rec_shuffle_partitions",   # Spark shuffle partitions
]
NODE_TARGET = "rec_node_type"   # categorical — one of NODE_TYPES_BY_MEM

# Node catalogue ordered by memory (ascending) — used both as the classifier's
# label space and for deterministic node selection in the label generator.
NODE_TYPES_BY_MEM = sorted(NODE_SPECS.keys(), key=lambda n: NODE_SPECS[n]["memory_gb"])

# Shuffle-partition tiers the recommender is allowed to emit (post-processing
# snaps a raw regression output to the nearest tier).
SHUFFLE_TIERS = [8, 16, 32, 64, 128, 200]

SIZE_HINT_LABELS = ["small", "medium", "large", "xlarge"]

# Bounds used by both the generator (clamping labels) and the predictor
# (clamping model outputs) so recommendations are always feasible.
BOUNDS = {
    "rec_workers":            (0, MAX_WORKERS),
    "rec_diu":                (0, MAX_DIU),
    "rec_memory_gb":          (2.0, MAX_TOTAL_MEM_GB),
    "rec_shuffle_partitions": (SHUFFLE_TIERS[0], SHUFFLE_TIERS[-1]),
}


# ── Helpers ──────────────────────────────────────────────────────────────────
def size_hint_to_ord(size_hint: str, csv_size_mb: float = 0.0) -> int:
    """Map a size-hint label (or fall back to raw MB) to an ordinal 0..3."""
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


def _aggregation_block(stage: dict) -> dict:
    """Return the stage's aggregation dict regardless of which key/shape is used.

    The Planner emits `aggregation = {"group_by": [...], "aggregations": [...]}`
    while the Resource Agent's own callers use `aggregations = {"agg_exprs": [...]}`.
    Accept both.
    """
    agg = stage.get("aggregations") or stage.get("aggregation") or {}
    return agg if isinstance(agg, dict) else {}


def _agg_count(agg: dict) -> int:
    if not agg:
        return 0
    exprs = agg.get("agg_exprs") or agg.get("aggregations") or []
    return len(exprs) if isinstance(exprs, list) else 0


def _has_groupby(agg: dict) -> int:
    if not agg:
        return 0
    gb = agg.get("group_by") or agg.get("groupBy") or []
    return 1 if gb else 0


def stage_features(
    stage: dict,
    schema: dict,
    csv_size_bytes: int = 0,
    stage_index: int = 0,
    n_stages: int = 1,
) -> dict:
    """
    Extract the flat feature dict for one stage. Used identically by the
    generator (to label rows), the trainer (via the generated CSV), and the
    predictor (at inference), guaranteeing train/serve parity.
    """
    schema = schema or {}
    stype = (stage.get("type") or "notebook").lower()
    is_copy = 1 if stype == "copy" else 0

    csv_size_mb = (csv_size_bytes / (1024 * 1024)) if csv_size_bytes else 0.0
    row_count = int(schema.get("row_count", 0) or 0)
    cols = schema.get("columns") or list((schema.get("inferred_types") or {}).keys())
    column_count = len(cols)

    transforms = stage.get("transformations") or []
    transform_count = len(transforms)
    tjoined = " ".join(str(t) for t in transforms).lower()

    agg = _aggregation_block(stage)
    agg_count = _agg_count(agg)

    size_hint = schema.get("size_hint", "")

    return {
        "stage_is_copy":    is_copy,
        "csv_size_mb":      round(csv_size_mb, 6),
        "row_count":        row_count,
        "column_count":     column_count,
        "size_hint_ord":    size_hint_to_ord(size_hint, csv_size_mb),
        "transform_count":  transform_count,
        "agg_count":        agg_count,
        "has_filter":       1 if stage.get("filter_condition") else 0,
        "has_join":         1 if "join" in tjoined else 0,
        "has_groupby":      _has_groupby(agg),
        "has_aggregation":  1 if (agg_count > 0 or agg) else 0,
        "has_distinct":     1 if "distinct" in tjoined else 0,
        "has_sort":         1 if ("orderby" in tjoined or "sort" in tjoined) else 0,
        "stage_index":      int(stage_index),
        "n_stages":         int(n_stages),
        "is_final_stage":   1 if stage_index >= n_stages - 1 else 0,
    }


def features_to_vector(feat: dict) -> list:
    """Order a feature dict into the exact FEATURE_COLS sequence the model expects."""
    return [float(feat[c]) for c in FEATURE_COLS]


def snap_shuffle(value: float) -> int:
    """Snap a raw shuffle-partition value to the nearest allowed tier."""
    return min(SHUFFLE_TIERS, key=lambda t: abs(t - value))
