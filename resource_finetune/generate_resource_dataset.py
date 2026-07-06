#!/usr/bin/env python3
"""
Generate a synthetic fine-tuning dataset for the RESOURCE AGENT.

Mirrors the planner's synthetic-dataset approach (procedural, seeded, dedup'd),
but the labelling strategy is different: instead of hand-rolling the target JSON,
we drive the *real* ResourceAgent.analyze() as a labelling ORACLE. Every example
is therefore exactly self-consistent with the student-tier hard limits and the
cost/throughput model the agent implements.

Each record:
    {
      "schema":           {columns, inferred_types, row_count, size_hint},
      "csv_size_bytes":   int,
      "input_plan":       {num_containers, containers_to_create,
                           recommended_settings, execution_order, stages[]},
      "execution_groups": [[stage_name, ...], ...],   # intended parallelism
      "resource_plan":    { ...ResourceAgent.analyze() output... }   # the label
    }

Grounding in the ORIGINAL dataset (Datasets/Datasets/Original, read via its
cleaned CSV mirror in Datasets/Datasets/Cleaned):
  * real table column schemas become the input CSV schema,
  * real workspace names and source-table names seed the pipeline,
  * real query-complexity flags (has_where / has_group_by / has_join) set the
    per-stage filter / aggregation probabilities.

Usage:
    python resource_finetune/generate_resource_dataset.py --rows 1500
"""

import argparse
import json
import os
import sys
import tempfile

# ── Make the real ResourceAgent importable (it lives in unified/) ────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
sys.path.insert(0, os.path.join(_REPO, "unified"))

from resource_agent import (  # noqa: E402
    ResourceAgent,
    NODE_SPECS,
    MAX_WORKERS,
    MAX_DIU,
)
import resource_agent.resource_agent as _ra_mod  # noqa: E402

# Point the feedback log at a path that does not exist so correction factors are
# a clean, deterministic 1.0 for every generated label (no environment drift).
_ra_mod._FEEDBACK_LOG = os.path.join(tempfile.gettempdir(), "no_such_resource_feedback.jsonl")

_CLEANED_DIR = os.path.join(_REPO, "Datasets", "Datasets", "Cleaned")
_ORIGINAL_DIR = os.path.join(_REPO, "Datasets", "Datasets", "Original")
_OUT_DEFAULT = os.path.join(_REPO, "Datasets", "Datasets", "Synthetic",
                            "synthetic_resource_dataset.json")

MB = 1024 * 1024

# ── Size buckets: (size_hint, row_range, byte_range) ─────────────────────────
SIZE_BUCKETS = [
    ("small (< 5MB)",     (500,        49_000),      (1 * MB,   5 * MB)),
    ("medium (5–50MB)",   (55_000,     980_000),     (5 * MB,   50 * MB)),
    ("large (50–200MB)",  (1_100_000,  9_500_000),   (50 * MB,  200 * MB)),
    ("xlarge (> 200MB)",  (11_000_000, 120_000_000), (200 * MB, 1200 * MB)),
]
SIZE_WEIGHTS = [0.30, 0.34, 0.22, 0.14]

# ── Container schemes (medallion etc.) ───────────────────────────────────────
CONTAINER_SCHEMES = {
    "medallion": ["raw", "bronze", "silver", "gold", "platinum", "diamond"],
    "lakehouse": ["landing", "staging", "curated", "serving", "mart", "sandbox"],
    "elt":       ["ingest", "clean", "enrich", "conform", "mart", "serving"],
    "generic":   ["l0", "l1", "l2", "l3", "l4", "l5"],
}
SCHEME_ITEMS = list(CONTAINER_SCHEMES.items())
SCHEME_WEIGHTS = [0.50, 0.20, 0.20, 0.10]

# Verbs used to name notebook (transform) stages.
NOTEBOOK_VERBS = ["clean", "enrich", "curate", "features", "aggregate",
                  "normalize", "dedup", "stage", "conform", "transform"]

NODE_KEYS = list(NODE_SPECS.keys())
# Bias toward the smaller nodes the Planner actually recommends, so feasibility
# is driven by data size × parallelism rather than by node RAM alone.
NODE_WEIGHTS = [NODE_SPECS[k]["memory_gb"] for k in NODE_KEYS]
NODE_WEIGHTS = [1.0 / (m ** 1.3) for m in NODE_WEIGHTS]


# ── Reference grounding from the original / cleaned dataset ───────────────────
def _pdtype(dtype) -> str:
    s = str(dtype)
    if "int" in s:
        return "integer"
    if "float" in s:
        return "double"
    if "datetime" in s:
        return "timestamp"
    return "string"


def _fallback_ref() -> dict:
    return {
        "table_schemas": [
            {"name": "job_runs",
             "columns": ["result_state", "workspace_name", "job_name",
                         "duration_seconds", "start_hour", "start_day"],
             "inferred_types": {"result_state": "string", "workspace_name": "string",
                                "job_name": "string", "duration_seconds": "double",
                                "start_hour": "double", "start_day": "double"},
             "numeric": ["duration_seconds", "start_hour", "start_day"],
             "categorical": ["result_state", "workspace_name", "job_name"]},
            {"name": "queries",
             "columns": ["statement_type", "duration_seconds", "text_length",
                         "word_count", "has_join", "has_aggregation"],
             "inferred_types": {"statement_type": "string", "duration_seconds": "double",
                                "text_length": "integer", "word_count": "integer",
                                "has_join": "integer", "has_aggregation": "integer"},
             "numeric": ["duration_seconds", "text_length", "word_count"],
             "categorical": ["statement_type"]},
        ],
        "workspaces": ["workspace1", "workspace2", "workspace3", "prod", "dev", "qa"],
        "source_tables": ["orders", "inventory", "truck", "journal", "sales",
                          "events", "txn", "customers"],
        "p_filter": 0.60, "p_agg": 0.35, "p_join": 0.30,
    }


def load_reference(cleaned_dir: str) -> dict:
    """Best-effort grounding in the real data; falls back cleanly if unavailable."""
    try:
        import pandas as pd
    except Exception:
        print("[ref] pandas unavailable — using built-in fallback pools")
        return _fallback_ref()

    ref = {"table_schemas": [], "workspaces": [], "source_tables": [],
           "p_filter": 0.60, "p_agg": 0.35, "p_join": 0.30}

    files = {
        "job_runs": "job_runs_cleaned.csv",
        "pipeline_runs": "pipeline_runs_cleaned.csv",
        "queries": "queries_cleaned.csv",
        "dbquery_statistics": "dbquery_statistics_cleaned.csv",
        "utilization": "utilization_cleaned.csv",
    }
    for name, fn in files.items():
        path = os.path.join(cleaned_dir, fn)
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_csv(path, nrows=1000)
        except Exception:
            continue
        cols = [str(c) for c in df.columns]
        types = {c: _pdtype(df[c].dtype) for c in cols}
        numeric = [c for c in cols if types[c] in ("integer", "double")]
        categorical = [c for c in cols if types[c] == "string"]
        ref["table_schemas"].append({
            "name": name, "columns": cols, "inferred_types": types,
            "numeric": numeric, "categorical": categorical,
        })

    # Workspaces + source-table names from job_runs.
    try:
        jr = pd.read_csv(os.path.join(cleaned_dir, "job_runs_cleaned.csv"),
                         usecols=["workspace_name", "job_name"], nrows=60000)
        ref["workspaces"] = sorted({str(x) for x in jr["workspace_name"].dropna()})[:8]
        prefix = "wf_copy_csvfiles_to_csvlake_prod_"
        tabs = set()
        for jn in jr["job_name"].dropna().astype(str).unique()[:600]:
            t = jn.split(prefix)[-1] if prefix in jn else jn
            t = t.strip("_").split("_")[-1]
            if t.isalpha() and 3 <= len(t) <= 24:
                tabs.add(t.lower())
        if tabs:
            ref["source_tables"] = sorted(tabs)[:40]
    except Exception:
        pass

    # Query-complexity probabilities → drive filter/aggregation frequency.
    try:
        q = pd.read_csv(os.path.join(cleaned_dir, "queries_cleaned.csv"),
                        usecols=["has_where", "has_aggregation", "has_group_by", "has_join"],
                        nrows=60000)

        def _p(col, default):
            try:
                return float(min(0.9, max(0.15, q[col].mean())))
            except Exception:
                return default

        # Floor at 0.30 so there are enough filter/aggregation examples for the
        # model to learn their duration impact, while still tracking the real mix.
        ref["p_filter"] = max(0.30, _p("has_where", 0.60))
        ref["p_agg"] = max(0.30, _p("has_group_by", 0.35), _p("has_aggregation", 0.35))
        ref["p_join"] = _p("has_join", 0.30)
    except Exception:
        pass

    fb = _fallback_ref()
    if not ref["table_schemas"]:
        ref["table_schemas"] = fb["table_schemas"]
    if not ref["workspaces"]:
        ref["workspaces"] = fb["workspaces"]
    if not ref["source_tables"]:
        ref["source_tables"] = fb["source_tables"]
    return ref


# ── Plan construction ────────────────────────────────────────────────────────
def _pick_weighted(rng, items, weights):
    return rng.choices(items, weights=weights, k=1)[0]


def build_notebook_ops(schema_ref, rng, ref, is_last):
    """Return (transformations, filter_condition, aggregations) for a notebook stage."""
    numeric = schema_ref["numeric"] or ["value"]
    categorical = schema_ref["categorical"] or ["category"]

    # Transformations (only the count matters to the agent; keep values realistic).
    n_tx = rng.choices([0, 1, 2, 3, 4, 5, 6, 8],
                       weights=[6, 12, 16, 16, 14, 10, 8, 4], k=1)[0]
    transforms = []
    if n_tx:
        transforms.append("processed_time = currentTimestamp()")
        pool = list(categorical) + list(numeric)
        for i in range(n_tx - 1):
            c = rng.choice(pool)
            if c in numeric:
                transforms.append(f"{c}_scaled = {c} * {rng.choice([2, 10, 100])}")
            else:
                transforms.append(f"{c}_norm = upper({c})")

    # Filter.
    filt = None
    if rng.random() < ref["p_filter"]:
        c = rng.choice(numeric)
        op = rng.choice([">", "<", ">=", "<="])
        filt = f"{c} {op} {rng.choice([0, 1, 10, 100, 1000])}"

    # Aggregation (a touch more likely on the final hop).
    aggs = None
    p_agg = ref["p_agg"] * (1.25 if is_last else 1.0)
    if rng.random() < p_agg:
        k = rng.randint(1, min(3, len(numeric)))
        cols = rng.sample(numeric, k)
        exprs = []
        for c in cols:
            exprs.append(f"{rng.choice(['sum', 'avg', 'max', 'min'])}({c})")
        exprs.append("count(*)")
        aggs = {"agg_exprs": exprs}

    return transforms, filt, aggs


def build_plan(rng, ref):
    """Assemble a Planner-style pipeline plan + schema + csv size."""
    scheme_name, containers_full = _pick_weighted(rng, SCHEME_ITEMS, SCHEME_WEIGHTS)
    ncont = min(rng.choice([3, 4, 4, 5, 5, 6]), len(containers_full))
    ncont = max(3, ncont)
    containers = containers_full[:ncont]

    # Size / rows.
    size_hint, (rlo, rhi), (blo, bhi) = _pick_weighted(rng, SIZE_BUCKETS, SIZE_WEIGHTS)
    row_count = rng.randint(rlo, rhi)
    csv_size_bytes = rng.randint(blo, bhi)

    # Input CSV schema (real columns from a real table).
    tbl = rng.choice(ref["table_schemas"])
    schema = {
        "columns": tbl["columns"],
        "inferred_types": tbl["inferred_types"],
        "row_count": row_count,
        "size_hint": size_hint,
    }

    # recommended_settings (Planner's suggestion; caps allocations).
    rec = {
        "num_workers": rng.choice([0, 1, 2, 2, 3, 4, 4, 6]),
        "node_type": rng.choices(NODE_KEYS, weights=NODE_WEIGHTS, k=1)[0],
        "diu": rng.choice([2, 4, 4, 8, 8, 12, 16]),
        "shuffle_partitions": rng.choice([8, 16, 32, 64]),
    }

    # Stages: stage0 = ADF copy, stages 1.. = Databricks notebooks.
    src_tbl = rng.choice(ref["source_tables"])
    verbs = rng.sample(NOTEBOOK_VERBS, min(ncont - 1, len(NOTEBOOK_VERBS)))
    stages, exec_order = [], []

    ingest_name = "ingest"
    stages.append({
        "name": ingest_name, "type": "copy",
        "source_dataset": f"DS_{src_tbl}",
        "sink_dataset": f"DS_{containers[1].capitalize()}",
        # Sometimes over-request DIU (> MAX_DIU) to exercise clamping/warnings.
        "diu": rng.choice([2, 4, 6, 8, 8, 10, 12, 16]),
    })
    exec_order.append(ingest_name)

    for i in range(1, ncont - 1):
        name = verbs[i - 1] if i - 1 < len(verbs) else f"stage{i}"
        if name in exec_order:
            name = f"{name}{i}"
        tx, filt, aggs = build_notebook_ops(rng.choice(ref["table_schemas"]), rng, ref,
                                            is_last=(i == ncont - 2))
        stage = {
            "name": name, "type": "notebook",
            "source_container": containers[i],
            "sink_container": containers[i + 1],
            # Sometimes over-request workers (> MAX_WORKERS) to exercise clamping.
            "num_workers": rng.choice([0, 1, 2, 2, 3, 3, 4, 5, 6]),
            "node_type": rec["node_type"],
            "transformations": tx,
        }
        if filt:
            stage["filter_condition"] = filt
        if aggs:
            stage["aggregations"] = aggs
        stages.append(stage)
        exec_order.append(name)

    plan = {
        "num_containers": ncont,
        "containers_to_create": containers,
        "recommended_settings": rec,
        "execution_order": exec_order,
        "stages": stages,
    }
    return plan, schema, csv_size_bytes


def build_execution_groups(stage_names, rng):
    """Copy stage runs alone; notebooks are sometimes bundled into parallel groups
    (occasionally oversized) so labels cover contention resolution + enforcement."""
    if not stage_names:
        return []
    groups = [[stage_names[0]]]
    rest = stage_names[1:]
    if not rest:
        return groups
    if rng.random() < 0.45 or len(rest) == 1:
        groups.extend([[n] for n in rest])          # fully sequential
        return groups
    i = 0
    while i < len(rest):
        size = rng.choice([1, 2, 2, 3, 3, 4])        # a few busts of MAX_CONCURRENT
        groups.append(rest[i:i + size])
        i += size
    return groups


def content_key(plan, groups, csv_size_bytes, row_count):
    return json.dumps({"p": plan, "g": groups, "b": csv_size_bytes, "r": row_count},
                      sort_keys=True)


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1500, help="number of examples")
    ap.add_argument("--seed", type=int, default=3407)
    ap.add_argument("--out", default=_OUT_DEFAULT)
    args = ap.parse_args()

    import random
    rng = random.Random(args.seed)

    ref = load_reference(_CLEANED_DIR)
    print(f"[ref] tables={[t['name'] for t in ref['table_schemas']]} "
          f"workspaces={len(ref['workspaces'])} source_tables={len(ref['source_tables'])} "
          f"p_filter={ref['p_filter']:.2f} p_agg={ref['p_agg']:.2f}")

    agent = ResourceAgent()
    records, seen, attempts = [], set(), 0
    max_attempts = args.rows * 60

    stats = {"feasible": 0, "with_warnings": 0, "contention": 0,
             "clamped": 0, "stages": 0}

    while len(records) < args.rows and attempts < max_attempts:
        attempts += 1
        plan, schema, csv_size_bytes = build_plan(rng, ref)
        groups = build_execution_groups([s["name"] for s in plan["stages"]], rng)

        key = content_key(plan, groups, csv_size_bytes, schema["row_count"])
        if key in seen:
            continue

        try:
            rp = agent.analyze(plan, csv_size_bytes=csv_size_bytes,
                               schema={"row_count": schema["row_count"]},
                               execution_groups=[list(g) for g in groups])
        except Exception as exc:
            continue  # skip any pathological combo rather than abort the run

        seen.add(key)
        records.append({
            "schema": schema,
            "csv_size_bytes": csv_size_bytes,
            "input_plan": plan,
            "execution_groups": groups,
            "resource_plan": rp,
        })

        # Stats for a quick sanity read-out.
        stats["stages"] += len(plan["stages"])
        if rp["feasible"]:
            stats["feasible"] += 1
        if rp["warnings"]:
            stats["with_warnings"] += 1
        if any(a["contention_adjusted"] for a in rp["allocations"]):
            stats["contention"] += 1
        if any("clamped" in w for w in rp["warnings"]):
            stats["clamped"] += 1

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    n = len(records)
    print(f"\nWrote {n} records -> {args.out}  ({attempts} attempts)")
    if n:
        print(f"  feasible:        {stats['feasible']}/{n} ({100*stats['feasible']//n}%)")
        print(f"  with warnings:   {stats['with_warnings']}/{n} ({100*stats['with_warnings']//n}%)")
        print(f"  contention-adj:  {stats['contention']}/{n} ({100*stats['contention']//n}%)")
        print(f"  clamp warnings:  {stats['clamped']}/{n} ({100*stats['clamped']//n}%)")
        print(f"  avg stages/plan: {stats['stages']/n:.2f}")


if __name__ == "__main__":
    main()
