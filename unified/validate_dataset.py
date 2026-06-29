#!/usr/bin/env python3
"""
Validator + canonical renderers for planner_config_dataset.jsonl.

SOURCE OF TRUTH for correctness. Self-contained (imports no generator code).
The generator imports the renderers / settings / range helpers from here, so
there is exactly one definition of "what a correct row looks like".

Run:
    python validate_dataset.py [path]            # validate + report
    python validate_dataset.py [path] --quiet    # validate only

Exit 0 iff every row passes every rule.

Rules:
  Structural (kept):
    S1 num_containers == len(containers) == len(containers_to_create)
    S2 len(stages) == num_containers - 1
    S3 execution_order == [s.name for s in stages]
    S4 stage0 type==copy; rest notebook
    S5 copy datasets + notebook source/sink containers are consecutive
    S6 one dataset per container in order; roles source/intermediate.../sink
    S7 every referenced column is real or derived-before-use; sum/avg/max/min
       numeric; count uses '*'
    S8 samples keys == columns; values parse to their type
  New (this task):
    F1 recommended_settings == deterministic f(size_hint, has_aggregation); equals
       per-stage settings; row_count in size bucket range; mapping monotone
    F2 filter_condition uses ONE SQL grammar (=,!=,<,<=,>,>=,between,in); no
       function-call predicates (equals(/upper(...))
    F3 user_prompt == canonical render of config (char-exact prompt<->config)
    F4 no '='/'!='/'in' filter on a double column
    F5 sample values + numeric filter thresholds within each column's range
    F6 (dataset-level) pass-through notebook ratio <= MAX_PASSTHROUGH_RATIO
"""

import json
import re
import sys
from collections import Counter, defaultdict

# ─────────────────────────────────────────────────────────────────────────────
# Specs
# ─────────────────────────────────────────────────────────────────────────────
SIZE_ORDER = ["small (< 5MB)", "medium (5–50MB)", "large (50–200MB)", "xlarge (> 200MB)"]

_DIU   = {0: 2, 1: 4, 2: 8, 3: 16}
_NODE  = {0: "Standard_DS3_v2", 1: "Standard_D4s_v3", 2: "Standard_D8s_v3", 3: "Standard_D16s_v3"}
_WORKERS_TIERS = [1, 2, 4, 8, 16]
_SHUFFLE_TIERS = [8, 16, 32, 64, 128]

ROW_RANGE_BY_SIZE = {
    "small (< 5MB)":    (500, 49_000),
    "medium (5–50MB)":  (55_000, 980_000),
    "large (50–200MB)": (1_100_000, 9_500_000),
    "xlarge (> 200MB)": (11_000_000, 400_000_000),
}

NUMERIC_TYPES = {"integer", "double"}
ALLOWED_TYPES = {"string", "integer", "double", "timestamp"}

MAX_PASSTHROUGH_RATIO = 0.25  # F6

EDITABLE_SETTINGS = {
    "diu": [1, 2, 4, 8, 16, 32],
    "num_workers": [0, 1, 2, 4, 8, 16],
    "shuffle_partitions": [4, 8, 16, 32, 64, 128],
    "node_type": ["Standard_DS3_v2", "Standard_D4s_v3", "Standard_DS4_v2",
                  "Standard_D8s_v3", "Standard_DS5_v2", "Standard_D16s_v3",
                  "Standard_E16s_v3"],
}

OPWORD = {"avg": "average", "sum": "total", "max": "maximum", "min": "minimum"}

# F5/F-D: per-column realistic ranges. Most-specific substring entries first
# (first match wins). DOMAIN_OVERRIDES disambiguate columns whose realistic range
# depends on the domain (same name, different meaning) — keyed by the domain's
# first column. Both are exposed/overridable from the generator's CONFIG.
RANGES = [
    ("temperature", -40, 55), ("temp_c", -40, 55),
    ("humidity", 0, 100), ("battery", 0, 100), ("percent", 0, 100),
    ("wind_kph", 0, 200), ("precip", 0, 500),
    ("age", 0, 100),
    ("length_of_stay", 0, 60),
    ("latency", 0, 60000), ("dwell", 0, 60000), ("_ms", 0, 60000),
    ("session_sec", 5, 14400), ("duration_sec", 1, 86400), ("_sec", 0, 86400),
    ("delay_min", 0, 600), ("duration_min", 0, 1440),
    ("salary", 20000, 400000), ("bonus", 0, 100000),
    ("unit_cost", 0, 10000), ("price", 0, 10000), ("cost", 0, 10000),
    ("amount", 0, 10000), ("fare", 1, 500), ("spend", 0, 10000), ("bid", 0, 100),
    ("voltage", 0, 500), ("kwh", 0, 10000), ("weight", 0, 1000),
    ("distance_mi", 30, 6000), ("distance_km", 0.5, 50), ("trip_km", 0.5, 50),
    ("distance", 0, 5000), ("_mi", 30, 6000), ("_km", 0.5, 50),
    ("player_level", 1, 100), ("reorder_point", 0, 100000),
    ("likes", 0, 100000), ("shares", 0, 100000), ("comments", 0, 100000),
    ("clicks", 0, 100000), ("passengers", 0, 1000), ("quantity", 1, 1000),
    ("on_hand", 0, 100000), ("score", 0, 1_000_000), ("rows_processed", 0, 100000),
    ("bytes", 0, 100000), ("kg", 0, 1000),
]
DOMAIN_OVERRIDES = {
    "call_id": {"duration_sec": (1, 7200)},   # telecom calls <= 2h
    "run_id": {"duration_sec": (1, 86400)},   # pipeline runs <= 24h
}
TYPE_DEFAULT_RANGE = {"integer": (0, 1000), "double": (0.0, 1000.0)}


def explicit_range(col, first_col=None, ranges=RANGES, overrides=DOMAIN_OVERRIDES):
    """Range only if the column name matches an override/known pattern, else None."""
    if first_col and col in overrides.get(first_col, {}):
        return overrides[first_col][col]
    low = col.lower()
    for sub, lo, hi in ranges:
        if sub in low:
            return lo, hi
    return None


def column_range(col, typ, first_col=None, ranges=RANGES, overrides=DOMAIN_OVERRIDES):
    """Range with a per-type fallback (used by the generator to draw samples)."""
    r = explicit_range(col, first_col, ranges, overrides)
    return r if r is not None else TYPE_DEFAULT_RANGE.get(typ, (0, 1000))


# ─────────────────────────────────────────────────────────────────────────────
# F1: deterministic settings
# ─────────────────────────────────────────────────────────────────────────────
def expected_settings(size_hint, has_agg):
    i = SIZE_ORDER.index(size_hint)
    workers = _WORKERS_TIERS[i + 1] if has_agg else _WORKERS_TIERS[i]
    shuffle = _SHUFFLE_TIERS[i + 1] if has_agg else _SHUFFLE_TIERS[i]
    return {"diu": _DIU[i], "num_workers": workers,
            "shuffle_partitions": shuffle, "node_type": _NODE[i]}


def _assert_monotone():
    for has_agg in (False, True):
        prev = None
        for size in SIZE_ORDER:
            s = expected_settings(size, has_agg)
            if prev:
                assert s["diu"] >= prev["diu"]
                assert s["num_workers"] >= prev["num_workers"]
                assert s["shuffle_partitions"] >= prev["shuffle_partitions"]
            prev = s


_assert_monotone()


# ─────────────────────────────────────────────────────────────────────────────
# F3: canonical prompt renderer (single source of truth; generator imports this)
# ─────────────────────────────────────────────────────────────────────────────
def render_notebook_segment(stage):
    src, snk = stage["source_container"], stage["sink_container"]
    clauses = []
    for t in stage.get("transformations", []):
        if t == "distinct()":
            clauses.append("deduplicate rows")
        elif t.startswith("orderBy("):
            inner = t[len("orderBy("):-1]
            if inner.endswith(" desc"):
                clauses.append(f"sort by {inner[:-5]} descending")
            else:
                clauses.append(f"sort by {inner} ascending")
        elif "=" in t:
            lhs, rhs = (x.strip() for x in t.split("=", 1))
            clauses.append(f"derive {lhs} as {rhs}")
    if stage.get("filter_condition"):
        clauses.append(f"keep only rows where {stage['filter_condition']}")
    agg = stage.get("aggregation")
    if agg:
        phrases = []
        for a in agg["aggregations"]:
            phrases.append("a row count" if a["op"] == "count"
                           else f"{OPWORD[a['op']]} of {a['column']}")
        clauses.append(f"group by {agg['group_by'][0]} computing " + ", ".join(phrases))
    if not clauses:
        return f"carry data from {src} into {snk}"
    return f"in {snk}, " + ", ".join(clauses)


def render_prompt(config):
    ctc = config["containers_to_create"]
    segs = [f"ingest data from {ctc[0]} into {ctc[1]}"]
    for st in config["stages"][1:]:
        segs.append(render_notebook_segment(st))
    return "; then ".join(segs) + "."


# ─────────────────────────────────────────────────────────────────────────────
# Filter parsing (F2/F4/F5) + column-ref / derived-type inference (S7/F4)
# ─────────────────────────────────────────────────────────────────────────────
RESERVED = {
    "upper", "lower", "round", "cast", "concat", "currenttimestamp", "distinct",
    "orderby", "as", "double", "int", "integer", "string", "long", "float",
    "timestamp", "date", "boolean", "between", "and", "or", "not", "in",
    "true", "false", "asc", "desc", "null",
}


def col_refs(expr):
    e = re.sub(r"'[^']*'", " ", expr)
    out = []
    for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", e):
        if tok.lower() not in RESERVED:
            out.append(tok)
    return out


def derived_type(rhs, types_map):
    r = rhs.lower()
    if "upper(" in r or "lower(" in r or "concat(" in r:
        return "string"
    if "round(" in r:
        return "double"
    m = re.search(r"cast\(.+\bas\s+(\w+)\)", r)
    if m:
        return {"int": "integer"}.get(m.group(1), m.group(1))
    if re.search(r"[+\-*/]", rhs):
        return "double"
    bare = rhs.strip()
    if bare in types_map:
        return types_map[bare]
    return "double"


def parse_filter(cond):
    """(col, op, [vals]) or (None, None, None). op in =,!=,<,<=,>,>=,between,in."""
    c = cond.strip()
    m = re.match(r"^(\w+)\s+between\s+(-?\d+(?:\.\d+)?)\s+and\s+(-?\d+(?:\.\d+)?)$", c)
    if m:
        return m.group(1), "between", [m.group(2), m.group(3)]
    m = re.match(r"^(\w+)\s+in\s+\((.*)\)$", c)
    if m:
        vals = [v.strip().strip("'") for v in m.group(2).split(",")]
        return m.group(1), "in", vals
    m = re.match(r"^(\w+)\s*(<=|>=|!=|=|<|>)\s*(.+)$", c)
    if m:
        return m.group(1), m.group(2), [m.group(3).strip().strip("'")]
    return None, None, None


def _num(s):
    try:
        return float(s)
    except ValueError:
        return None


def apply_constraint(state, col, op, vals):
    """Fold one filter into the column's running feasible region (Fix A/B).

    Returns "ok", "dominated" (no-op vs an earlier same-direction filter), or
    "empty" (the chain can never match a row). state[col] holds a discrete
    allow/exclude set and a numeric interval; both are intersected over stages.
    """
    st = state.setdefault(col, {"allowed": None, "excluded": set(),
                                "lo": float("-inf"), "hi": float("inf"),
                                "lo_s": False, "hi_s": False})

    def feasible():
        if st["allowed"] is not None:
            eff = st["allowed"] - st["excluded"]
            if not eff:
                return False
            if st["lo"] > float("-inf") or st["hi"] < float("inf"):
                nums = [_num(x) for x in eff]
                if any(n is not None for n in nums):
                    ok = any(n is not None and st["lo"] <= n <= st["hi"]
                             and not (n == st["lo"] and st["lo_s"])
                             and not (n == st["hi"] and st["hi_s"]) for n in nums)
                    return ok
            return True
        if st["lo"] > st["hi"]:
            return False
        if st["lo"] == st["hi"] and (st["lo_s"] or st["hi_s"]):
            return False
        return True

    if op in ("<", "<="):
        v = _num(vals[0]); strict = (op == "<")
        if v is None:
            return "ok"
        tighten = v < st["hi"] or (v == st["hi"] and strict and not st["hi_s"])
        if not tighten:
            return "dominated"
        st["hi"], st["hi_s"] = v, strict
    elif op in (">", ">="):
        v = _num(vals[0]); strict = (op == ">")
        if v is None:
            return "ok"
        tighten = v > st["lo"] or (v == st["lo"] and strict and not st["lo_s"])
        if not tighten:
            return "dominated"
        st["lo"], st["lo_s"] = v, strict
    elif op == "between":
        a, b = _num(vals[0]), _num(vals[1])
        if a is not None:
            st["lo"] = max(st["lo"], a)
        if b is not None:
            st["hi"] = min(st["hi"], b)
    elif op == "=":
        s = {vals[0]}
        if st["allowed"] == s:
            return "dominated"
        st["allowed"] = s if st["allowed"] is None else st["allowed"] & s
    elif op == "in":
        s = set(vals)
        if st["allowed"] is not None and st["allowed"] <= s:
            return "dominated"
        st["allowed"] = s if st["allowed"] is None else st["allowed"] & s
    elif op == "!=":
        if vals[0] in st["excluded"]:
            return "dominated"
        st["excluded"].add(vals[0])
        if st["allowed"] is not None:
            st["allowed"] = st["allowed"] - {vals[0]}
    return "ok" if feasible() else "empty"


TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$")


# ─────────────────────────────────────────────────────────────────────────────
# Per-row validation
# ─────────────────────────────────────────────────────────────────────────────
def validate_row(rec):
    errs = []
    add = errs.append

    if not isinstance(rec, dict) or {"schema", "user_prompt", "config"} - rec.keys():
        return ["missing top-level keys"]
    schema, cfg, prompt = rec["schema"], rec["config"], rec["user_prompt"]

    for k in ("columns", "inferred_types", "row_count", "size_hint", "samples"):
        if k not in schema:
            add(f"schema missing '{k}'")
    if errs:
        return errs

    cols = schema["columns"]
    types_map = schema["inferred_types"]
    size_hint = schema["size_hint"]
    col_set = set(cols)

    if size_hint not in SIZE_ORDER:
        add(f"bad size_hint '{size_hint}'")
    if set(types_map) != col_set:
        add("inferred_types keys != columns")
    for c, t in types_map.items():
        if t not in ALLOWED_TYPES:
            add(f"col '{c}' bad type '{t}'")

    # S8 + F5 samples
    if not isinstance(schema["samples"], list) or len(schema["samples"]) != 3:
        add("samples must be list of 3")
    else:
        for si, s in enumerate(schema["samples"]):
            if set(s) != col_set:
                add(f"sample {si} keys != columns"); continue
            for c, v in s.items():
                t = types_map.get(c)
                if not isinstance(v, str):
                    add(f"sample {si} '{c}' not string"); continue
                if t == "integer":
                    try:
                        iv = int(v)
                    except ValueError:
                        add(f"sample {si} '{c}'='{v}' not integer"); continue
                    rng = explicit_range(c)
                    if rng and not (rng[0] <= iv <= rng[1]):
                        add(f"F5: sample {si} '{c}'={iv} outside range [{rng[0]},{rng[1]}]")
                elif t == "double":
                    fv = _num(v)
                    if fv is None:
                        add(f"sample {si} '{c}'='{v}' not double"); continue
                    rng = explicit_range(c)
                    if rng and not (rng[0] <= fv <= rng[1]):
                        add(f"F5: sample {si} '{c}'={fv} outside range [{rng[0]},{rng[1]}]")
                elif t == "timestamp" and not TS_RE.match(v):
                    add(f"sample {si} '{c}'='{v}' not timestamp")

    for k in ("containers", "containers_to_create", "datasets", "stages",
              "execution_order", "num_containers", "recommended_settings",
              "editable_settings", "reasoning"):
        if k not in cfg:
            add(f"config missing '{k}'")
    if errs:
        return errs

    containers, ctc = cfg["containers"], cfg["containers_to_create"]
    datasets, stages, n = cfg["datasets"], cfg["stages"], cfg["num_containers"]

    # S1
    if not (n == len(containers) == len(ctc)):
        add(f"S1: counts {n}/{len(containers)}/{len(ctc)}")
    if list(containers.keys()) != [f"stage{i}" for i in range(len(ctc))] or \
       [containers[k] for k in containers] != ctc:
        add("S1: containers dict != containers_to_create")
    # S2
    if len(stages) != n - 1:
        add(f"S2: stages {len(stages)} != {n-1}")
    # S3
    if cfg["execution_order"] != [s.get("name") for s in stages]:
        add("S3: execution_order mismatch")
    # S6
    if [d.get("container") for d in datasets] != ctc:
        add("S6: dataset containers != order")
    else:
        for i, d in enumerate(datasets):
            want = "source" if i == 0 else "sink" if i == len(datasets) - 1 else "intermediate"
            if d.get("role") != want:
                add(f"S6: dataset {i} role {d.get('role')} != {want}")
    if errs:
        return errs
    ds_by_c = {d["container"]: d["name"] for d in datasets}

    # S4 + S5
    if stages[0].get("type") != "copy":
        add("S4: stage0 not copy")
    for s in stages[1:]:
        if s.get("type") != "notebook":
            add(f"S4: '{s.get('name')}' not notebook")
    cp = stages[0]
    if cp.get("source_dataset") != ds_by_c.get(ctc[0]):
        add("S5: copy.source_dataset wrong")
    if cp.get("sink_dataset") != ds_by_c.get(ctc[1]):
        add("S5: copy.sink_dataset wrong")
    for i, s in enumerate(stages[1:], start=1):
        if s.get("source_container") != ctc[i] or s.get("sink_container") != ctc[i + 1]:
            add(f"S5: '{s.get('name')}' containers wrong")

    # F1 settings
    has_agg = any("aggregation" in s for s in stages)
    exp = expected_settings(size_hint, has_agg) if size_hint in SIZE_ORDER else None
    rec_s = cfg["recommended_settings"]
    if exp and rec_s != exp:
        add(f"F1: recommended_settings {rec_s} != deterministic {exp}")
    edit = cfg["editable_settings"]
    for k in ("diu", "num_workers", "shuffle_partitions", "node_type"):
        if rec_s.get(k) not in edit.get(k, []):
            add(f"F1: editable[{k}] missing recommended {rec_s.get(k)}")
    lo, hi = ROW_RANGE_BY_SIZE.get(size_hint, (0, 1 << 62))
    if not (lo <= schema["row_count"] <= hi):
        add(f"F1: row_count {schema['row_count']} outside {size_hint} range")
    if cp.get("diu") != rec_s.get("diu"):
        add("F1: copy diu != recommended")
    for s in stages[1:]:
        if s.get("num_workers") != rec_s.get("num_workers"):
            add(f"F1: '{s.get('name')}' num_workers != recommended")
        if s.get("shuffle_partitions") != rec_s.get("shuffle_partitions"):
            add(f"F1: '{s.get('name')}' shuffle_partitions != recommended")

    # S7 lineage + F2/F4/F5 filters + FA/FB/FC/FE semantic checks
    first_col = cols[0]
    known = dict(types_map)
    derived_names = []           # FB: every derived column name in the pipeline
    constraints = {}             # FA/FB: per-column running feasible region
    has_real_op = False          # FE: at least one filter/derive/agg anywhere
    for s in stages[1:]:
        for t in s.get("transformations", []):
            if "=" in t:  # assignment (distinct()/orderBy(...) contain no '=')
                has_real_op = True
                lhs, rhs = (x.strip() for x in t.split("=", 1))
                for r in col_refs(rhs):
                    if r not in known:
                        add(f"S7: '{s.get('name')}' transform refs unknown '{r}'")
                # FC: identity rename — RHS is a bare existing column, no op applied
                if re.fullmatch(r"[A-Za-z_]\w*", rhs) and rhs in known:
                    add(f"FC: '{s.get('name')}' identity rename '{t}'")
                # FB: duplicate derived column name
                if lhs in derived_names:
                    add(f"FB: derived column '{lhs}' defined more than once")
                derived_names.append(lhs)
                known[lhs] = derived_type(rhs, known)
            else:  # distinct()/orderBy(...)
                has_real_op = True
                for r in col_refs(t):
                    if r not in known:
                        add(f"S7: '{s.get('name')}' transform refs unknown '{r}'")

        fc = s.get("filter_condition")
        if fc:
            has_real_op = True
            # F2 grammar: no function-call predicates, no '=='
            if re.search(r"[A-Za-z_]\(", fc) or "==" in fc:
                add(f"F2: filter '{fc}' not SQL grammar")
            fcol, fop, fvals = parse_filter(fc)
            if fcol is None:
                add(f"F2: unparseable filter '{fc}'")
            else:
                if fcol not in known:
                    add(f"S7: filter refs unknown col '{fcol}'")
                ftype = known.get(fcol)
                # F4: no =/!=/in on double
                if ftype == "double" and fop in ("=", "!=", "in"):
                    add(f"F4: '{fop}' filter on double col '{fcol}'")
                # F5: thresholds within range (original numeric cols with a known range)
                if fcol in types_map and types_map[fcol] in NUMERIC_TYPES:
                    rng = explicit_range(fcol, first_col)
                    if rng:
                        for v in fvals:
                            nv = _num(v)
                            if nv is not None and not (rng[0] <= nv <= rng[1]):
                                add(f"F5: filter threshold {nv} on '{fcol}' outside [{rng[0]},{rng[1]}]")
                # FA/FB: fold into the column's running feasible region
                res = apply_constraint(constraints, fcol, fop, fvals)
                if res == "empty":
                    add(f"FA: contradictory filter chain on '{fcol}' (always empty) at '{fc}'")
                elif res == "dominated":
                    add(f"FB: dominated/no-op filter on '{fcol}' at '{fc}'")

        agg = s.get("aggregation")
        if agg:
            has_real_op = True
            for g in agg.get("group_by", []):
                if g not in known:
                    add(f"S7: group_by unknown '{g}'")
            for a in agg.get("aggregations", []):
                c, op = a.get("column"), a.get("op")
                if c == "*":
                    if op != "count":
                        add(f"S7: '*' with non-count '{op}'")
                    continue
                if c not in known:
                    add(f"S7: aggregation refs unknown '{c}'")
                elif op in ("sum", "avg", "max", "min") and known.get(c) not in NUMERIC_TYPES:
                    add(f"S7: {op} on non-numeric '{c}'")
                if a.get("alias"):
                    known[a["alias"]] = "double"

    # FE: a pipeline must contain at least one real operation somewhere
    if not has_real_op:
        add("FE: pipeline has no real operation (all pass-through)")

    # F3 prompt exactness
    expected_prompt = render_prompt(cfg)
    if prompt != expected_prompt:
        add(f"F3: prompt != canonical render\n        prompt:   {prompt}\n        expected: {expected_prompt}")

    if not isinstance(cfg["reasoning"], str) or len(cfg["reasoning"]) < 10:
        add("reasoning missing/short")

    return errs


# ─────────────────────────────────────────────────────────────────────────────
# Dataset-level checks (F6) + diversity report
# ─────────────────────────────────────────────────────────────────────────────
CONTAINER_SCHEMES = {
    "medallion": ["raw", "bronze", "silver", "gold", "platinum"],
    "lakehouse": ["landing", "staging", "curated", "serving"],
    "elt": ["ingest", "clean", "enrich", "mart"],
    "generic": ["l0", "l1", "l2", "l3", "l4", "l5"],
}


def detect_scheme(ctc):
    for name, seq in CONTAINER_SCHEMES.items():
        if all(c in seq for c in ctc):
            return name
    return "other"


def is_passthrough(stage):
    return not stage.get("transformations") and not stage.get("filter_condition") \
        and "aggregation" not in stage


def classify_transforms(stages):
    kinds = []
    for s in stages[1:]:
        any_op = False
        if "aggregation" in s:
            kinds.append("aggregation"); any_op = True
        if s.get("filter_condition"):
            kinds.append("filter"); any_op = True
        for t in s.get("transformations", []):
            tl = t.lower()
            if t.startswith("processed_time"):
                kinds.append("timestamp_stamp")
            elif "upper(" in tl or "lower(" in tl:
                kinds.append("normalize")
            elif "round(" in tl:
                kinds.append("round")
            elif "cast(" in tl:
                kinds.append("cast")
            elif "concat(" in tl:
                kinds.append("concat")
            elif "distinct(" in tl:
                kinds.append("dedup")
            elif "orderby(" in tl:
                kinds.append("sort")
            elif "=" in t:
                kinds.append("derive_arith" if re.search(r"[+\-*/]", t.split("=", 1)[1]) else "rename")
            any_op = True
        if not any_op:
            kinds.append("passthrough")
    return kinds


def report(records):
    total = len(records)
    domain = Counter(r["schema"]["columns"][0] for r in records)
    stage_count = Counter(len(r["config"]["stages"]) for r in records)
    scheme = Counter(detect_scheme(r["config"]["containers_to_create"]) for r in records)
    size = Counter(r["schema"]["size_hint"] for r in records)
    ncols = Counter(len(r["schema"]["columns"]) for r in records)
    tkind = Counter()
    for r in records:
        tkind.update(classify_transforms(r["config"]["stages"]))

    # F1: settings identical within (size, has_agg)
    groups = defaultdict(set)
    for r in records:
        st = r["config"]["stages"]
        ha = any("aggregation" in s for s in st)
        key = (r["schema"]["size_hint"], ha)
        groups[key].add(json.dumps(r["config"]["recommended_settings"], sort_keys=True))

    # filter grammar usage
    func_filters = sum(1 for r in records for s in r["config"]["stages"][1:]
                       if s.get("filter_condition") and re.search(r"[A-Za-z_]\(", s["filter_condition"]))

    def show(title, counter, by_key=False):
        print(f"\n{title}")
        items = sorted(counter.items()) if by_key else counter.most_common()
        for k, v in items:
            print(f"  {str(k):<22} {v:>6}  {100.0*v/total:5.1f}%  {'█'*int(50.0*v/total)}")

    print("\n" + "=" * 60 + f"\nDIVERSITY REPORT  ({total} rows)\n" + "=" * 60)
    show("By domain (first column):", domain)
    show("By stage count:", stage_count, by_key=True)
    show("By container scheme:", scheme)
    show("By size bucket:", size)
    show("By column count:", ncols, by_key=True)
    show("By transform type (per stage):", tkind)

    print("\n" + "-" * 60)
    print("F1  settings identical within (size_hint, has_aggregation):")
    for key in sorted(groups, key=lambda k: (SIZE_ORDER.index(k[0]), k[1])):
        s = sorted(groups[key])
        flag = "OK" if len(s) == 1 else "FAIL"
        print(f"  {flag}  {key[0]:<18} agg={str(key[1]):<5} -> {s[0] if s else '{}'}"
              + ("" if len(s) == 1 else f"  ({len(s)} distinct!)"))
    # FA–FE semantic tallies (all must be 0 on a passing dataset)
    contradictory = dominated = identity = dup_derived = nooppipe = oor = 0
    for r in records:
        st = r["config"]["stages"]
        types_map = r["schema"]["inferred_types"]
        first_col = r["schema"]["columns"][0]
        known = dict(types_map)
        dnames, cons = [], {}
        real = False
        for s in st[1:]:
            for t in s.get("transformations", []):
                if "=" in t:
                    real = True
                    lhs, rhs = (x.strip() for x in t.split("=", 1))
                    if re.fullmatch(r"[A-Za-z_]\w*", rhs) and rhs in known:
                        identity += 1
                    if lhs in dnames:
                        dup_derived += 1
                    dnames.append(lhs); known[lhs] = derived_type(rhs, known)
                else:
                    real = True
            if s.get("filter_condition"):
                real = True
                fcol, fop, fvals = parse_filter(s["filter_condition"])
                if fcol:
                    res = apply_constraint(cons, fcol, fop, fvals)
                    contradictory += res == "empty"
                    dominated += res == "dominated"
                    if fcol in types_map and types_map[fcol] in NUMERIC_TYPES:
                        rng = explicit_range(fcol, first_col)
                        if rng:
                            for v in fvals:
                                nv = _num(v)
                                if nv is not None and not (rng[0] <= nv <= rng[1]):
                                    oor += 1
            if "aggregation" in s:
                real = True
        if not real:
            nooppipe += 1
    print("\nSemantic checks (must all be 0):")
    print(f"  FA contradictory filter chains : {contradictory}")
    print(f"  FB dominated/no-op filters     : {dominated}")
    print(f"  FB duplicate derived columns   : {dup_derived}")
    print(f"  FC identity renames            : {identity}")
    print(f"  FD out-of-range thresholds     : {oor}")
    print(f"  FE all-pass-through pipelines  : {nooppipe}")
    print(f"\nF2  function-style filters remaining : {func_filters} (must be 0)")
    print(f"max single domain share : {100.0*domain.most_common(1)[0][1]/total:.1f}%")
    print(f"size buckets present    : {len(size)}/4   stage counts: {sorted(stage_count)}")
    print(f"schemes used            : {sorted(scheme)}")


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    quiet = "--quiet" in sys.argv
    path = args[0] if args else "planner_config_dataset.jsonl"

    records, failures = [], []
    with open(path, encoding="utf-8") as f:
        for idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as e:
                failures.append((idx, [f"invalid JSON: {e}"])); continue
            records.append(rec)
            row_errs = validate_row(rec)
            if row_errs:
                failures.append((idx, row_errs))

    print(f"validated {len(records)} rows from {path}")

    # F6 dataset-level pass-through ratio
    nb = sum(len(r["config"]["stages"]) - 1 for r in records)
    pt = sum(1 for r in records for s in r["config"]["stages"][1:] if is_passthrough(s))
    ratio = pt / nb if nb else 0.0
    print(f"F6 pass-through ratio: {pt}/{nb} = {ratio:.1%} (max {MAX_PASSTHROUGH_RATIO:.0%})")
    f6_fail = ratio > MAX_PASSTHROUGH_RATIO

    if failures or f6_fail:
        for idx, errs in failures[:40]:
            print(f"  row {idx}:")
            for e in errs:
                print(f"      - {e}")
        if len(failures) > 40:
            print(f"  ... +{len(failures)-40} more failing rows")
        if f6_fail:
            print(f"  F6: pass-through ratio {ratio:.1%} exceeds {MAX_PASSTHROUGH_RATIO:.0%}")
        print(f"\nFAIL: {len(failures)} row failure(s)" + (" + F6" if f6_fail else ""))
        return 1

    print("PASS: all rows valid (0 violations)")
    if not quiet:
        report(records)
    return 0


if __name__ == "__main__":
    sys.exit(main())
