#!/usr/bin/env python3
"""
Configurable, seeded generator for the planner-agent synthetic dataset.

Output: planner_config_dataset.jsonl  (one {schema, user_prompt, config} per line).
Format unchanged; the training notebook still parses it.

Correctness is shared with validate_dataset.py — this generator IMPORTS the
canonical pieces from there so the two can never drift:
  * render_prompt(config)      -> the prompt is a pure function of the config
                                   (Fix 3: prompt<->config is char-exact)
  * expected_settings(size,agg)-> deterministic resources (Fix 1)
  * column_range / RANGES       -> realistic value ranges (Fix 5)
  * EDITABLE_SETTINGS

Logical guarantees:
  * one SQL filter grammar everywhere: =, !=, <, <=, >, >=, between, in (Fix 2)
  * no =/!=/in on double columns — only range predicates (Fix 4)
  * sample values and numeric thresholds bounded to each column's range (Fix 5)
  * resources are a deterministic function of size_hint (+ aggregation bump) and
    are echoed into every stage (Fix 1)
  * pass-through notebook stages kept rare via work_prob (Fix 6)
  * derived columns defined before use; aggregation only on the final stage.

Everything tunable lives in CONFIG below.
    python generate_dataset.py --rows 2000 --seed 7 --out planner_config_dataset.jsonl
"""

import argparse
import json
import math
import random
import sys

from validate_dataset import (render_prompt, expected_settings, column_range,
                              parse_filter, RANGES, DOMAIN_OVERRIDES,
                              EDITABLE_SETTINGS, OPWORD)

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║                              CONFIG                                        ║
# ╚══════════════════════════════════════════════════════════════════════════╝
CONFIG = {
    "num_rows": 5000,
    "seed": 20260628,
    "output_path": "planner_config_dataset.jsonl",

    "size_dist": {"small (< 5MB)": 0.30, "medium (5–50MB)": 0.30,
                  "large (50–200MB)": 0.20, "xlarge (> 200MB)": 0.20},
    "row_range_by_size": {
        "small (< 5MB)": (500, 49_000), "medium (5–50MB)": (55_000, 980_000),
        "large (50–200MB)": (1_100_000, 9_500_000), "xlarge (> 200MB)": (11_000_000, 400_000_000)},

    "num_containers_dist": {3: 0.35, 4: 0.30, 5: 0.20, 6: 0.15},
    "container_schemes": {
        "medallion": ["raw", "bronze", "silver", "gold", "platinum"],
        "lakehouse": ["landing", "staging", "curated", "serving"],
        "elt": ["ingest", "clean", "enrich", "mart"],
        "generic": ["l0", "l1", "l2", "l3", "l4", "l5"]},
    "scheme_weights": {"medallion": 0.4, "lakehouse": 0.25, "elt": 0.25, "generic": 0.2},

    # Fix 6: per-notebook-stage probability of doing real work (else pass-through).
    "work_prob": 0.85,          # earlier hops
    "final_work_prob": 0.95,    # final hop
    "max_passthrough_ratio": 0.25,

    "agg_prob": 0.30,           # chance the FINAL stage is an aggregation
    "processed_time_prob": 0.06,

    "op_weights": {
        "filter_measure": 1.6, "filter_flag": 0.8, "filter_catint": 0.7,
        "filter_cat_eq": 1.0, "filter_cat_in": 0.7, "convert": 1.3, "round": 0.7,
        "cast": 0.7, "normalize": 1.0, "concat": 0.7,
        "dedup": 0.6, "sort": 0.6},

    # editable lists + value ranges come from validate_dataset (single source);
    # copied here so they're visible/overridable from CONFIG.
    "editable_settings": EDITABLE_SETTINGS,
    "value_ranges": RANGES,
    "domain_overrides": DOMAIN_OVERRIDES,

    # ── DOMAINS ── each column: (name, type, role); first column unique per domain.
    "domains": {
        "sales_orders": {"cols": [("order_id", "integer", "id"), ("region", "string", "cat"),
            ("channel", "string", "cat"), ("product", "string", "name"), ("quantity", "integer", "madd"),
            ("unit_price", "double", "mlvl"), ("discount", "double", "madd"), ("customer_email", "string", "email")],
            "pools": {"region": ["US", "EU", "APAC", "LATAM", "MEA", "CA", "ANZ"],
                      "channel": ["web", "mobile", "store", "partner", "phone"]}},
        "iot_sensors": {"cols": [("device_id", "string", "id"), ("location", "string", "cat"),
            ("temperature", "double", "mlvl"), ("humidity", "double", "mlvl"), ("battery", "integer", "mlvl"),
            ("reading_ts", "timestamp", "ts")],
            "pools": {"location": ["warehouse", "office", "field", "datacenter", "retail", "factory", "lab"]}},
        "finance_txn": {"cols": [("txn_id", "integer", "id"), ("account", "string", "name"),
            ("amount", "double", "madd"), ("currency", "string", "cat"), ("fraud_flag", "integer", "flag"),
            ("merchant", "string", "name"), ("txn_ts", "timestamp", "ts")],
            "pools": {"currency": ["USD", "EUR", "GBP", "JPY", "INR", "CAD", "AUD", "CHF"]}},
        "web_logs": {"cols": [("session", "string", "id"), ("country", "string", "cat"),
            ("path", "string", "text"), ("status_code", "integer", "catint"), ("latency_ms", "integer", "mlvl"),
            ("bytes", "integer", "madd"), ("request_ts", "timestamp", "ts")],
            "pools": {"country": ["US", "IN", "DE", "GB", "FR", "JP", "BR", "CA", "AU", "SG"],
                      "status_code": [200, 201, 301, 400, 401, 403, 404, 429, 500, 502, 503]}},
        "hr_employees": {"cols": [("emp_id", "integer", "id"), ("department", "string", "cat"),
            ("seniority", "string", "cat"), ("salary", "integer", "madd"), ("bonus", "double", "madd"),
            ("active", "integer", "flag"), ("hire_date", "timestamp", "ts")],
            "pools": {"department": ["Engineering", "Sales", "HR", "Marketing", "Finance", "Operations", "Legal", "Support"],
                      "seniority": ["junior", "mid", "senior", "staff", "principal"]}},
        "healthcare_visits": {"cols": [("visit_id", "integer", "id"), ("facility", "string", "cat"),
            ("specialty", "string", "cat"), ("age", "integer", "mlvl"), ("length_of_stay", "integer", "madd"),
            ("cost", "double", "madd"), ("readmitted", "integer", "flag"), ("payer", "string", "cat"),
            ("admit_ts", "timestamp", "ts")],
            "pools": {"facility": ["north", "south", "central", "east", "west"],
                      "specialty": ["cardiology", "oncology", "neurology", "ortho", "pediatrics", "general"],
                      "payer": ["medicare", "medicaid", "private", "selfpay"]}},
        "gaming_events": {"cols": [("event_id", "string", "id"), ("game", "string", "cat"),
            ("event_type", "string", "cat"), ("player_level", "integer", "mlvl"), ("score", "integer", "madd"),
            ("session_sec", "integer", "madd"), ("event_ts", "timestamp", "ts")],
            "pools": {"game": ["raiders", "speedway", "questline", "blockwars", "arena"],
                      "event_type": ["login", "purchase", "level_up", "match_end", "logout"]}},
        "ecommerce_clicks": {"cols": [("click_id", "string", "id"), ("page_type", "string", "cat"),
            ("device", "string", "cat"), ("dwell_ms", "integer", "mlvl"), ("is_bounce", "integer", "flag"),
            ("referrer", "string", "name")],
            "pools": {"page_type": ["home", "product", "cart", "checkout", "search", "category"],
                      "device": ["desktop", "mobile", "tablet"]}},
        "telecom_cdr": {"cols": [("call_id", "string", "id"), ("network", "string", "cat"),
            ("caller", "string", "name"), ("callee", "string", "name"), ("duration_sec", "integer", "madd"),
            ("call_type", "string", "cat"), ("dropped", "integer", "flag")],
            "pools": {"network": ["4G", "5G", "3G", "wifi"], "call_type": ["voice", "video", "sms", "data"]}},
        "ad_impressions": {"cols": [("impression_id", "string", "id"), ("campaign", "string", "cat"),
            ("placement", "string", "cat"), ("bid_price", "double", "mlvl"), ("clicks", "integer", "madd"),
            ("spend", "double", "madd"), ("clicked", "integer", "flag"), ("creative", "string", "name"),
            ("device", "string", "cat"), ("impression_ts", "timestamp", "ts")],
            "pools": {"campaign": ["spring", "holiday", "retarget", "brand", "launch"],
                      "placement": ["feed", "banner", "video", "native", "search"],
                      "device": ["desktop", "mobile", "ctv", "tablet"]}},
        "weather_obs": {"cols": [("station_id", "string", "id"), ("zone", "string", "cat"),
            ("temp_c", "double", "mlvl"), ("wind_kph", "double", "mlvl"), ("precip_mm", "double", "madd"),
            ("condition", "string", "cat"), ("obs_ts", "timestamp", "ts")],
            "pools": {"zone": ["north", "south", "coastal", "alpine", "desert", "plains"],
                      "condition": ["clear", "cloudy", "rain", "snow", "fog", "storm"]}},
        "shipping_pkg": {"cols": [("shipment_id", "string", "id"), ("carrier", "string", "cat"),
            ("origin", "string", "cat"), ("destination", "string", "cat"), ("weight_kg", "double", "mlvl"),
            ("distance_km", "double", "madd"), ("delivered", "integer", "flag")],
            "pools": {"carrier": ["fedex", "ups", "dhl", "usps", "local"],
                      "origin": ["NYC", "LAX", "CHI", "DAL", "SEA", "MIA"],
                      "destination": ["NYC", "LAX", "CHI", "DAL", "SEA", "MIA"]}},
        "energy_meter": {"cols": [("meter_id", "string", "id"), ("grid_zone", "string", "cat"),
            ("kwh", "double", "madd"), ("peak_flag", "integer", "flag")],
            "pools": {"grid_zone": ["A", "B", "C", "D", "E"]}},
        "rideshare_trips": {"cols": [("trip_id", "string", "id"), ("city", "string", "cat"),
            ("vehicle_type", "string", "cat"), ("trip_km", "double", "madd"), ("fare", "double", "madd"),
            ("surge_flag", "integer", "flag"), ("trip_ts", "timestamp", "ts")],
            "pools": {"city": ["nyc", "sf", "la", "chi", "bos", "sea", "atl"],
                      "vehicle_type": ["economy", "xl", "lux", "pool", "green"]}},
        "social_posts": {"cols": [("post_id", "string", "id"), ("platform", "string", "cat"),
            ("language", "string", "cat"), ("likes", "integer", "madd"), ("shares", "integer", "madd"),
            ("comments", "integer", "madd"), ("is_verified", "integer", "flag"), ("topic", "string", "cat")],
            "pools": {"platform": ["x", "instagram", "tiktok", "facebook", "reddit"],
                      "language": ["en", "es", "fr", "de", "pt", "ja", "hi"],
                      "topic": ["sports", "news", "tech", "music", "food", "travel"]}},
        "pipeline_runs": {"cols": [("run_id", "integer", "id"), ("pipeline", "string", "name"),
            ("workspace", "string", "cat"), ("status", "string", "cat"), ("duration_sec", "integer", "madd"),
            ("rows_processed", "integer", "madd"), ("cost", "double", "madd")],
            "pools": {"workspace": ["prod", "staging", "dev", "qa", "sandbox", "analytics"],
                      "status": ["completed", "failed", "running", "queued", "cancelled", "timed_out"]}},
        "inventory_stock": {"cols": [("sku", "string", "id"), ("warehouse", "string", "cat"),
            ("category", "string", "cat"), ("on_hand", "integer", "madd"), ("reorder_point", "integer", "mlvl"),
            ("unit_cost", "double", "mlvl")],
            "pools": {"warehouse": ["wh1", "wh2", "wh3", "wh4"],
                      "category": ["electronics", "apparel", "grocery", "home", "toys", "auto"]}},
        "flight_segments": {"cols": [("flight_id", "string", "id"), ("airline", "string", "cat"),
            ("origin", "string", "cat"), ("dest", "string", "cat"), ("delay_min", "integer", "mlvl"),
            ("passengers", "integer", "madd"), ("distance_mi", "double", "madd"), ("cancelled", "integer", "flag")],
            "pools": {"airline": ["AA", "DL", "UA", "WN", "BA", "LH", "EK"],
                      "origin": ["JFK", "LAX", "ORD", "ATL", "SFO", "DFW", "SEA"],
                      "dest": ["JFK", "LAX", "ORD", "ATL", "SFO", "DFW", "SEA"]}},
    },
}

NAME_POOLS = {
    "pipeline": ["nightly_etl", "sales_sync", "ml_featurize", "cdc_ingest", "dbt_run"],
    "product": ["Wireless Mouse", "USB-C Cable", "Laptop Stand", "Mechanical Keyboard", "27in Monitor"],
    "account": ["ACC-100482", "ACC-330917", "ACC-558204", "ACC-771630"],
    "merchant": ["Amazon", "Walmart", "Starbucks", "Uber", "Shell", "Netflix"],
    "caller": ["+1-202-555-0143", "+1-415-555-0192", "+44-20-7946-0958"],
    "callee": ["+1-312-555-0177", "+1-646-555-0125", "+91-22-2841-0099"],
    "creative": ["banner_a", "video_15s", "carousel_3", "native_dark"],
    "referrer": ["google", "facebook", "direct", "newsletter", "affiliate"],
}
EMAIL_NAMES = ["amelia.jones", "noah.smith", "olivia.brown", "ethan.davis", "sofia.garcia"]
EMAIL_DOMAINS = ["gmail.com", "outlook.com", "company.com", "yahoo.com"]
TEXT_POOL = ["/", "/api/v1/orders", "/login", "/products/42", "/checkout", "/health"]


def index_domain(spec):
    roles = {r: [] for r in ("id", "cat", "catint", "flag", "madd", "mlvl",
                             "name", "email", "text", "ts")}
    types, name_role = {}, {}
    for name, typ, role in spec["cols"]:
        roles[role].append(name); types[name] = typ; name_role[name] = role
    return {"cols": [c[0] for c in spec["cols"]], "types": types, "roles": roles,
            "name_role": name_role, "pools": spec.get("pools", {}),
            "first": spec["cols"][0][0]}


def weighted_choice(rng, weights):
    items = list(weights.items())
    r = rng.random() * sum(w for _, w in items)
    upto = 0.0
    for k, w in items:
        upto += w
        if r <= upto:
            return k
    return items[-1][0]


# ─────────────────────────────────────────────────────────────────────────────
# Ranges / thresholds (Fix 4 + Fix 5)
# ─────────────────────────────────────────────────────────────────────────────
def crange(col, typ, first_col=None):
    return column_range(col, typ, first_col, CONFIG["value_ranges"], CONFIG["domain_overrides"])


def pick_threshold(lo, hi, rng):
    lo, hi = int(math.ceil(lo)), int(math.floor(hi))
    if hi <= lo:
        return lo
    pad = max(1, (hi - lo) // 10)
    a, b = lo + pad, hi - pad
    if a > b:
        a, b = lo, hi
    return rng.randint(a, b)


def sample_ints(lo, hi, k, rng):
    lo, hi = int(math.ceil(lo)), int(math.floor(hi))
    k = min(k, hi - lo + 1)
    return sorted(rng.sample(range(lo, hi + 1), k))


# ─────────────────────────────────────────────────────────────────────────────
# Samples (Fix 5: realistic, range-bounded)
# ─────────────────────────────────────────────────────────────────────────────
def make_id(col, typ, rng):
    if typ == "integer":
        return str(rng.randint(100_000, 9_999_999))
    return col.split("_")[0][:3].upper() + "-" + "".join(rng.choice("0123456789ABCDEF") for _ in range(8))


def make_ts(rng):
    return (f"2026-{rng.randint(1,12):02d}-{rng.randint(1,28):02d} "
            f"{rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}")


def sample_value(col, typ, role, pools, rng, first_col):
    if role == "id":
        return make_id(col, typ, rng)
    if role in ("cat", "catint") and col in pools:
        return str(rng.choice(pools[col]))
    if role == "flag":
        return str(rng.choice([0, 1]))
    if role == "ts":
        return make_ts(rng)
    if role == "email":
        return f"{rng.choice(EMAIL_NAMES)}@{rng.choice(EMAIL_DOMAINS)}"
    if role == "name" and col in NAME_POOLS:
        return rng.choice(NAME_POOLS[col])
    if role == "text":
        return rng.choice(TEXT_POOL)
    if typ == "integer":
        lo, hi = crange(col, typ, first_col)
        return str(rng.randint(int(math.ceil(lo)), int(math.floor(hi))))
    if typ == "double":
        lo, hi = crange(col, typ, first_col)
        return f"{rng.uniform(lo, hi):.2f}"
    return rng.choice(["alpha", "bravo", "charlie"]) + str(rng.randint(1, 9))


def build_schema(D, size_hint, row_count, rng):
    cols, types, pools, nr, fc = D["cols"], D["types"], D["pools"], D["name_role"], D["first"]
    samples = [{c: sample_value(c, types[c], nr[c], pools, rng, fc) for c in cols} for _ in range(3)]
    return {"columns": list(cols), "inferred_types": dict(types),
            "row_count": row_count, "size_hint": size_hint, "samples": samples}


# ─────────────────────────────────────────────────────────────────────────────
# Work-op builders -> (transforms, filter_condition, aggregation)
# Filters use ONE SQL grammar (Fix 2); never =/!=/in on a double col (Fix 4);
# thresholds bounded to the column range (Fix 5).
# ─────────────────────────────────────────────────────────────────────────────
def op_filter_measure(D, rng):
    m = rng.choice(D["roles"]["madd"] + D["roles"]["mlvl"])
    t = D["types"][m]
    lo, hi = crange(m, t, D["first"])
    if t == "double":
        op = rng.choice([">", "<", ">=", "<=", "between"])
    else:
        op = rng.choice([">", "<", ">=", "<=", "=", "!=", "between", "in"])
    if op == "between":
        a = pick_threshold(lo, hi, rng)
        b = pick_threshold(a, hi, rng)
        if b <= a:
            b = min(int(math.floor(hi)), a + 1)
        return [], f"{m} between {a} and {b}", None
    if op == "in":
        vals = sample_ints(lo, hi, 3, rng)
        return [], f"{m} in ({', '.join(str(v) for v in vals)})", None
    return [], f"{m} {op} {pick_threshold(lo, hi, rng)}", None


def op_filter_flag(D, rng):
    f = rng.choice(D["roles"]["flag"])
    return [], f"{f} = {rng.choice([0, 1])}", None


def op_filter_catint(D, rng):
    c = rng.choice(D["roles"]["catint"])
    pool = D["pools"][c]
    if rng.random() < 0.5:
        return [], f"{c} = {rng.choice(pool)}", None
    codes = sorted(rng.sample(pool, min(3, len(pool))))
    return [], f"{c} in ({', '.join(str(x) for x in codes)})", None


def op_filter_cat_eq(D, rng):
    c = rng.choice(D["roles"]["cat"])
    return [], f"{c} = '{rng.choice(D['pools'][c])}'", None


def op_filter_cat_in(D, rng):
    c = rng.choice(D["roles"]["cat"])
    vals = rng.sample(D["pools"][c], min(2, len(D["pools"][c])))
    return [], f"{c} in ({', '.join(chr(39) + v + chr(39) for v in vals)})", None


_CONVERT = {"sec": ("_min", 60), "ms": ("_sec", 1000), "bytes": ("_kb", 1024),
            "kwh": ("_mwh", 1000), "km": ("_mi", 2), "kg": ("_lb", 1)}


def _convert_target(m):
    for key, (suffix, div) in _CONVERT.items():
        if key in m:
            base = m.split(key)[0].rstrip("_") or m
            return f"{base}{suffix}", div
    return f"{m}_scaled", 10


def op_convert(D, rng):
    m = rng.choice(D["roles"]["madd"] + D["roles"]["mlvl"])
    newcol, div = _convert_target(m)
    if newcol in D["types"]:
        newcol = f"{m}_conv"
    transforms = [f"{newcol} = {m} / {div}"]
    if rng.random() < 0.6:
        lo, hi = crange(m, D["types"][m], D["first"])
        return transforms, f"{newcol} > {pick_threshold(lo/div, hi/div, rng)}", None
    return transforms, None, None


def op_round(D, rng):
    doubles = [m for m in D["roles"]["madd"] + D["roles"]["mlvl"] if D["types"][m] == "double"]
    if not doubles:
        return op_filter_measure(D, rng)
    m = rng.choice(doubles)
    newcol = f"{m}_rounded"
    transforms = [f"{newcol} = round({m}, 2)"]
    if rng.random() < 0.5:
        lo, hi = crange(m, "double", D["first"])
        return transforms, f"{newcol} > {pick_threshold(lo, hi, rng)}", None
    return transforms, None, None


def op_cast(D, rng):
    m = rng.choice(D["roles"]["madd"] + D["roles"]["mlvl"])
    if D["types"][m] == "double":
        newcol, tgt = f"{m}_int", "integer"
    else:
        newcol, tgt = f"{m}_dbl", "double"
    return [f"{newcol} = cast({m} as {tgt})"], None, None


def op_normalize(D, rng):
    c = rng.choice(D["roles"]["cat"])
    newcol = f"{c}_norm"
    fn, applied = rng.choice([("upper", str.upper), ("lower", str.lower)])
    v = applied(rng.choice(D["pools"][c]))
    return [f"{newcol} = {fn}({c})"], f"{newcol} = '{v}'", None


def op_concat(D, rng):
    pool = D["roles"]["name"] + D["roles"]["cat"] + D["roles"]["text"]
    if len(pool) < 2:
        return op_cast(D, rng)
    a, b = rng.sample(pool, 2)
    return [f"{a}_{b} = concat({a}, ' ', {b})"], None, None


def op_dedup(D, rng):
    return ["distinct()"], None, None


def op_sort(D, rng):
    x = rng.choice(D["roles"]["madd"] + D["roles"]["mlvl"] + D["roles"]["cat"])
    return [f"orderBy({x}{rng.choice(['', ' desc'])})"], None, None


def op_aggregate(D, rng):
    g = rng.choice(D["roles"]["cat"] + D["roles"]["catint"])
    measures = D["roles"]["madd"] + D["roles"]["mlvl"]
    k = rng.randint(1, min(3, len(measures)))
    aggs = []
    for col in rng.sample(measures, k):
        additive = col in D["roles"]["madd"]
        op = rng.choice(["avg", "max", "min"] + (["sum"] if additive else []))
        aggs.append({"op": op, "column": col, "alias": f"{op}_{col}"})
    aggs.append({"op": "count", "column": "*", "alias": "row_count"})
    return [], None, {"group_by": [g], "aggregations": aggs}


OP_FUNCS = {"filter_measure": op_filter_measure, "filter_flag": op_filter_flag,
            "filter_catint": op_filter_catint, "filter_cat_eq": op_filter_cat_eq,
            "filter_cat_in": op_filter_cat_in, "convert": op_convert, "round": op_round,
            "cast": op_cast, "normalize": op_normalize, "concat": op_concat,
            "dedup": op_dedup, "sort": op_sort}


def op_available(name, D):
    r = D["roles"]
    if name in ("filter_measure", "convert", "sort", "cast"):
        return bool(r["madd"] + r["mlvl"])
    if name == "round":
        return any(D["types"][m] == "double" for m in r["madd"] + r["mlvl"])
    if name == "filter_flag":
        return bool(r["flag"])
    if name == "filter_catint":
        return bool(r["catint"])
    if name in ("filter_cat_eq", "filter_cat_in", "normalize"):
        return bool(r["cat"])
    if name == "concat":
        return len(r["name"] + r["cat"] + r["text"]) >= 2
    return True


def pick_work_op(D, rng):
    avail = {k: w for k, w in CONFIG["op_weights"].items() if op_available(k, D)}
    return OP_FUNCS[weighted_choice(rng, avail)](D, rng)


def op_conflicts(transforms, filt, derived_names, filtered_cols):
    """True if this op would duplicate a derived name (FB) or re-filter a column
    already constrained earlier (FA/FB) — both forbidden across a pipeline."""
    for t in transforms:
        if "=" in t and t.split("=", 1)[0].strip() in derived_names:
            return True
    if filt and parse_filter(filt)[0] in filtered_cols:
        return True
    return False


def pick_nonconflicting_op(D, rng, derived_names, filtered_cols, tries=8):
    for _ in range(tries):
        tr, fl, ag = pick_work_op(D, rng)
        if not op_conflicts(tr, fl, derived_names, filtered_cols):
            return tr, fl, ag
    return [], None, None  # give up -> stays a pass-through this hop


# ─────────────────────────────────────────────────────────────────────────────
# Record builder
# ─────────────────────────────────────────────────────────────────────────────
def ds_name(c):
    return "DS_" + c.title().replace("_", "")


def title(c):
    return c.title().replace("_", "")


def build_record(domain_name, D, rng):
    size_hint = weighted_choice(rng, CONFIG["size_dist"])
    lo, hi = CONFIG["row_range_by_size"][size_hint]
    row_count = rng.randint(lo, hi)

    n = int(weighted_choice(rng, {str(k): v for k, v in CONFIG["num_containers_dist"].items()}))
    schemes = {s: w for s, w in CONFIG["scheme_weights"].items()
               if len(CONFIG["container_schemes"][s]) >= n}
    scheme = weighted_choice(rng, schemes)
    ctc = CONFIG["container_schemes"][scheme][:n]
    containers = {f"stage{i}": ctc[i] for i in range(n)}
    datasets = [{"name": ds_name(c), "container": c,
                 "role": "source" if i == 0 else "sink" if i == n - 1 else "intermediate"}
                for i, c in enumerate(ctc)]

    schema = build_schema(D, size_hint, row_count, rng)

    m_notebooks = n - 2
    has_measures = bool(D["roles"]["madd"] + D["roles"]["mlvl"])
    agg_on_last = has_measures and rng.random() < CONFIG["agg_prob"]
    work_kinds = []

    # Track constraints so no column is filtered twice (FA/FB), no derived name
    # repeats (FB), and processed_time is stamped at most once.
    derived_names, filtered_cols = set(), set()
    pt_used = False

    def register(transforms, filt):
        for t in transforms:
            if "=" in t:
                derived_names.add(t.split("=", 1)[0].strip())
        if filt:
            filtered_cols.add(parse_filter(filt)[0])

    nb_specs = []  # [src, snk, transforms, filt, agg]
    for j in range(m_notebooks):
        src, snk = ctc[1 + j], ctc[2 + j]
        is_last = (j == m_notebooks - 1)
        prob = CONFIG["final_work_prob"] if is_last else CONFIG["work_prob"]
        transforms, filt, agg = [], None, None
        if rng.random() < prob:
            if is_last and agg_on_last:
                transforms, filt, agg = op_aggregate(D, rng); work_kinds.append("aggregate")
            else:
                transforms, filt, agg = pick_nonconflicting_op(D, rng, derived_names, filtered_cols)
                if transforms or filt:
                    work_kinds.append("transform")
            if (transforms or filt) and not pt_used and agg is None \
                    and rng.random() < CONFIG["processed_time_prob"]:
                transforms = list(transforms) + ["processed_time = currentTimestamp()"]
                pt_used = True
        register(transforms, filt)
        nb_specs.append([src, snk, transforms, filt, agg])

    # FE: guarantee at least one real operation somewhere in the pipeline.
    if not any(spec[2] or spec[3] or spec[4] for spec in nb_specs):
        last = nb_specs[-1]
        if agg_on_last:
            last[2], last[3], last[4] = op_aggregate(D, rng)
        else:
            last[2], last[3], last[4] = pick_nonconflicting_op(D, rng, derived_names, filtered_cols)
            if not (last[2] or last[3]):  # ops exhausted -> force a guaranteed-valid filter
                last[2], last[3], last[4] = op_filter_measure(D, rng)
        work_kinds.append("aggregate" if last[4] else "transform")

    has_agg = any(spec[4] for spec in nb_specs)
    rec = expected_settings(size_hint, has_agg)

    stages = [{"name": f"Ingest_{title(ctc[0])}_To_{title(ctc[1])}", "type": "copy",
               "source_dataset": ds_name(ctc[0]), "sink_dataset": ds_name(ctc[1]),
               "diu": rec["diu"]}]
    for src, snk, transforms, filt, agg in nb_specs:
        stage = {"name": f"Transform_{title(src)}_To_{title(snk)}", "type": "notebook",
                 "source_container": src, "sink_container": snk,
                 "transformations": transforms, "filter_condition": filt,
                 "num_workers": rec["num_workers"], "shuffle_partitions": rec["shuffle_partitions"]}
        if agg:
            stage["aggregation"] = agg
        stages.append(stage)

    work_desc = ", ".join(work_kinds) if work_kinds else "pure staging"
    reasoning = (f"{scheme} pipeline with {n} containers and {n-1} stages over {domain_name}: "
                 f"ADF copy ingests {ctc[0]}->{ctc[1]} at {rec['diu']} DIU; "
                 f"{m_notebooks} notebook hop(s) on {rec['node_type']} ({rec['num_workers']} workers, "
                 f"{rec['shuffle_partitions']} shuffle partitions) do {work_desc}; sink is {ctc[-1]}.")

    config = {
        "containers": containers, "containers_to_create": ctc, "datasets": datasets,
        "stages": stages, "execution_order": [s["name"] for s in stages],
        "num_containers": n, "recommended_settings": rec,
        "editable_settings": json.loads(json.dumps(CONFIG["editable_settings"])),
        "reasoning": reasoning,
    }
    config_prompt = render_prompt(config)  # Fix 3: prompt is a pure function of config
    return {"schema": schema, "user_prompt": config_prompt, "config": config}


def content_key(rec):
    return json.dumps({"u": rec["user_prompt"], "c": rec["config"]},
                      sort_keys=True, separators=(",", ":"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=CONFIG["num_rows"])
    ap.add_argument("--seed", type=int, default=CONFIG["seed"])
    ap.add_argument("--out", default=CONFIG["output_path"])
    args = ap.parse_args()

    rng = random.Random(args.seed)
    domains = {name: index_domain(spec) for name, spec in CONFIG["domains"].items()}
    names = list(domains.keys())

    records, seen, attempts = [], set(), 0
    max_attempts = args.rows * 200
    while len(records) < args.rows and attempts < max_attempts:
        attempts += 1
        name = names[len(records) % len(names)] if attempts <= len(names) else rng.choice(names)
        rec = build_record(name, domains[name], rng)
        key = content_key(rec)
        if key in seen:
            continue
        seen.add(key)
        records.append(rec)

    with open(args.out, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")

    print(f"wrote {len(records)} rows to {args.out} (seed={args.seed}, attempts={attempts})")
    if len(records) < args.rows:
        print(f"WARNING: only {len(records)}/{args.rows}; widen CONFIG.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
