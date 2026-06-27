"""
Synthetic dataset generator for the unified ADF+Databricks planner.

Each record = {schema, user_prompt, config}, built to be SEMANTICALLY correct,
not merely structurally valid. Generation is driven by per-column *meaning*
(semantics), not raw type, and obeys these hard rules:

  COLUMN SEMANTICS
    flag     int 0/1   -> only `<flag> == 1` in a filter. Never arithmetic,
                          never round/abs, never avg/sum.
    id       int/str   -> never transformed, filtered by value, or measured.
    cat      str        -> low-cardinality. group_by key and/or `equals(cat,'v')`
                          with v from the column's real value pool. May be
                          uppercased then filtered on the uppercased output.
    catint   int        -> code-like (e.g. HTTP status). `<col> == <code>` filter
                          or group_by. Never avg/sum/round.
    measure  int/double -> avg/sum/min/max (sum only if additive). Unit
                          conversions (e.g. seconds->minutes) allowed and then
                          consumed by a filter. Never rounded if integer.
    name/email str       -> left as-is (cleanups would be unused / wrong, e.g.
                          title-casing an email). Not filtered by category value.

  LINEAGE / USAGE
    * The work happens in the LAST notebook stage; earlier notebook stages are
      pure staging hops (only processed_time). This removes cross-stage lineage
      ambiguity entirely.
    * Every business transform a stage creates is CONSUMED in that same stage
      (by its filter or aggregation). No orphan columns.
    * group_by uses an ORIGINAL column (the planner's aggregation validator only
      accepts original columns); aggregations run on original measures.

  PROMPT <-> CONFIG
    The English prompt is generated FROM the chosen ops, so they always agree.

Every record is then VERIFIED against the REAL planner + notebook builder, and
against the semantic rules above.
"""

import copy
import json
import random
import re
import sys
import types

_fake = types.ModuleType("config")
_fake.GROQ_API_KEY = "synthetic"
# executor_agent imports Azure/Databricks creds at module top; dataset
# generation never touches them, so stub with dummies to satisfy the import.
for _name in (
    "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET",
    "AZURE_SUBSCRIPTION_ID", "AZURE_RESOURCE_GROUP", "AZURE_DATA_FACTORY",
    "AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY",
    "DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_NOTEBOOK_BASE",
):
    setattr(_fake, _name, "synthetic")
sys.modules.setdefault("config", _fake)

from planner_agent import groq_planner as gp
from executor_agent.notebook_builder import build_notebook_source, _convert_expr, _convert_filter

random.seed(20260618)


# ────────────────────────────────────────────────────────────────────────────
# Schemas with per-column SEMANTICS. types use only integer/double/string
# (what the profiler emits). measure kind: "add" (sum-able) or "lvl" (level).
# ────────────────────────────────────────────────────────────────────────────
SCHEMAS = {
    "pipeline_runs": {
        "types": {"pipeline": "string", "workspace": "string", "status": "string",
                  "duration_sec": "integer", "rows_processed": "integer", "cost": "double"},
        "size_hint": "medium (5–50MB)", "row_count": 142_000,
        "cat": {"status": ["completed", "failed", "running", "queued", "cancelled", "timed_out"],
                "workspace": ["prod", "staging", "dev", "qa", "sandbox", "analytics"]},
        "catint": {}, "flags": [], "names": ["pipeline"], "emails": [], "ids": [],
        "measures": {"duration_sec": "add", "rows_processed": "add", "cost": "add"},
        "conversions": {"duration_sec": ("duration_min", "duration_sec / 60", "minutes")},
    },
    "sales_orders": {
        "types": {"order_id": "integer", "region": "string", "product": "string",
                  "quantity": "integer", "price": "double", "customer_email": "string"},
        "size_hint": "small (< 5MB)", "row_count": 4_200,
        "cat": {"region": ["US", "EU", "APAC", "LATAM", "MEA", "CA", "ANZ"]},
        "catint": {}, "flags": [], "names": ["product"], "emails": ["customer_email"],
        "ids": ["order_id"],
        "measures": {"quantity": "add", "price": "lvl"},
        "conversions": {},
    },
    "iot_sensors": {
        "types": {"device_id": "string", "location": "string", "temperature": "double",
                  "humidity": "double", "battery": "integer", "reading_ts": "string"},
        "size_hint": "large (50–200MB)", "row_count": 1_800_000,
        "cat": {"location": ["warehouse", "office", "field", "datacenter", "retail", "factory", "lab"]},
        "catint": {}, "flags": [], "names": ["reading_ts"], "emails": [],
        "ids": ["device_id"],
        "measures": {"temperature": "lvl", "humidity": "lvl", "battery": "lvl"},
        "conversions": {},
    },
    "employees": {
        "types": {"emp_name": "string", "department": "string", "salary": "integer",
                  "bonus": "double", "active": "integer"},
        "size_hint": "small (< 5MB)", "row_count": 980,
        "cat": {"department": ["Engineering", "Sales", "HR", "Marketing", "Finance",
                               "Operations", "Legal", "Support"]},
        "catint": {}, "flags": ["active"], "names": ["emp_name"], "emails": [], "ids": [],
        "measures": {"salary": "add", "bonus": "add"},
        "conversions": {},
    },
    "web_logs": {
        "types": {"session": "string", "country": "string", "path": "string",
                  "status_code": "integer", "latency_ms": "integer", "bytes": "integer"},
        "size_hint": "xlarge (> 200MB)", "row_count": 12_500_000,
        "cat": {"country": ["US", "IN", "DE", "GB", "FR", "JP", "BR", "CA", "AU", "SG"]},
        "catint": {"status_code": [200, 201, 301, 400, 401, 403, 404, 429, 500, 502, 503]},
        "flags": [], "names": ["path"], "emails": [], "ids": ["session"],
        "measures": {"latency_ms": "lvl", "bytes": "add"},
        "conversions": {"bytes": ("bytes_kb", "bytes / 1024", "KB"),
                        "latency_ms": ("latency_sec", "latency_ms / 1000", "seconds")},
    },
    "transactions": {
        "types": {"txn_id": "integer", "account": "string", "amount": "double",
                  "currency": "string", "fraud_flag": "integer", "merchant": "string"},
        "size_hint": "medium (5–50MB)", "row_count": 67_300,
        "cat": {"currency": ["USD", "EUR", "GBP", "JPY", "INR", "CAD", "AUD", "CHF"]},
        "catint": {}, "flags": ["fraud_flag"], "names": ["account", "merchant"],
        "emails": [], "ids": ["txn_id"],
        "measures": {"amount": "add"},
        "conversions": {},
    },
}

OP_WORD = {"avg": "average", "sum": "total", "max": "maximum", "min": "minimum"}

# Realistic value pools for free-text columns (used for samples only; these
# columns are never filtered/grouped, so they don't affect logical correctness).
NAME_POOLS = {
    "pipeline":   ["nightly_etl", "sales_sync", "ml_featurize", "cdc_ingest", "dbt_run"],
    "product":    ["Wireless Mouse", "USB-C Cable", "Laptop Stand", "Mechanical Keyboard",
                   "27in Monitor", "Webcam HD"],
    "emp_name":   ["Aisha Khan", "Diego Martinez", "Mei Chen", "Liam O'Brien",
                   "Priya Nair", "Tom Becker"],
    "path":       ["/", "/api/v1/orders", "/login", "/products/42", "/checkout", "/health"],
    "account":    ["ACC-100482", "ACC-330917", "ACC-558204", "ACC-771630"],
    "merchant":   ["Amazon", "Walmart", "Starbucks", "Uber", "Shell", "Netflix"],
    "reading_ts": ["2026-03-14 08:22:11", "2026-03-14 08:22:41", "2026-03-14 08:23:09"],
}
EMAIL_NAMES = ["amelia.jones", "noah.smith", "olivia.brown", "ethan.davis", "sofia.garcia"]
EMAIL_DOMAINS = ["gmail.com", "outlook.com", "company.com", "yahoo.com"]

# Realistic id formats. String ids -> tokens; integer ids stay digit-only so the
# profiler still infers "integer". ids are never filtered/grouped (cosmetic only).
ID_GENERATORS = {
    "device_id": lambda: "DEV-" + "".join(random.choice("0123456789ABCDEF") for _ in range(6)),
    "session":   lambda: "".join(random.choice("0123456789abcdef") for _ in range(16)),
    "order_id":  lambda: str(random.randint(100_000, 999_999)),
    "txn_id":    lambda: str(random.randint(5_000_000, 9_999_999)),
}


def build_schema(sd):
    types_map = sd["types"]
    columns = list(types_map.keys())
    flags = set(sd["flags"])

    def sample_val(col, t):
        for cat, pool in sd["cat"].items():
            if col == cat:
                return random.choice(pool)
        for ci, pool in sd["catint"].items():
            if col == ci:
                return str(random.choice(pool))
        if col in flags:
            return str(random.choice([0, 1]))
        if col in ID_GENERATORS:
            return ID_GENERATORS[col]()
        if t == "integer":
            return str(random.randint(1, 9999))
        if t == "double":
            return f"{random.uniform(1, 9999):.2f}"
        if col in sd["emails"]:
            return f"{random.choice(EMAIL_NAMES)}@{random.choice(EMAIL_DOMAINS)}"
        if col in NAME_POOLS:
            return random.choice(NAME_POOLS[col])
        return random.choice(["alpha", "bravo", "charlie"]) + str(random.randint(1, 9))

    samples = [{c: sample_val(c, types_map[c]) for c in columns} for _ in range(3)]
    return {"columns": columns, "inferred_types": dict(types_map),
            "row_count": sd["row_count"], "size_hint": sd["size_hint"], "samples": samples}


# ────────────────────────────────────────────────────────────────────────────
# Work-stage builders. Each returns (transforms, filter, aggregation, english).
# transforms exclude the trailing processed_time (added by caller).
# ────────────────────────────────────────────────────────────────────────────
def work_aggregate(sd):
    """group_by an original cat, aggregate original measures, + row count."""
    gb = random.choice(list(sd["cat"].keys()))
    measures = list(sd["measures"].items())
    aggs, phrases = [], []
    k = random.randint(1, min(3, len(measures)))
    for col, kind in random.sample(measures, k=k):
        ops = ["avg", "max", "min"] + (["sum"] if kind == "add" else [])
        op = random.choice(ops)
        aggs.append({"op": op, "column": col, "alias": f"{op}_{col}"})
        phrases.append(f"{OP_WORD[op]} of {col}")
    aggs.append({"op": "count", "column": "*", "alias": "row_count"})
    phrases.append("a row count")
    agg = {"group_by": [gb], "aggregations": aggs}

    filt, feng = _maybe_simple_filter(sd, allow_cat=False)
    eng = f"group by {gb} computing " + ", ".join(phrases)
    if feng:
        eng = f"{feng}; then {eng}"
    return [], filt, agg, eng


def work_transform_filter(sd):
    """Create exactly one business transform and consume it with the filter."""
    convs = sd["conversions"]
    if convs and random.random() < 0.6:
        src = random.choice(list(convs.keys()))
        newcol, expr, unit = convs[src]
        thr = random.choice([1, 2, 3, 5, 8, 10, 15, 25, 50, 75, 100])
        transforms = [f"{newcol} = {expr}"]
        filt = f"{newcol} > {thr}"
        eng = f"convert {src} to {unit} as {newcol}, then keep rows where {newcol} > {thr}"
        return transforms, filt, None, eng
    # cat normalize -> filter on the normalized output
    cat = random.choice(list(sd["cat"].keys()))
    val = random.choice(sd["cat"][cat])
    norm = f"{cat}_norm"
    transforms = [f"{norm} = upper({cat})"]
    filt = f"equals({norm}, '{val.upper()}')"
    eng = f"normalize {cat} to uppercase as {norm}, then keep rows where {norm} = '{val.upper()}'"
    return transforms, filt, None, eng


def work_filter_only(sd):
    filt, eng = _maybe_simple_filter(sd, allow_cat=True, force=True)
    return [], filt, None, eng


def _maybe_simple_filter(sd, allow_cat, force=False):
    """A filter that needs no transform: flag / catint / measure-threshold / raw cat."""
    options = []
    if sd["flags"]:
        f = random.choice(sd["flags"])
        options.append((f"{f} == 1", f"keep only rows where {f} = 1"))
    if sd["catint"]:
        c = random.choice(list(sd["catint"].keys()))
        code = random.choice(sd["catint"][c])
        options.append((f"{c} == {code}", f"keep only rows where {c} = {code}"))
    meas = [m for m in sd["measures"]]
    if meas:
        m = random.choice(meas)
        thr = random.choice([5, 10, 25, 50, 75, 100, 150, 250, 500, 750, 1000])
        options.append((f"{m} > {thr}", f"keep only rows where {m} > {thr}"))
    if allow_cat and sd["cat"]:
        c = random.choice(list(sd["cat"].keys()))
        v = random.choice(sd["cat"][c])
        options.append((f"equals({c}, '{v}')", f"keep only rows where {c} = '{v}'"))
    if not options:
        return None, None
    if not force and random.random() < 0.35:
        return None, None
    return random.choice(options)


# ────────────────────────────────────────────────────────────────────────────
# Record builder
# ────────────────────────────────────────────────────────────────────────────
def build_record(name, num_containers, mode):
    sd = SCHEMAS[name]
    schema = build_schema(sd)
    rec = gp.get_recommended_settings(sd["size_hint"])
    clist = gp._resolve_container_names(num_containers, None)
    containers = {f"stage{i}": clist[i] for i in range(num_containers)}
    datasets = gp._build_datasets(clist)

    stages, prompt_parts = [], []
    n_hops = num_containers - 1
    for i in range(n_hops):
        src, snk = clist[i], clist[i + 1]
        if i == 0:
            stages.append({"name": f"Ingest_{src.title()}_To_{snk.title()}", "type": "copy",
                           "source_dataset": f"DS_{src.title().replace('_', '')}",
                           "sink_dataset": f"DS_{snk.title().replace('_', '')}", "diu": rec["diu"]})
            prompt_parts.append(f"ingest data from {src} into {snk}")
            continue

        is_last = (i == n_hops - 1)
        if is_last:
            if mode == "agg":
                tlist, filt, agg, eng = work_aggregate(sd)
            elif mode == "tf":
                tlist, filt, agg, eng = work_transform_filter(sd)
            else:
                tlist, filt, agg, eng = work_filter_only(sd)
            seg = f"in {snk}, " + eng
        else:
            tlist, filt, agg = [], None, None
            seg = f"stage data from {src} into {snk}"

        stage = {"name": f"Transform_{src.title()}_To_{snk.title()}", "type": "notebook",
                 "source_container": src, "sink_container": snk,
                 "transformations": tlist + ["processed_time = currentTimestamp()"],
                 "filter_condition": filt,
                 "num_workers": rec["num_workers"], "shuffle_partitions": rec["shuffle_partitions"]}
        if agg:
            stage["aggregation"] = agg
        stages.append(stage)
        prompt_parts.append(seg)

    user_prompt = "; then ".join(prompt_parts) + "."
    config = {
        "containers": containers, "containers_to_create": clist, "datasets": datasets,
        "stages": stages, "execution_order": [s["name"] for s in stages],
        "num_containers": num_containers, "recommended_settings": rec,
        "editable_settings": gp.DEFAULT_EDITABLE_SETTINGS,
        "reasoning": (f"{num_containers}-stage medallion pipeline over {name}: ADF copy ingests "
                      f"{clist[0]}->{clist[1]}; staging hops carry data forward; the final "
                      f"Databricks notebook applies the requested logic."),
    }
    return {"schema": schema, "user_prompt": user_prompt, "config": config}


# ────────────────────────────────────────────────────────────────────────────
# Verification — real planner code + semantic rules
# ────────────────────────────────────────────────────────────────────────────
def verify(record, name):
    sd = SCHEMAS[name]
    schema, config = record["schema"], record["config"]
    cols = set(schema["columns"])
    types_map = schema["inferred_types"]
    flags, ids = set(sd["flags"]), set(sd["ids"])
    cat_pools = sd["cat"]
    n = config["num_containers"]
    errs = []

    # ── counts derive from one num_containers ──
    if len(config["datasets"]) != n: errs.append("datasets count")
    if len(config["stages"]) != n - 1: errs.append("stages count")
    if len(config["containers_to_create"]) != n: errs.append("containers count")
    if [s for s in config["stages"] if s["type"] == "copy"] != [config["stages"][0]] or \
       config["stages"][0]["type"] != "copy":
        errs.append("exactly one copy, first")
    if config["execution_order"] != [s["name"] for s in config["stages"]]:
        errs.append("execution_order")
    if config["recommended_settings"] != gp.get_recommended_settings(schema["size_hint"]):
        errs.append("settings != size_hint")

    # ── 1) real structural validator accepts config UNCHANGED ──
    reval = gp._structural_validate(copy.deepcopy(config), schema)
    if json.dumps(reval, sort_keys=True) != json.dumps(config, sort_keys=True):
        errs.append("changed by _structural_validate")

    for s in config["stages"]:
        if s["type"] != "notebook":
            continue
        produced = set()
        # transforms: type-safe, non-redundant, no flag corruption, col-existent
        for t in s["transformations"]:
            lhs, rhs = (x.strip() for x in t.split("=", 1))
            if lhs == "processed_time":
                produced.add(lhs); continue
            for ref in re.findall(r'col\("([^"]+)"\)', _convert_expr(rhs)):
                if ref not in cols and ref not in produced:
                    errs.append(f"transform refs unknown col '{ref}'")
                if ref in flags:
                    errs.append(f"flag '{ref}' used in transform (corruption)")
                if ref in ids:
                    errs.append(f"id '{ref}' transformed")
            if re.search(r'round\(\s*(\w+)', rhs):
                inner = re.search(r'round\(\s*(\w+)', rhs).group(1)
                if types_map.get(inner) == "integer":
                    errs.append(f"round on integer '{inner}' (redundant)")
            if re.search(r'toInteger\(\s*(\w+)', rhs):
                inner = re.search(r'toInteger\(\s*(\w+)', rhs).group(1)
                if types_map.get(inner) == "integer":
                    errs.append(f"toInteger on integer '{inner}' (redundant)")
            produced.add(lhs)

        business = produced - {"processed_time"}

        # filter: col-existent; string-equality only on cat (or _norm) w/ valid value
        filt_refs = set()
        fc = s.get("filter_condition")
        if fc:
            filt_refs = set(re.findall(r'col\("([^"]+)"\)', _convert_filter(fc)))
            for ref in filt_refs:
                if ref not in cols and ref not in produced:
                    errs.append(f"filter refs unknown col '{ref}'")
            m = re.match(r"^equals\((\w+),\s*'([^']+)'\)$", fc)
            if m:
                col, val = m.group(1), m.group(2)
                base = col[:-5] if col.endswith("_norm") else col
                if base not in cat_pools:
                    errs.append(f"string-equality filter on non-category '{col}'")
                else:
                    valid = set(cat_pools[base]) | {v.upper() for v in cat_pools[base]}
                    if val not in valid:
                        errs.append(f"filter value '{val}' not in {base} pool")

        # aggregation: group_by original col, measures only, avg/sum numeric
        agg = s.get("aggregation")
        agg_refs = set()
        if agg:
            for g in agg["group_by"]:
                if g not in cols:
                    errs.append(f"group_by '{g}' not original col")
                agg_refs.add(g)
            for a in agg["aggregations"]:
                c = a["column"]
                if c != "*":
                    agg_refs.add(c)
                    if c not in sd["measures"]:
                        errs.append(f"aggregating non-measure '{c}'")
                    if a["op"] in ("avg", "sum") and types_map.get(c) not in ("integer", "double"):
                        errs.append(f"{a['op']} on non-numeric '{c}'")

        # 2) every business transform must be CONSUMED in this stage
        for b in business:
            if b not in filt_refs and b not in agg_refs:
                errs.append(f"transform '{b}' created but never used")

        # 3) real notebook builder: no skipped/malformed, groupBy present iff agg
        src = build_notebook_source(s, "acct")
        if "# skipped malformed" in src:
            errs.append(f"malformed transform in '{s['name']}'")
        if agg and "groupBy" not in src:
            errs.append(f"agg without groupBy in '{s['name']}'")

    return errs


def _content_key(rec):
    """Logical identity of a record: prompt + config only.

    The `samples` rows under schema are random noise (never used by the
    planner), so two records with identical prompt+config are duplicate
    training examples even though their JSON bytes differ. Dedup on this.
    """
    return json.dumps({"u": rec["user_prompt"], "c": rec["config"]},
                      sort_keys=True, separators=(",", ":"))


TARGET = 5000
MAX_ATTEMPTS = TARGET * 400  # safety stop if the op-space is exhausted
SATURATE_AT = 1500           # consecutive collisions on one schema -> exhausted


def main():
    names = list(SCHEMAS.keys())
    records, seen = [], set()
    all_ok = True
    attempts = collisions = verify_drops = 0
    saturated = set()                 # schemas whose distinct space is used up
    streak = {n: 0 for n in names}    # consecutive collisions per schema
    per_schema = {n: 0 for n in names}
    rr = 0

    while (len(records) < TARGET and len(saturated) < len(names)
           and attempts < MAX_ATTEMPTS):
        attempts += 1
        # round-robin over schemas that still have unused plans -> keeps the
        # file balanced, but lets a small-space schema (e.g. transactions) cap
        # out instead of stalling the whole loop.
        active = [n for n in names if n not in saturated]
        name = active[rr % len(active)]
        rr += 1
        num_containers = random.choice([3, 3, 3, 4, 4, 5])
        mode = random.choice(["agg", "agg", "tf", "tf", "filter"])
        rec = build_record(name, num_containers, mode)

        key = _content_key(rec)
        if key in seen:           # logical duplicate -> reject, draw again
            collisions += 1
            streak[name] += 1
            if streak[name] >= SATURATE_AT:
                saturated.add(name)
                print(f"  [saturated] {name} at {per_schema[name]} unique plans")
            continue

        errs = verify(rec, name)
        if errs:                  # should never fire; never emit a bad record
            all_ok = False
            verify_drops += 1
            print(f"[FAIL] dropped {name}: {'; '.join(errs)}")
            continue

        streak[name] = 0
        seen.add(key)
        per_schema[name] += 1
        records.append(rec)
        if len(records) % 500 == 0:
            print(f"  accepted {len(records):>4}/{TARGET}  "
                  f"(attempts={attempts}, collisions={collisions})")

    with open("synthetic_planner_dataset.jsonl", "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")

    print(f"\nunique records written : {len(records)}")
    print(f"per-schema counts      : "
          + ", ".join(f"{n}={per_schema[n]}" for n in names))
    print(f"attempts               : {attempts}")
    print(f"collisions rejected    : {collisions}")
    print(f"verify drops           : {verify_drops}")
    short = len(records) < TARGET
    if short:
        print(f"WARNING: op-space exhausted before {TARGET}; "
              f"widen schemas/ops for more.")
    print("ALL RECORDS VERIFIED OK & UNIQUE" if all_ok and not short
          else "SOME RECORDS FAILED OR TARGET NOT MET")
    print("Written: synthetic_planner_dataset.jsonl")
    return 0 if all_ok and not short else 1


if __name__ == "__main__":
    sys.exit(main())
