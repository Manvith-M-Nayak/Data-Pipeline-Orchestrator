"""Live benchmark on Azure (ADF + Databricks) through the running backend.

Uses the real product path: /api/schema/detect -> /api/planner/plan ->
/api/manager/run -> poll /api/manager/status. Every run goes through the
learning loop, so later runs see corrections learned from earlier ones.

    uvicorn main:app --host 127.0.0.1 --port 8000      # backend (starts Ollama)
    python scripts/paper_eval/live_benchmark.py batch    out_dir
    python scripts/paper_eval/live_benchmark.py parallel out_dir
    PAPER_EVAL_REPEATS=4 python scripts/paper_eval/live_benchmark.py batch out_dir
    python scripts/paper_eval/live_benchmark.py pinned   out_dir

Every completed batch / pinned run's output is downloaded and compared with a
reference computed locally from the same CSV (row count and values).

Writes out_dir/<experiment>.jsonl (one line per run) and the generated CSVs.
Costs real (small) Azure money: about $0.02-0.10 per run by the cost formula.
"""
import csv
import json
import os
import random
import sys
import time

import requests

API = "http://127.0.0.1:8000/api"
SIZES = {"xs": 1_000, "m": 50_000, "l": 400_000}
SHAPES = {
    "filter2": "keep only rows where region = 'EU' and quantity > 5",
    "agg3": "Stage 1: keep rows where quantity > 5. "
            "Stage 2: compute the average unit_price and a row count per region",
}
REGIONS = ["EU", "US", "APAC", "LATAM", "MEA"]
PRODUCTS = ["Mouse", "Keyboard", "Monitor", "Laptop Stand", "Headset", "Webcam"]


def make_csv(path, n, seed=7):
    rng = random.Random(seed + n)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "region", "channel", "product", "quantity",
                    "unit_price", "discount", "customer_email"])
        for i in range(n):
            w.writerow([100000 + i, rng.choice(REGIONS), rng.choice(["web", "store", "phone"]),
                        rng.choice(PRODUCTS), rng.randint(1, 20), round(rng.uniform(5, 500), 2),
                        round(rng.uniform(0, 50), 2), f"user{i}@example.com"])


def detect(path):
    with open(path, "rb") as f:
        r = requests.post(f"{API}/schema/detect", files={"csv_file": (os.path.basename(path), f)})
    r.raise_for_status()
    return r.json()


def plan(schema, prompt):
    t = time.time()
    r = requests.post(f"{API}/planner/plan", json={"schema": schema, "prompt": prompt}, timeout=900)
    r.raise_for_status()
    body = r.json()
    ver = body.get("verification") or {}
    return body["config"], {"plan_latency_s": round(time.time() - t, 1),
                            "used_fallback": body.get("used_fallback"),
                            "verified": ver.get("verified"), "attempts": ver.get("attempts")}


STATE_DIR = None  # set in __main__: full run states are saved here


def run(path, config, schema, prompt, timeout_s=2400):
    t = time.time()
    with open(path, "rb") as f:
        r = requests.post(f"{API}/manager/run",
                          files={"csv_file": (os.path.basename(path), f)},
                          data={"pipeline_config": json.dumps(config), "schema": json.dumps(schema),
                                "user_request": prompt}, timeout=600)
    r.raise_for_status()
    run_id = r.json()["run_id"]
    while time.time() - t < timeout_s:
        time.sleep(5)
        st = requests.get(f"{API}/manager/status/{run_id}").json()
        if st.get("status") in ("completed", "failed"):
            break
    st["_wall_s"] = round(time.time() - t, 1)
    if STATE_DIR:
        with open(os.path.join(STATE_DIR, f"{run_id}.json"), "w") as f:
            json.dump(st, f)
    return st


def reference(path, shape):
    """What a correct pipeline must output, computed locally from the same CSV."""
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if shape == "filter2":
        kept = [r for r in rows if r["region"] == "EU" and int(r["quantity"]) > 5]
        return {"rows": len(kept), "sum_quantity": sum(int(r["quantity"]) for r in kept)}
    if shape == "agg3":
        groups = {}
        for r in rows:
            if int(r["quantity"]) > 5:
                g = groups.setdefault(r["region"], [0, 0.0])
                g[0] += 1
                g[1] += float(r["unit_price"])
        return {"rows": len(groups),
                "per_region": {k: {"count": n, "avg_unit_price": t / n} for k, (n, t) in groups.items()}}
    return None


def verify(state, path, shape):
    """Download the run's output and compare it with reference(). Values, not just counts."""
    ref = reference(path, shape)
    ex = state.get("executor_result") or {}
    sink = ex.get("sink_container") or ((state.get("plan") or {}).get("containers_to_create") or [None])[-1]
    if not ref or not sink or state.get("status") != "completed":
        return {"checked": False}
    r = requests.get(f"{API}/executor/download/{sink}", timeout=300)
    if r.status_code != 200:
        return {"checked": False, "error": f"download {r.status_code}"}
    if STATE_DIR:   # keep the downloaded output as evidence
        odir = os.path.join(os.path.dirname(STATE_DIR), "outputs")
        os.makedirs(odir, exist_ok=True)
        with open(os.path.join(odir, f"{state.get('run_id')}.csv"), "w") as f:
            f.write(r.text)
    out = list(csv.DictReader(r.text.splitlines()))
    res = {"checked": True, "expected_rows": ref["rows"], "output_rows": len(out)}
    if shape == "filter2":
        try:
            got = sum(int(float(o["quantity"])) for o in out)
        except (KeyError, ValueError):
            got = None
        res["expected_sum_quantity"] = ref["sum_quantity"]
        res["output_sum_quantity"] = got
        res["correct"] = len(out) == ref["rows"] and got == ref["sum_quantity"]
    else:
        # The planner names the output columns; find the count and the average
        # by value: a column matches if every region's value agrees with the
        # reference (count exactly, average within 1e-6 relative).
        exp = ref["per_region"]
        by_region = {o.get("region"): o for o in out}
        def matches(col, key, tol):
            try:
                return all(abs(float(by_region[g][col]) - v[key]) <= tol * max(1.0, abs(v[key]))
                           for g, v in exp.items())
            except (KeyError, ValueError, TypeError):
                return False
        cols = [c for c in (out[0].keys() if out else []) if c != "region"]
        res["count_column"] = next((c for c in cols if matches(c, "count", 0)), None)
        res["avg_column"] = next((c for c in cols if matches(c, "avg_unit_price", 1e-6)), None)
        res["correct"] = (set(by_region) == set(exp) and res["count_column"] is not None
                          and res["avg_column"] is not None)
    return res


def record(state, extra):
    pp = state.get("performance_prediction") or {}
    rp = state.get("resource_plan") or {}
    co = state.get("cost_optimization") or {}
    ex = state.get("executor_result") or {}
    return {
        **extra,
        "run_id": state.get("run_id"), "status": state.get("status"),
        "error": (state.get("error") or "")[:300], "retries": state.get("retries"),
        "execution_s": state.get("execution_s"), "wall_s": state.get("_wall_s"),
        "stages": len((state.get("plan") or {}).get("stages", [])),
        "execution_groups": (state.get("plan") or {}).get("execution_groups"),
        "complexity": (state.get("predictions") or {}).get("complexity"),
        "resource_estimated_total_s": rp.get("estimated_total_s"),
        "resource_correction_factors": rp.get("correction_factors"),
        "perf_predicted_total_s": pp.get("predicted_total_s"),
        "perf_source": pp.get("prediction_source"), "perf_outcome": pp.get("outcome"),
        "perf_confidence": pp.get("confidence"),
        "expected_duration_s": pp.get("expected_duration_s"),
        "expected_duration_basis": pp.get("expected_duration_basis"),
        "slower_than_usual": pp.get("slower_than_usual"),
        "manager_cost_usd": (state.get("cost_estimate") or {}).get("total_usd"),
        "cost_agent_usd": (co.get("estimated_cost") or {}).get("total_usd"),
        "cost_recommendations": len(co.get("recommendations") or []),
        "cost_source": co.get("optimization_source"),
        "cost_slowdown_applied": state.get("cost_slowdown_applied"),
        "execution_settings": state.get("execution_settings"),
        "rows_written": ex.get("rows_written"),
        "post_assurance": (state.get("assurance") or {}).get("passed"),
    }


def batch(out, repeats=None):
    repeats = repeats or int(os.getenv("PAPER_EVAL_REPEATS", "2"))
    log = open(os.path.join(out, "batch.jsonl"), "a")
    plans = {}
    for size, n in SIZES.items():
        path = os.path.join(out, f"sales_{size}.csv")
        if not os.path.exists(path):
            make_csv(path, n)
        schema = detect(path)
        for shape, prompt in SHAPES.items():
            cfg, pinfo = plan(schema, prompt)
            plans[(size, shape)] = (path, cfg, schema, prompt, pinfo)
            print(f"planned {size}/{shape}: {pinfo}", flush=True)
    for rep in range(repeats):
        for (size, shape), (path, cfg, schema, prompt, pinfo) in plans.items():
            st = run(path, cfg, schema, prompt)
            row = record(st, {"experiment": "batch", "size": size, "rows": SIZES[size],
                              "bytes": os.path.getsize(path), "shape": shape, "rep": rep,
                              **(pinfo if rep == 0 else {})})
            row["output_check"] = verify(st, path, shape)
            log.write(json.dumps(row) + "\n")
            log.flush()
            print(size, shape, rep, row["status"], row["execution_s"], row["perf_predicted_total_s"],
                  row["error"][:120], flush=True)


def fanout_plan(sequential):
    """copy raw->bronze, then two independent notebooks that both read bronze."""
    c = ["bench-raw", "bench-bronze", "bench-filtered", "bench-summary"]
    cfg = {
        "containers": {f"stage{i}": n for i, n in enumerate(c)},
        "containers_to_create": c,
        "datasets": [{"name": "DS_BenchRaw", "container": c[0], "role": "source"},
                     {"name": "DS_BenchBronze", "container": c[1], "role": "intermediate"},
                     {"name": "DS_BenchFiltered", "container": c[2], "role": "sink"},
                     {"name": "DS_BenchSummary", "container": c[3], "role": "sink"}],
        "stages": [
            {"name": "Ingest_Raw_To_Bronze", "type": "copy", "source_dataset": "DS_BenchRaw",
             "sink_dataset": "DS_BenchBronze", "diu": 2},
            {"name": "Filter_Bronze", "type": "notebook", "source_container": c[1],
             "sink_container": c[2], "transformations": [],
             "filter_condition": "region = 'EU' and quantity > 5", "num_workers": 1,
             "shuffle_partitions": 8},
            {"name": "Summarize_Bronze", "type": "notebook", "source_container": c[1],
             "sink_container": c[3], "transformations": [], "filter_condition": None,
             "aggregation": {"group_by": ["region"], "aggregations": [
                 {"op": "avg", "column": "unit_price", "alias": "avg_price"},
                 {"op": "count", "column": "*", "alias": "orders"}]},
             "num_workers": 1, "shuffle_partitions": 8},
        ],
        "execution_order": ["Ingest_Raw_To_Bronze", "Filter_Bronze", "Summarize_Bronze"],
        "num_containers": 4,
        "recommended_settings": {"diu": 2, "num_workers": 1, "shuffle_partitions": 8,
                                 "node_type": "Standard_DS3_v2"},
        "reasoning": "fan-out benchmark plan",
    }
    cfg["execution_groups"] = ([["Ingest_Raw_To_Bronze"], ["Filter_Bronze"], ["Summarize_Bronze"]]
                               if sequential else
                               [["Ingest_Raw_To_Bronze"], ["Filter_Bronze", "Summarize_Bronze"]])
    return cfg


def parallel(out, repeats=2, size="m"):
    log = open(os.path.join(out, "parallel.jsonl"), "a")
    path = os.path.join(out, f"sales_{size}.csv")
    if not os.path.exists(path):
        make_csv(path, SIZES[size])
    schema = detect(path)
    prompt = "keep EU rows with quantity > 5, and separately average unit_price and count per region"
    for rep in range(repeats):
        for sequential in (True, False):
            st = run(path, fanout_plan(sequential), schema, prompt)
            row = record(st, {"experiment": "parallel", "mode": "sequential" if sequential else "parallel",
                              "rep": rep, "size": size, "rows": SIZES[size]})
            log.write(json.dumps(row) + "\n")
            log.flush()
            print(row["mode"], rep, row["status"], row["execution_s"], row["execution_groups"],
                  row["error"][:120], flush=True)


def streaming(out, drops=3, rows_per_drop=2_000):
    """Same two-step request as a single merged stream stage vs chained stages.
    Each drop uploads a new file and runs one incremental tick."""
    log = open(os.path.join(out, "streaming.jsonl"), "a")
    seed_path = os.path.join(out, "stream_seed.csv")
    make_csv(seed_path, 200, seed=1)
    schema = detect(seed_path)
    prompt = "Stage 1: keep rows where region = 'EU'. Stage 2: keep rows where quantity > 5."
    for layout in ("single", "multi"):
        t = time.time()
        r = requests.post(f"{API}/planner/plan", timeout=900, json={
            "schema": schema, "prompt": prompt, "mode": "streaming", "stream_layout": layout})
        r.raise_for_status()
        cfg = r.json()["config"]
        plan_s = round(time.time() - t, 1)
        stream_stages = [s for s in cfg.get("stages", []) if s.get("type") == "stream"]
        r = requests.post(f"{API}/manager/stream/start", json={"config": cfg, "schema": schema})
        if r.status_code != 200:
            log.write(json.dumps({"experiment": "streaming", "layout": layout,
                                  "error": r.text[:300]}) + "\n")
            continue
        sid = r.json()["stream_id"]
        for d in range(drops):
            path = os.path.join(out, f"stream_{layout}_{d}.csv")
            make_csv(path, rows_per_drop, seed=100 + d)
            t = time.time()
            with open(path, "rb") as f:
                res = requests.post(f"{API}/manager/stream/{sid}/data",
                                    files={"csv_file": (os.path.basename(path), f)}, timeout=3600)
            tick_s = round(time.time() - t, 1)
            body = res.json() if res.headers.get("content-type", "").startswith("application/json") else {}
            preview = requests.get(f"{API}/manager/stream/{sid}/output", params={"limit": 100000}).json()
            row = {"experiment": "streaming", "layout": layout, "drop": d,
                   "stream_stages": len(stream_stages), "plan_latency_s": plan_s if d == 0 else None,
                   "tick_wall_s": tick_s, "ok": body.get("ok"),
                   "message": ((body.get("tick") or {}).get("message") or "")[:200],
                   "sink_rows_total": len(preview.get("rows") or []) if isinstance(preview, dict) else None,
                   "stream_id": sid}
            log.write(json.dumps(row) + "\n")
            log.flush()
            print(row, flush=True)
        requests.post(f"{API}/manager/stream/{sid}/stop")


def pinned(out, repeats=2):
    """Same plan, alternately with the Resource Agent's settings applied and
    with the planner's own DIU / shuffle pinned (the Manager then leaves them
    alone, _execution_plan) — i.e. without the Resource Agent's settings."""
    log = open(os.path.join(out, "pinned.jsonl"), "a")
    prompt = SHAPES["filter2"]
    for size, n in SIZES.items():
        path = os.path.join(out, f"sales_{size}.csv")
        if not os.path.exists(path):
            make_csv(path, n)
        schema = detect(path)
        cfg, pinfo = plan(schema, prompt)
        print(f"planned {size}: {pinfo}", flush=True)
        for rep in range(repeats):
            for mode in ("resource_agent", "planner_pinned"):
                c = json.loads(json.dumps(cfg))
                if mode == "planner_pinned":
                    for st_ in c.get("stages", []):
                        st_["pinned_settings"] = sorted(set(st_.get("pinned_settings") or []) |
                                                        {"diu", "shuffle_partitions"})
                st = run(path, c, schema, prompt)
                row = record(st, {"experiment": "pinned", "mode": mode, "size": size,
                                  "rows": n, "rep": rep, **(pinfo if rep == 0 else {})})
                row["planner_settings"] = {s_.get("name"): {"diu": s_.get("diu"),
                                                            "shuffle": s_.get("shuffle_partitions")}
                                           for s_ in cfg.get("stages", [])}
                row["output_check"] = verify(st, path, "filter2")
                log.write(json.dumps(row) + "\n")
                log.flush()
                print(size, mode, rep, row["status"], row["execution_s"], row["execution_settings"],
                      row["output_check"].get("correct"), flush=True)


def recheck(out, size="xs", shape="agg3", repeats=1):
    """Plan and run one (size, shape) again and keep its downloaded output."""
    log = open(os.path.join(out, "recheck.jsonl"), "a")
    path = os.path.join(out, f"sales_{size}.csv")
    if not os.path.exists(path):
        make_csv(path, SIZES[size])
    schema = detect(path)
    cfg, pinfo = plan(schema, SHAPES[shape])
    for rep in range(repeats):
        st = run(path, cfg, schema, SHAPES[shape])
        row = record(st, {"experiment": "recheck", "size": size, "shape": shape, "rep": rep, **pinfo})
        row["output_check"] = verify(st, path, shape)
        log.write(json.dumps(row) + "\n")
        log.flush()
        print(size, shape, rep, row["status"], row["output_check"], flush=True)


if __name__ == "__main__":
    which, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    STATE_DIR = os.path.join(out, "states")
    os.makedirs(STATE_DIR, exist_ok=True)
    fn = {"batch": batch, "parallel": parallel, "streaming": streaming, "pinned": pinned,
          "recheck": recheck}[which]
    if which == "parallel" and len(sys.argv) > 3:
        fn(out, size=sys.argv[3])      # e.g. xs: the 3 MB fan-out plan is aborted by the perf gate
    else:
        fn(out)
