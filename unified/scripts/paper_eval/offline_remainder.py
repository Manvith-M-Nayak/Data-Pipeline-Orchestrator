"""Offline remainder of the paper evaluation. No Azure, no model training.

Writes one JSON report. Does not read or write data/adf_monitor.db, the
feedback logs, or the model files (it only loads the outcome classifier
when that pickle is already on disk).

Sections:
  perf_gate     — abort precision/recall on the synthetic held-out fold
  anomalies     — precision/recall of detect_and_store on a temporary database
  filter_fuzz   — adversarial and benign filter strings through the compiler
  resource_ml   — heuristic settings vs ResourceAgent.analyze on 1,000 plans
"""
import ast
import asyncio
import copy
import io
import json
import os
import random
import statistics
import sys
import tempfile
from contextlib import redirect_stdout

# Temporary monitor database. Set before any project import so db_service
# binds DB_PATH here and the real data/adf_monitor.db is never opened.
_TMP_DB_DIR = tempfile.mkdtemp(prefix="paper_eval_anomaly_")
os.environ["DB_PATH"] = os.path.join(_TMP_DB_DIR, "eval.db")

_UNIFIED = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CALLER_CWD = os.getcwd()
sys.path.insert(0, _UNIFIED)
os.chdir(_UNIFIED)

REPORT = {}


def _pct(n, d):
    if not d:
        return None
    return round(100.0 * n / d, 2)


# ── Performance gate thresholds ──────────────────────────────────────────────

def perf_gate():
    clf_path = os.path.join(
        "performance_prediction_agent", "models", "outcome_classifier.pkl"
    )
    enc_path = os.path.join(
        "performance_prediction_agent", "models", "feature_encoder.pkl"
    )
    if not (os.path.isfile(clf_path) and os.path.isfile(enc_path)):
        return {
            "status": "not_run",
            "reason": (
                "outcome_classifier.pkl and feature_encoder.pkl are gitignored "
                "and are not in this checkout. The saved model was not retrained."
            ),
        }

    import joblib
    from sklearn.model_selection import train_test_split

    src_path = os.path.join("performance_prediction_agent", "run_training.py")
    src = open(src_path, encoding="utf-8").read()
    head = src[: src.index('print("STEP 1:')]
    head = head.replace('os.makedirs("models", exist_ok=True)', "pass")
    ns = {"__name__": "trainer_lib", "__file__": os.path.abspath(src_path)}
    exec(compile(head, src_path, "exec"), ns)

    print("perf_gate: generating synthetic rows (seed 42)...", flush=True)
    frame = ns["generate_dataset"]()
    feature_cols = ns["FEATURE_COLS"]
    encoder = joblib.load(enc_path)
    frame["complexity_encoded"] = encoder.transform(frame["complexity"])
    x_train, x_test, _y_train, y_test = train_test_split(
        frame[feature_cols],
        frame["outcome"],
        test_size=0.2,
        random_state=42,
        stratify=frame["outcome"],
    )
    del x_train, _y_train, frame

    clf = joblib.load(clf_path)
    print(f"perf_gate: scoring {len(x_test):,} held-out rows...", flush=True)
    proba = clf.predict_proba(x_test)
    classes = [str(c) for c in clf.classes_]
    fail_i = classes.index("failure")
    p_fail = proba[:, fail_i]
    top = clf.predict(x_test)
    y = y_test.to_numpy()
    true_fail = y == "failure"
    n_fail = int(true_fail.sum())

    def rates(abort):
        tp = int((abort & true_fail).sum())
        fp = int((abort & ~true_fail).sum())
        fn = int((~abort & true_fail).sum())
        return {
            "aborts": int(abort.sum()),
            "precision_pct": _pct(tp, tp + fp),
            "recall_pct": _pct(tp, tp + fn),
            "true_positives": tp,
            "false_positives": fp,
            "false_negatives": fn,
        }

    thresholds = {}
    for t in (0.5, 0.6, 0.7, 0.8, 0.9):
        thresholds[str(t)] = rates(p_fail >= t)
    return {
        "status": "measured",
        "distribution": "synthetic held-out fold only (seed 42, 80/20, no real-row blend)",
        "n_test": int(len(y)),
        "n_failure": n_fail,
        "failure_rate_pct": _pct(n_fail, len(y)),
        "current_rule_top_class_is_failure": rates(top == "failure"),
        "threshold_p_failure": thresholds,
    }


# ── Anomaly detector ─────────────────────────────────────────────────────────

SCHEMA = {"columns": ["region", "quantity", "unit_price"]}
SCHEMA_NEXT = {"columns": ["region", "quantity", "sku"]}


def _plan(name):
    return {"pipeline_name": name, "stages": [{"type": "notebook", "name": "transform"}]}


def _state(name, run_id, retries=0, cost=0.03, error=""):
    return {
        "run_id": run_id,
        "plan": _plan(name),
        "retries": retries,
        "error": error,
        "cost_estimate": {"total_usd": cost},
    }


def _result(run_id, status="ok", rows=50, message=""):
    return {
        "status": status,
        "run_id": run_id,
        "dbx_run_id": run_id,
        "rows_written": rows,
        "mode": "batch",
        "message": message,
    }


async def _seed(detect, key_of, name, n, schema, duration_s=100.0, cost=0.03):
    for i in range(n):
        rid = f"{name}-seed-{i}"
        await detect(
            _state(name, rid, cost=cost),
            _result(rid),
            int(duration_s * 1000),
            schema,
        )
    plan, result = _plan(name), _result(f"{name}-seed-0")
    return key_of(plan, result)


async def anomalies():
    from monitor_agent.services.anomaly_detector import detect_and_store, pipeline_key
    from monitor_agent.services.db_service import DBService, DB_PATH
    import aiosqlite

    db = DBService()
    await db.initialize()

    async def backdate(pipeline, hours):
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "UPDATE run_metrics SET created_at = datetime('now', ?) WHERE pipeline_name = ?",
                (f"-{hours} hours", pipeline),
            )
            await conn.commit()

    kinds = {}

    async def run_case(kind, build):
        normal_name = f"paper-{kind}-normal"
        await _seed(detect_and_store, pipeline_key, normal_name, 5, SCHEMA)
        if kind == "cold_start":
            key = pipeline_key(_plan(normal_name), _result("x"))
            await backdate(key, 8)
        fp = 0
        normal_any = 0
        for i in range(20):
            rid = f"{normal_name}-n-{i}"
            events = await detect_and_store(
                _state(normal_name, rid), _result(rid), 100_000, SCHEMA
            )
            got = {e["kind"] for e in events}
            if kind in got:
                fp += 1
            if got:
                normal_any += 1
        tp = 0
        extras = []
        for i in range(5):
            name = f"paper-{kind}-pos-{i}"
            await _seed(detect_and_store, pipeline_key, name, 5, SCHEMA)
            if kind == "cold_start":
                await backdate(pipeline_key(_plan(name), _result("x")), 8)
            if kind == "schema_drift":
                # The seed runs saved SCHEMA. The labelled run changes it.
                pass
            rid = f"{name}-hit"
            state, result, elapsed, schema = build(name, rid)
            events = await detect_and_store(state, result, elapsed, schema)
            got = sorted({e["kind"] for e in events})
            if kind in got:
                tp += 1
            extras.append(got)
        fn = 5 - tp
        kinds[kind] = {
            "positives": 5,
            "normals": 20,
            "true_positives": tp,
            "false_negatives": fn,
            "false_positives_on_normals": fp,
            "precision_pct": _pct(tp, tp + fp),
            "recall_pct": _pct(tp, 5),
            "normal_false_positive_pct": _pct(fp, 20),
            "normals_with_any_event": normal_any,
            "kinds_on_positives": extras,
        }

    await run_case("failure", lambda name, rid: (
        _state(name, rid, error="stage failed"),
        _result(rid, status="failed", message="stage failed"),
        100_000, SCHEMA,
    ))
    await run_case("timeout", lambda name, rid: (
        _state(name, rid, error="TIMEOUT waiting for the job"),
        _result(rid, status="failed", message="TIMEOUT waiting for the job"),
        100_000, SCHEMA,
    ))
    await run_case("retry_storm", lambda name, rid: (
        _state(name, rid, retries=2),
        _result(rid),
        100_000, SCHEMA,
    ))
    await run_case("slow_runtime", lambda name, rid: (
        _state(name, rid),
        _result(rid),
        300_000, SCHEMA,
    ))
    await run_case("cold_start", lambda name, rid: (
        _state(name, rid),
        _result(rid),
        300_000, SCHEMA,
    ))
    await run_case("zero_rows", lambda name, rid: (
        _state(name, rid),
        _result(rid, rows=0),
        100_000, SCHEMA,
    ))
    await run_case("cost_spike", lambda name, rid: (
        _state(name, rid, cost=0.20),
        _result(rid),
        100_000, SCHEMA,
    ))
    await run_case("schema_drift", lambda name, rid: (
        _state(name, rid),
        _result(rid),
        100_000, SCHEMA_NEXT,
    ))
    return {
        "status": "measured",
        "database": "temporary (DB_PATH overridden; data/adf_monitor.db not opened)",
        "data_skew": "not detectable on serverless; not scored",
        "per_kind": kinds,
    }


# ── Filter fuzz ──────────────────────────────────────────────────────────────

# `col` as a method is the compiler wrapping an identifier (`col("a").col("b")`),
# which is a field lookup, not a Python attribute load.
_ALLOWED_METHODS = {
    "col", "isin", "contains", "startswith", "endswith", "isNull", "isNotNull", "cast",
}


def _injection_reason(expr, keep_bare):
    tree = ast.parse(expr, mode="eval")
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id not in keep_bare:
            return f"name {node.id}"
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            return f"dunder {node.attr}"
        if isinstance(node, (ast.Lambda, ast.NamedExpr, ast.Starred)):
            return "disallowed construct"
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id not in keep_bare:
                return f"call {func.id}"
            if isinstance(func, ast.Attribute) and func.attr not in _ALLOWED_METHODS:
                return f"method {func.attr}"
    return None


def filter_fuzz():
    from executor_agent.notebook_builder import (
        UnsupportedTransformError, _PYSPARK_KEEP_BARE, _convert_filter,
    )

    attacks = []
    payloads = [
        "region = 'a'); import os; os.system('id'); ('",
        "region = 'a'); __import__('os').system('id'); ('",
        "'; DROP TABLE runs; --",
        "region = 'EU' or __import__('os').system('id')",
        "quantity > 1 and __builtins__",
        "__import__('os')",
        "eval('__import__(\"os\").system(\"id\")')",
        "exec('import os')",
        "open('/etc/passwd')",
        "lambda: __import__('os').system('id')",
        "region.__class__.__bases__",
        "getattr(__import__('os'), 'system')('id')",
        "quantity > 1; import os",
        "region = 'EU'\nimport os",
        "region = \"EU\"); import os; (\"",
        "region like '%'; import os; '%'",
        "quantity in (1) or os.system('id')",
        "region = 'EU' /* comment */ or 1=1",
        "col(__import__('os'))",
        "'; os.system('id'); '",
        "region = 'EU') | (os.system('id')) | (region = '",
        "quantity > 5 and compile('','','exec')",
        "().__class__.__bases__[0].__subclasses__()",
        "breakpoint()",
        "region = 'EU' or True",
        "quantity > 1 or globals()",
        "name = 'x'; __import__('os')",
        "region = 'EU' and getattr(region, '__class__')",
        "quantity >= 1; exec('print(1)')",
        "region in ('EU') or __import__('subprocess')",
    ]
    suffixes = ["", " --", " #", " /*", " or 1=1", " and 1=0", ") ", " ;"]
    cols = ["region", "quantity", "unit_price", "customer"]
    i = 0
    while len(attacks) < 1000:
        payload = payloads[i % len(payloads)]
        suffix = suffixes[(i // len(payloads)) % len(suffixes)]
        col = cols[(i // (len(payloads) * len(suffixes))) % len(cols)]
        attacks.append(f"{payload}{suffix} /*{col}-{i}*/")
        i += 1

    rejected = 0
    safe = 0
    injected = []
    for expr in attacks:
        try:
            compiled = _convert_filter(expr)
        except (UnsupportedTransformError, ValueError, SyntaxError):
            rejected += 1
            continue
        reason = _injection_reason(compiled, _PYSPARK_KEEP_BARE)
        if reason:
            injected.append({"input": expr[:180], "output": compiled[:180], "reason": reason})
        else:
            safe += 1

    benign = []
    for col, num in (("region", None), ("quantity", "5"), ("unit_price", "10.5")):
        if num:
            for op in (">", ">=", "<", "<=", "=", "!="):
                benign.append(f"{col} {op} {num}")
            benign.append(f"{col} between 1 and 10")
            benign.append(f"{col} in (1, 2, 3)")
            benign.append(f"{col} is null")
            benign.append(f"{col} is not null")
        else:
            benign.append(f"{col} = 'EU'")
            benign.append(f"{col} != 'EU'")
            benign.append(f"{col} in ('EU', 'US')")
            benign.append(f"{col} like '%EU%'")
            benign.append(f"{col} like 'EU%'")
            benign.append(f"{col} not like '%EU%'")
            benign.append(f"{col} is null")
            benign.append(f"{col} is not null")
            benign.append(f"{col} = 'foo and bar'")
    benign.extend([
        "region = 'EU' and quantity > 5",
        "region = 'EU' or quantity > 5",
        "(region = 'EU') and (quantity > 5)",
        "region = 'EU' and quantity > 5 and unit_price >= 10.5",
        "region = 'EU' or quantity > 5 and unit_price < 10.5",
        "quantity between 1 and 10 and region = 'EU'",
    ])
    # Repeat the benign grammar across extra columns so the benign set is
    # larger than a handful of hand-written lines, without changing the grammar.
    for col in ("channel", "status", "country"):
        benign.append(f"{col} = 'web'")
        benign.append(f"{col} in ('a', 'b')")
        benign.append(f"{col} like '%x%'")
        benign.append(f"{col} = 'web' and quantity > 5")

    benign_ok = 0
    benign_fail = []
    for expr in benign:
        try:
            _convert_filter(expr)
            benign_ok += 1
        except (UnsupportedTransformError, ValueError, SyntaxError) as exc:
            benign_fail.append({"input": expr, "error": str(exc)[:180]})

    return {
        "status": "measured",
        "adversarial": len(attacks),
        "rejected": rejected,
        "compiled_inert": safe,
        "executable_injections": len(injected),
        "injection_examples": injected[:8],
        "benign": len(benign),
        "benign_compiled": benign_ok,
        "benign_compiled_pct": _pct(benign_ok, len(benign)),
        "benign_failures": benign_fail[:12],
    }


# ── Resource ML vs heuristic ─────────────────────────────────────────────────

def resource_ml():
    from planner_agent.planner_common import _normalize_container_names, _structural_validate
    from resource_agent.resource_agent import (
        MAX_DIU, MAX_TOTAL_MEM_GB, MAX_WORKERS, ResourceAgent,
    )

    path = "planner_agent/training/datasets/planner_config_dataset.jsonl"
    if not os.path.isfile(path):
        return {"status": "not_run", "reason": f"missing {path}"}

    rows = [json.loads(line) for line in open(path, encoding="utf-8")]
    for rec in rows:
        rec["config"] = _normalize_container_names(rec["config"])
    random.seed(20261003)
    sample = random.sample(rows, 1000)
    agent = ResourceAgent()

    kinds = ("workers", "diu", "memory_gb", "shuffle")
    pairs = {k: [] for k in kinds}
    by_type = {
        stype: {k: [] for k in kinds}
        for stype in ("copy", "notebook")
    }
    exact_by_type = {
        stype: {k: 0 for k in kinds}
        for stype in ("copy", "notebook")
    }
    over = {
        "heuristic": {"workers": 0, "diu": 0, "memory": 0},
        "ml": {"workers": 0, "diu": 0, "memory": 0},
    }
    exact = {k: 0 for k in kinds}
    n_stages = {k: 0 for k in kinds}
    ml_sized_stages = 0
    stages_seen = 0

    for rec in sample:
        schema = rec["schema"]
        with redirect_stdout(io.StringIO()):
            repaired = _structural_validate(copy.deepcopy(rec["config"]), schema)
            analyzed = agent.analyze(
                repaired, csv_size_bytes=int(schema.get("row_count") or 0) * 140, schema=schema,
            )
        by_name = {a["stage_name"]: a for a in analyzed.get("allocations", [])}
        h_over = {"workers": False, "diu": False, "memory": False}
        m_over = {"workers": False, "diu": False, "memory": False}
        mb = (int(schema.get("row_count") or 0) * 140) / (1024 * 1024)
        for stage in repaired["stages"]:
            cf = 1.0
            if stage.get("type") == "copy":
                req = agent._predict_copy(stage.get("name", "unknown"), stage, mb, cf)
            else:
                req = agent._predict_notebook(
                    stage.get("name", "unknown"), stage, mb, schema, cf,
                )
            alloc = by_name.get(stage.get("name"))
            if alloc is None:
                continue
            stages_seen += 1
            if alloc.get("ml_sized"):
                ml_sized_stages += 1
            h = {
                "workers": req.estimated_workers,
                "diu": req.estimated_diu,
                "memory_gb": round(req.estimated_mem_gb, 2),
                "shuffle": req.recommended_shuffle,
            }
            m = {
                "workers": alloc["workers"],
                "diu": alloc["diu"],
                "memory_gb": round(alloc["memory_gb"], 2),
                "shuffle": alloc["shuffle_partitions"],
            }
            stype = "copy" if stage.get("type") == "copy" else "notebook"
            for key in kinds:
                diff = abs(h[key] - m[key])
                pairs[key].append(diff)
                by_type[stype][key].append(diff)
                n_stages[key] += 1
                if h[key] == m[key]:
                    exact[key] += 1
                    exact_by_type[stype][key] += 1
            if h["workers"] > MAX_WORKERS:
                h_over["workers"] = True
            if h["diu"] > MAX_DIU:
                h_over["diu"] = True
            if h["memory_gb"] > MAX_TOTAL_MEM_GB:
                h_over["memory"] = True
            if m["workers"] > MAX_WORKERS:
                m_over["workers"] = True
            if m["diu"] > MAX_DIU:
                m_over["diu"] = True
            if m["memory_gb"] > MAX_TOTAL_MEM_GB:
                m_over["memory"] = True
        for key in over["heuristic"]:
            over["heuristic"][key] += h_over[key]
            over["ml"][key] += m_over[key]

    def _summ(diffs, hits):
        return {
            "stages": len(diffs),
            "exact_match_pct": _pct(hits, len(diffs)),
            "mean_abs_diff": round(statistics.mean(diffs), 3) if diffs else None,
        }

    summary = {key: _summ(pairs[key], exact[key]) for key in kinds}
    by_type_summary = {
        stype: {
            key: _summ(by_type[stype][key], exact_by_type[stype][key])
            for key in kinds
        }
        for stype in by_type
    }
    return {
        "status": "measured",
        "plans": 1000,
        "seed": 20261003,
        "stages": stages_seen,
        "ml_sized_stages": ml_sized_stages,
        "limits": {"workers": MAX_WORKERS, "diu": MAX_DIU, "memory_gb": MAX_TOTAL_MEM_GB},
        "agreement": summary,
        "agreement_by_stage_type": by_type_summary,
        "plans_over_limit": over,
        "live_runtime": "not measured",
    }


def main():
    only = {p.strip() for p in os.environ.get("PAPER_EVAL_ONLY", "").split(",") if p.strip()}

    def wanted(name):
        return not only or name in only

    if wanted("filter_fuzz"):
        print("filter_fuzz...", flush=True)
        REPORT["filter_fuzz"] = filter_fuzz()
    if wanted("resource_ml"):
        print("resource_ml...", flush=True)
        REPORT["resource_ml"] = resource_ml()
    if wanted("anomalies"):
        print("anomalies...", flush=True)
        REPORT["anomalies"] = asyncio.run(anomalies())
    if wanted("perf_gate"):
        print("perf_gate...", flush=True)
        REPORT["perf_gate"] = perf_gate()

    text = json.dumps(REPORT, indent=1)
    print(text)
    dest = sys.argv[1] if len(sys.argv) > 1 else "offline_remainder.json"
    if not os.path.isabs(dest):
        dest = os.path.join(_CALLER_CWD, dest)
    with open(dest, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(f"wrote {dest}", flush=True)


if __name__ == "__main__":
    try:
        main()
    finally:
        import shutil
        shutil.rmtree(_TMP_DB_DIR, ignore_errors=True)
