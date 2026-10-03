"""Offline with/without experiments (no LLM, no Azure).

R  — with vs without Resource Agent: planner settings vs Resource Agent settings
     against the student-tier hard limits, on planner-format configs.
A  — with vs without the Assurance structural gate: inject faults into valid
     plans; where is each fault caught (gate / executor pre-checks / not until cloud)?
S  — with vs without the system: what a person would author by hand for the
     same pipelines (notebook lines, ADF objects, settings decisions).
"""
import copy, io, json, os, random, statistics as st, sys
from contextlib import redirect_stdout

_UNIFIED = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CALLER_CWD = os.getcwd()  # output paths resolve from where the script was run
sys.path.insert(0, _UNIFIED)
os.chdir(_UNIFIED)
from resource_agent.resource_agent import ResourceAgent, MAX_WORKERS, MAX_DIU, MAX_CONCURRENT
from assurance_agent import AssuranceAgent
from executor_agent.plan_safety import plan_safety_issues
from executor_agent.notebook_builder import build_notebook_source
from planner_agent.planner_common import _normalize_container_names

random.seed(20261003)
rows = [json.loads(l) for l in open("planner_agent/training/datasets/planner_config_dataset.jsonl")]
# training rows use short names (l1, l2); the planner makes them Azure-safe at runtime
for r in rows:
    r["config"] = _normalize_container_names(r["config"])
sample = random.sample(rows, 1000)
out = {}

# ── R: Resource Agent ────────────────────────────────────────────────────────
ra = ResourceAgent()
viol_without = viol_with = infeasible = 0
w_plan, w_res, d_plan, d_res, m_res = [], [], [], [], []
by_size = {}
for rec in sample:
    cfg, schema = rec["config"], rec["schema"]
    size = schema["size_hint"].split()[0]
    stages = cfg["stages"]
    over = any((s["type"] == "notebook" and s.get("num_workers", 0) > MAX_WORKERS) or
               (s["type"] == "copy" and s.get("diu", 0) > MAX_DIU) for s in stages)
    viol_without += over
    by_size.setdefault(size, [0, 0])
    by_size[size][0] += 1
    by_size[size][1] += over
    for s in stages:
        (w_plan if s["type"] == "notebook" else d_plan).append(s.get("num_workers" if s["type"] == "notebook" else "diu", 0))
    with redirect_stdout(io.StringIO()):
        rp = ra.analyze(cfg, csv_size_bytes=int(schema["row_count"] * 140), schema=schema)
    infeasible += not rp.get("feasible", True)
    over2 = any((a["stage_type"] == "notebook" and a["workers"] > MAX_WORKERS) or
                (a["stage_type"] == "copy" and a["diu"] > MAX_DIU) for a in rp.get("allocations", []))
    viol_with += over2
    for a in rp.get("allocations", []):
        (w_res if a["stage_type"] == "notebook" else d_res).append(a["workers"] if a["stage_type"] == "notebook" else a["diu"])
        if a["stage_type"] == "notebook":
            m_res.append(a["memory_gb"])
out["R"] = {
    "plans": len(sample),
    "exceed_limits_without_resource": viol_without,
    "exceed_limits_with_resource": viol_with,
    "flagged_infeasible_with_resource": infeasible,
    "exceed_by_size_without": {k: f"{v[1]}/{v[0]}" for k, v in by_size.items()},
    "mean_workers_per_notebook_stage": [round(st.mean(w_plan), 2), round(st.mean(w_res), 2)],
    "max_workers": [max(w_plan), max(w_res)],
    "mean_diu_per_copy_stage": [round(st.mean(d_plan), 2), round(st.mean(d_res), 2)],
    "max_diu": [max(d_plan), max(d_res)],
}

# ── A: Assurance structural gate ─────────────────────────────────────────────
def first_notebook(cfg):
    return next(s for s in cfg["stages"] if s["type"] == "notebook")

def f_unknown_col(cfg):
    first_notebook(cfg)["filter_condition"] = "discount_code = 'X'"
def f_bad_agg(cfg):
    first_notebook(cfg)["aggregation"] = {"group_by": [cfg["_c0"]], "aggregations": [{"op": "median", "column": cfg["_c0"], "alias": "m"}]}
def f_order(cfg):
    nb = first_notebook(cfg)
    cfg["stages"][0]["type"] = "notebook"
    nb["type"] = "copy"
def f_missing_key(cfg):
    cfg.pop("execution_order", None)
def f_bad_name(cfg):
    first_notebook(cfg)["sink_container"] = "Gold_Data!"
def f_injection(cfg):
    first_notebook(cfg)["filter_condition"] = "region = 'a'); import os; os.system('id'); ('"
def f_bad_type(cfg):
    cfg["stages"][1]["type"] = "spark_sql"

FAULTS = {"unknown column": f_unknown_col, "unsupported aggregation": f_bad_agg,
          "stage order inverted": f_order, "missing required key": f_missing_key,
          "unsafe container name": f_bad_name, "code-injection filter": f_injection,
          "unknown stage type": f_bad_type}

def executor_catches(cfg):
    """What the executor checks before any cloud call (no assurance gate)."""
    if plan_safety_issues(cfg):
        return True
    try:
        for s in cfg["stages"]:
            if s.get("type") in ("notebook", "stream"):
                build_notebook_source(s, "acct")
            elif s.get("type") != "copy":
                return True  # executor refuses unknown types
    except Exception:
        return True
    return False

agent = AssuranceAgent()
A = {}
# fault-inject only into plans the gate accepts when clean (cast plans are
# falsely rejected — reported separately as a gate false-positive rate)
clean_pool = [r for r in rows if agent.assure("", r["config"], r["schema"], run_semantic=False).overall_status == "pass"]
cast_false_rejects = len(rows) - len(clean_pool)
base = random.sample(clean_pool, 200)
for fname, fn in FAULTS.items():
    gate = execu = cloud = 0
    for rec in base:
        cfg = copy.deepcopy(rec["config"])
        cfg["_c0"] = rec["schema"]["columns"][1]
        fn(cfg)
        cfg.pop("_c0")
        g = agent.assure("", cfg, rec["schema"], run_semantic=False).overall_status == "fail"
        e = executor_catches(cfg)
        gate += g
        if not g:
            pass
        execu += e
        cloud += (not e)
    A[fname] = {"n": len(base), "caught_by_gate": gate, "caught_by_executor_only_path": execu,
                "reaches_cloud_without_gate": cloud, "reaches_cloud_with_gate": sum(
                    0 for _ in [0])}
# with the gate, a plan reaches the cloud only if BOTH miss it
for fname, fn in FAULTS.items():
    both = 0
    for rec in base:
        cfg = copy.deepcopy(rec["config"])
        cfg["_c0"] = rec["schema"]["columns"][1]
        fn(cfg)
        cfg.pop("_c0")
        g = agent.assure("", cfg, rec["schema"], run_semantic=False).overall_status == "fail"
        both += (not g) and (not executor_catches(cfg))
    A[fname]["reaches_cloud_with_gate"] = both
clean_fp = sum(agent.assure("", r["config"], r["schema"], run_semantic=False).overall_status == "fail" for r in base)
out["A"] = {"faults": A, "false_rejects_on_clean_plans": f"{cast_false_rejects}/{len(rows)}"}

# ── S: with vs without the system (manual authoring proxy) ───────────────────
def manual_effort(cfg):
    nb_lines = 0
    nb = 0
    for s in cfg["stages"]:
        if s.get("type") in ("notebook", "stream"):
            src = build_notebook_source(s, "acct")
            nb_lines += sum(1 for l in src.splitlines() if l.strip() and not l.strip().startswith("#"))
            nb += 1
    copies = sum(1 for s in cfg["stages"] if s.get("type") == "copy")
    adf_objects = len(cfg.get("datasets", [])) + (1 if copies else 0) + copies + 1  # datasets + pipeline + copy activities + linked service
    decisions = copies * 1 + nb * 2  # DIU per copy; workers + shuffle per notebook
    return nb_lines, nb, len(cfg.get("containers_to_create", [])), adf_objects, decisions

eff = [manual_effort(r["config"]) for r in sample]
import sqlite3
real = []
for (sj,) in sqlite3.connect("data/adf_monitor.db").execute("select state_json from manager_runs where status='completed'"):
    plan = json.loads(sj).get("plan") or {}
    if plan.get("stages"):
        try:
            real.append(manual_effort(plan))
        except Exception:
            pass
def summ(e):
    return {"pipelines": len(e),
            "notebook_code_lines_mean": round(st.mean(x[0] for x in e), 1),
            "notebook_code_lines_range": [min(x[0] for x in e), max(x[0] for x in e)],
            "notebooks_mean": round(st.mean(x[1] for x in e), 2),
            "containers_mean": round(st.mean(x[2] for x in e), 2),
            "adf_objects_mean": round(st.mean(x[3] for x in e), 2),
            "sizing_decisions_mean": round(st.mean(x[4] for x in e), 2)}
out["S"] = {"dataset_plans": summ(eff), "real_completed_plans": summ(real) if real else None}

print(json.dumps(out, indent=1))
json.dump(out, open(os.path.join(_CALLER_CWD, sys.argv[1] if len(sys.argv) > 1 else "offline_results.json"), "w"), indent=1)
