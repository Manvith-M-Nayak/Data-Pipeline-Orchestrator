"""Offline with/without experiments (no LLM, no Azure).

R  — with vs without Resource Agent: the settings the planner hands over (model
     output, and after the planner's own repair layer) vs the Resource Agent's,
     against the student-tier hard limits.
A  — with vs without the Assurance structural gate: inject faults into valid
     plans and pass them through the real layers in run order (Manager Phase-1
     validation, gate, executor pre-cloud checks); classify where each stops.
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
# "Without the Resource Agent" = the settings the planner itself hands over.
# The planner's repair layer (_structural_validate) already caps notebook
# workers by data size, so the fair baseline is the repaired plan, not the raw
# training target. The raw target is reported too, as the model's own output.
from planner_agent.planner_common import _structural_validate
from executor_agent.notebook_builder import _AGG_FUNCS

ra = ResourceAgent()

def over_limits_plan(stages):
    return any((s.get("type") == "notebook" and (s.get("num_workers") or 0) > MAX_WORKERS) or
               (s.get("type") == "copy" and (s.get("diu") or 0) > MAX_DIU) for s in stages)

def settings(stages):
    w = [s.get("num_workers") or 0 for s in stages if s.get("type") == "notebook"]
    d = [s.get("diu") or 0 for s in stages if s.get("type") == "copy"]
    return w, d

R = {"plans": len(sample), "limits": {"MAX_WORKERS": MAX_WORKERS, "MAX_DIU": MAX_DIU}}
acc = {k: {"over": 0, "w": [], "d": [], "by_size": {}} for k in ("model_raw", "planner_repaired", "resource_agent")}
infeasible = 0
for rec in sample:
    schema = rec["schema"]
    size = schema["size_hint"].split()[0]
    raw = copy.deepcopy(rec["config"])
    with redirect_stdout(io.StringIO()):
        repaired = _structural_validate(copy.deepcopy(rec["config"]), schema)
        rp = ra.analyze(repaired, csv_size_bytes=int(schema["row_count"] * 140), schema=schema)
    infeasible += not rp.get("feasible", True)
    allocs = [{"type": a["stage_type"], "num_workers": a["workers"], "diu": a["diu"]}
              for a in rp.get("allocations", [])]
    for key, stages in (("model_raw", raw["stages"]), ("planner_repaired", repaired["stages"]),
                        ("resource_agent", allocs)):
        o = over_limits_plan(stages)
        acc[key]["over"] += o
        bs = acc[key]["by_size"].setdefault(size, [0, 0])
        bs[0] += 1
        bs[1] += o
        w, d = settings(stages)
        acc[key]["w"] += w
        acc[key]["d"] += d
for key, v in acc.items():
    R[key] = {"plans_over_limits": v["over"],
              "over_by_size": {k: f"{x[1]}/{x[0]}" for k, x in v["by_size"].items()},
              "mean_workers_per_notebook_stage": round(st.mean(v["w"]), 2), "max_workers": max(v["w"]),
              "mean_diu_per_copy_stage": round(st.mean(v["d"]), 2), "max_diu": max(v["d"])}
R["resource_agent"]["plans_flagged_infeasible"] = infeasible
out["R"] = R

# ── A: Assurance structural gate (fault injection through the real layers) ──
# Layers in run order (central_manager_agent/manager.py execute_run):
#   1. Manager Phase-1 validation   — real CentralManager.validate_plan
#   2. Assurance structural gate    — real AssuranceAgent, run_semantic=False
#   3. Executor pre-cloud checks    — same checks as executor._execute_pipeline
#      before any cloud call: plan_safety_issues, containers_to_create, copy
#      dataset refs, compute container refs, and building every notebook.
# A plan that passes all checks it meets reaches Databricks. What happens
# there is classified from the code: the executor ignores stages whose type
# it does not know, and the notebook builder drops aggregation ops it does
# not support — both run "successfully" with wrong output. Anything else that
# the gate would have rejected (e.g. an unknown column) is expected to fail
# inside the Spark job; that last step was not executed.
from central_manager_agent.manager import CentralManager, RunState

def phase1_blocks(cfg):
    st_ = RunState(run_id="offline-eval", plan=copy.deepcopy(cfg))
    with redirect_stdout(io.StringIO()):
        return not CentralManager().validate_plan(st_)["ok"]

def gate_blocks(cfg, schema):
    return agent.assure("", cfg, schema, run_semantic=False).overall_status == "fail"

def executor_blocks(cfg):
    if plan_safety_issues(cfg) or not cfg.get("containers_to_create"):
        return True
    stages = cfg.get("stages", [])
    for s in stages:
        if s.get("type") == "copy" and (not s.get("source_dataset") or not s.get("sink_dataset")):
            return True
        if s.get("type") in ("notebook", "stream") and (not s.get("source_container") or not s.get("sink_container")):
            return True
    try:
        for s in stages:
            if s.get("type") in ("notebook", "stream"):
                build_notebook_source(s, "acct")
    except Exception:
        return True
    return False

def cloud_outcome(cfg):
    stages = cfg.get("stages", [])
    if any(s.get("type") not in ("copy", "notebook", "stream") for s in stages):
        return "runs, stage silently skipped"
    for s in stages:
        for a in ((s.get("aggregation") or {}).get("aggregations") or []):
            if str(a.get("op", "")).lower() not in _AGG_FUNCS:
                return "runs, aggregation silently dropped"
    return "fails inside Spark (expected, not executed)"

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

def first_notebook(cfg):
    return next(s for s in cfg["stages"] if s["type"] == "notebook")

FAULTS = {"unknown column": f_unknown_col, "unsupported aggregation": f_bad_agg,
          "stage order inverted": f_order, "missing required key": f_missing_key,
          "unsafe container name": f_bad_name, "code-injection filter": f_injection,
          "unknown stage type": f_bad_type}

agent = AssuranceAgent()
# fault-inject only into plans the gate accepts when clean (cast plans are
# falsely rejected — reported separately as a gate false-positive rate)
clean_pool = [r for r in rows if not gate_blocks(r["config"], r["schema"])]
cast_false_rejects = len(rows) - len(clean_pool)
base = random.sample(clean_pool, 200)
A = {}
for fname, fn in FAULTS.items():
    c = {"n": len(base), "without_gate": {}, "with_gate": {}}
    for rec in base:
        cfg = copy.deepcopy(rec["config"])
        cfg["_c0"] = rec["schema"]["columns"][1]
        fn(cfg)
        cfg.pop("_c0")
        p1, g, ex = phase1_blocks(cfg), gate_blocks(cfg, rec["schema"]), executor_blocks(cfg)
        # without the gate: Phase-1 -> executor
        wo = ("stopped: manager validation" if p1 else
              "stopped: executor pre-cloud check" if ex else cloud_outcome(cfg))
        # with the gate: Phase-1 -> gate -> executor
        wi = ("stopped: manager validation" if p1 else
              "stopped: assurance gate" if g else
              "stopped: executor pre-cloud check" if ex else cloud_outcome(cfg))
        c["without_gate"][wo] = c["without_gate"].get(wo, 0) + 1
        c["with_gate"][wi] = c["with_gate"].get(wi, 0) + 1
    A[fname] = c
out["A"] = {"faults": A, "gate_false_rejects_on_valid_plans": f"{cast_false_rejects}/{len(rows)}"}

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
