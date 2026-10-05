"""More offline results for docs/PAPER_RESULTS.md (no Azure, no LLM, no training).

Every section drives the real agent code. Nothing under data/ or the agents'
state files is written: simulations use temporary policy/feedback files, and
history is only read.

Sections (PAPER_EVAL_ONLY=a,b to pick):
  retry_replay   — what automatic retries achieved on the logged failed runs
  sla_replay     — a fixed time limit vs the learned "usual duration"
                   (PerformancePredictionAgent._expected_duration) on simulated
                   pipelines of different sizes with injected slow runs
  realloc_replay — what ResourceAgent.dynamic_reallocate would recommend on
                   the real completed live runs (it is not called during runs)
  learning_sim   — the Learning & Policy agent's real normalize -> analyze ->
                   evaluate_and_apply loop on simulated runs with a known bias:
                   convergence by learning rate, a regime change, and aborted
                   runs mixed in

    python scripts/paper_eval/offline_more.py /tmp/offline_more.json
"""
import copy
import datetime as dt
import glob
import io
import json
import math
import os
import random
import statistics as st
import sys
import tempfile
from contextlib import redirect_stdout
from unittest import mock

_UNIFIED = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CALLER_CWD = os.getcwd()
sys.path.insert(0, _UNIFIED)
os.chdir(_UNIFIED)

REPORT = {}


def _read_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


# ── retry replay ─────────────────────────────────────────────────────────────
def retry_replay():
    """Logged runs that used automatic retries, and how each ended."""
    from central_manager_agent.manager import CentralManager

    runs = _read_jsonl("data/manager_feedback.jsonl")
    retried = [r for r in runs if (r.get("retries") or 0) > 0]
    backoff = sum(CentralManager.RETRY_BACKOFF_S[: CentralManager.MAX_RETRIES])
    return {
        "managed_runs": len(runs),
        "runs_with_retries": len(retried),
        "retried_runs_that_succeeded": sum(
            1 for r in retried if r.get("final_status") in ("feedback", "completed")),
        "retried_runs": [{"ts": r["ts"][:16], "retries": r["retries"],
                          "final_status": r["final_status"],
                          "logged_duration_s": r.get("actual_duration_s")} for r in retried],
        "backoff_per_fully_retried_run_s": backoff,
        "max_retries": CentralManager.MAX_RETRIES,
    }


# ── fixed limit vs learned usual duration ────────────────────────────────────
def sla_replay(seed=7, runs_per_pipeline=30, slow_every=10, fixed_limit_s=900):
    """Pipelines of very different sizes; every `slow_every`-th run is a
    genuine 2x slowdown. Compare a fixed limit with the learned p95 rule.

    The learned rule is the real _expected_duration: each run is judged
    against the pipeline's own previous completed runs (min 3, last 20, input
    size 0.5-2x). A run is flagged when its duration exceeds the learned
    expected duration — the same comparison the agent makes for a prediction.
    """
    from performance_prediction_agent.performance_agent import PerformancePredictionAgent

    rng = random.Random(seed)
    pipelines = {  # size_mb -> typical duration (s) on this kind of serverless run
        "tiny_1MB": (1, 95),
        "medium_50MB": (50, 140),
        "large_500MB": (500, 420),
        "xlarge_2GB": (2000, 1150),
    }
    agent = PerformancePredictionAgent()
    out = {"fixed_limit_s": fixed_limit_s, "pipelines": {}}
    tot = {"fixed": [0, 0, 0], "learned": [0, 0, 0]}  # tp, fp, fn
    for name, (size_mb, typical) in pipelines.items():
        history = []
        counts = {"fixed": [0, 0, 0], "learned": [0, 0, 0], "slow_runs": 0, "judged": 0}
        for i in range(runs_per_pipeline):
            slow = (i + 1) % slow_every == 0
            dur = typical * rng.uniform(0.9, 1.1) * (2.0 if slow else 1.0)
            with mock.patch.object(agent, "_load_feedback", return_value=list(history)), \
                 mock.patch.object(PerformancePredictionAgent, "_pipeline_key",
                                   staticmethod(lambda plan: plan.get("key"))):
                exp = agent._expected_duration({"key": name}, {"file_size_mb": size_mb}, dur)
            learned_flag = exp["slower_than_usual"]
            fixed_flag = dur > fixed_limit_s
            if exp["expected_duration_s"] is not None:   # learned rule can judge
                counts["judged"] += 1
                counts["slow_runs"] += slow
                for rule, flag in (("fixed", fixed_flag), ("learned", learned_flag)):
                    c = counts[rule]
                    if flag and slow:
                        c[0] += 1
                    elif flag and not slow:
                        c[1] += 1
                    elif slow:
                        c[2] += 1
            # a run enters the history only if it is a normal completed run;
            # slow runs are real too, so they are kept (realistic, not cleaned)
            history.append({"pipeline_key": name, "final_status": "completed", "executed": True,
                            "file_size_mb": size_mb, "actual_duration_s": dur})
        for rule in ("fixed", "learned"):
            for k in range(3):
                tot[rule][k] += counts[rule][k]
        out["pipelines"][name] = {
            "typical_s": typical, "judged_runs": counts["judged"], "slow_runs": counts["slow_runs"],
            **{f"{rule}_tp_fp_fn": counts[rule] for rule in ("fixed", "learned")},
        }

    def pr(t):
        tp, fp, fn = t
        return {"tp": tp, "fp": fp, "fn": fn,
                "precision_pct": round(100 * tp / (tp + fp), 1) if tp + fp else None,
                "recall_pct": round(100 * tp / (tp + fn), 1) if tp + fn else None}
    out["fixed_limit"] = pr(tot["fixed"])
    out["learned_usual"] = pr(tot["learned"])
    out["note"] = ("runs judged only once the learned rule has 3 prior runs; slow = 2x typical, "
                   f"every {slow_every}th run; durations ±10% noise; seed {seed}")
    return out


# ── dynamic reallocation on the real live runs ───────────────────────────────
def realloc_replay():
    from resource_agent.resource_agent import ResourceAgent, StageAllocation

    agent = ResourceAgent()
    actions = {}
    rows = []
    for f in sorted(glob.glob("data/paper_eval/live*/states/*.json")):
        d = json.load(open(f))
        if d.get("status") != "completed" or not d.get("execution_s"):
            continue
        rp = d.get("resource_plan") or {}
        allocs = []
        for a in rp.get("allocations", []):
            fields = {k: a.get(k) for k in StageAllocation.__dataclass_fields__ if k in a}
            allocs.append(StageAllocation(**fields))
        if not allocs:
            continue
        est_total = sum(a.duration_s for a in allocs) or 1
        # Only the run total is measured; split it across stages by predicted
        # share, the same way the manager records stage feedback.
        live = [{"pipelineName": a.stage_name,
                 "elapsedSec": d["execution_s"] * a.duration_s / est_total,
                 "status": "Succeeded"} for a in allocs]
        with redirect_stdout(io.StringIO()):
            recs = agent.dynamic_reallocate(live, allocs, elapsed_s=d["execution_s"])
        for r in recs:
            actions[r["action"]] = actions.get(r["action"], 0) + 1
        rows.append({"run": d["run_id"][:8], "execution_s": d["execution_s"],
                     "resource_estimate_s": est_total,
                     "actions": [r["action"] for r in recs]})
    return {"completed_live_runs": len(rows), "stage_recommendations": actions,
            "wired_into_runs": False,
            "note": "dynamic_reallocate is only reachable via POST /api/resource/reallocate; "
                    "the Manager never calls it during a run",
            "runs": rows}


# ── learning loop simulation ─────────────────────────────────────────────────
def _sim_learning(true_dur, true_cost, n_runs, seed, lr=None, abort_every=0,
                  shift_at=None, shift_dur=None, shift_cost=None, cycle_every=5):
    """Simulated managed runs through the real Learning & Policy code.

    Each run: raw ML prediction ~100 s, raw cost estimate ~$0.05; the Manager
    applies the current factors; actual = raw × true ratio × noise. Every
    `cycle_every` runs one learning cycle runs (normalize → analyze →
    evaluate_and_apply, which includes the rollback review). Aborted runs
    (final_status failed, ~0.1 s) are mixed in every `abort_every` runs.
    """
    from learning_policy_agent.error_analyzer import ErrorAnalyzer
    from learning_policy_agent.feedback_collector import FeedbackCollector
    from learning_policy_agent import policy_engine as pe
    from learning_policy_agent.safety import SafetyManager

    rng = random.Random(seed)
    tmp = tempfile.mkdtemp(prefix="learning_sim_")
    engine = pe.PolicyEngine(policy_path=os.path.join(tmp, "policies.json"),
                             log_path=os.path.join(tmp, "learning_log.jsonl"),
                             safety=SafetyManager(os.path.join(tmp, "versions")))
    if lr is not None:
        p = engine.load()
        p["learning_rate"] = lr
        p["cost_learning_rate"] = lr
        engine._save(p)
    clock = [1_800_000_000.0]
    raws, trace, events = [], [], []
    for i in range(n_runs):
        clock[0] += 600
        pol = engine.load()
        fd, fc = pol["duration_correction_factor"], pol["cost_correction_factor"]
        td, tc = true_dur, true_cost
        if shift_at is not None and i >= shift_at:
            td, tc = shift_dur, shift_cost
        raw_d = 100 * rng.uniform(0.8, 1.2)
        raw_c = 0.05 * rng.uniform(0.8, 1.2)
        # Record time and the policy engine's clock must be the same timeline,
        # or the rollback review never sees a run "after" a change.
        ts = dt.datetime.fromtimestamp(clock[0], dt.timezone.utc).isoformat().replace("+00:00", "Z")
        aborted = abort_every and (i + 1) % abort_every == 0
        if aborted:
            rec = {"ts": ts, "run_id": f"sim-{i}", "final_status": "failed", "executed": False,
                   "actual_duration_s": 0.1, "perf_predicted_total_s": raw_d * fd,
                   "perf_uncorrected_total_s": raw_d, "prediction_source": "ml_model",
                   "cost_estimate_usd": raw_c * 0.2, "actual_cost_usd": 0.001,
                   "stage_count": 2, "complexity": "low"}
        else:
            act_d = raw_d * td * math.exp(rng.gauss(0, 0.1))
            act_c = raw_c * tc * math.exp(rng.gauss(0, 0.1))
            rec = {"ts": ts, "run_id": f"sim-{i}", "final_status": "completed", "executed": True,
                   "actual_duration_s": act_d, "perf_predicted_total_s": raw_d * fd,
                   "perf_uncorrected_total_s": raw_d, "prediction_source": "ml_model",
                   "estimated_cost_usd": raw_c * fc, "cost_uncorrected_estimated_usd": raw_c,
                   "actual_cost_usd": act_c, "stage_count": 2, "complexity": "low"}
            trace.append({"run": i, "duration_ape": abs(raw_d * fd - act_d) / act_d,
                          "cost_ape": abs(raw_c * fc - act_c) / act_c,
                          "duration_factor": fd, "cost_factor": fc})
        raws.append(rec)
        if (i + 1) % cycle_every == 0:
            records = [FeedbackCollector.normalize(r) for r in raws]
            metrics = ErrorAnalyzer().analyze(records)
            with mock.patch.object(pe.time, "time", lambda: clock[0]), \
                 redirect_stdout(io.StringIO()):
                rep = engine.evaluate_and_apply({**metrics, "_records": records})
            for c in rep.get("changes", []):
                events.append({"run": i, "type": "update", "policy": c.get("policy"),
                               "old": c.get("old"), "new": c.get("new")})
            for e in rep.get("review_events", []):
                events.append({"run": i, "type": e.get("action"), "policy": e.get("policy")})
    final = engine.load()

    def mape(key, rows):
        return round(100 * st.mean(r[key] for r in rows), 1) if rows else None
    first, last = trace[:10], trace[-10:]
    return {
        "true_duration_ratio": true_dur, "true_cost_ratio": true_cost,
        "final_duration_factor": final["duration_correction_factor"],
        "final_cost_factor": final["cost_correction_factor"],
        "duration_mape_first10_pct": mape("duration_ape", first),
        "duration_mape_last10_pct": mape("duration_ape", last),
        "cost_mape_first10_pct": mape("cost_ape", first),
        "cost_mape_last10_pct": mape("cost_ape", last),
        "updates": sum(1 for e in events if e["type"] == "update"),
        "confirmed": sum(1 for e in events if e["type"] == "confirmed"),
        "rolled_back": sum(1 for e in events if e["type"] == "rolled_back"),
        "events": events,
        "factor_trace": [(t["run"], round(t["duration_factor"], 4), round(t["cost_factor"], 4))
                         for t in trace[::5]],
    }


def learning_sim():
    out = {"setup": "true ratio 0.6 (system over-estimates by 1/0.6), noise sd 10%, "
                    "1 learning cycle per 5 runs, evidence gate 10 runs, review after 5"}
    out["by_learning_rate"] = {
        str(lr): {k: v for k, v in _sim_learning(0.6, 0.6, 80, seed=1, lr=lr).items()
                  if k not in ("events", "factor_trace")}
        for lr in (0.1, 0.3, 0.5)}
    out["production_lr_0.3_trace"] = _sim_learning(0.6, 0.6, 80, seed=1)["factor_trace"]
    out["regime_change"] = {k: v for k, v in _sim_learning(
        0.6, 0.6, 120, seed=2, shift_at=60, shift_dur=1.2, shift_cost=1.2).items()
        if k != "factor_trace"}
    out["with_aborts_every_4th_run"] = {k: v for k, v in _sim_learning(
        0.6, 0.6, 80, seed=3, abort_every=4).items() if k != "factor_trace"}
    return out


SECTIONS = {"retry_replay": retry_replay, "sla_replay": sla_replay,
            "realloc_replay": realloc_replay, "learning_sim": learning_sim}

if __name__ == "__main__":
    only = {s for s in os.environ.get("PAPER_EVAL_ONLY", "").split(",") if s}
    for name, fn in SECTIONS.items():
        if only and name not in only:
            continue
        print(f"== {name}", flush=True)
        REPORT[name] = fn()
    dest = os.path.join(_CALLER_CWD, sys.argv[1] if len(sys.argv) > 1 else "offline_more.json")
    with open(dest, "w") as f:
        json.dump(REPORT, f, indent=1, default=str)
    print("wrote", dest)
