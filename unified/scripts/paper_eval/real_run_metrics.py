"""Recompute every real-run number in docs/PAPER_RESULTS.md from the logs.

Read-only. Sources: data/manager_feedback.jsonl, data/resource_feedback.jsonl,
data/adf_monitor.db (demo rows excluded), data/ollama.log.

    python scripts/paper_eval/real_run_metrics.py
"""
import json
import os
import re
import sqlite3
import statistics as st

UNIFIED = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA = os.path.join(UNIFIED, "data")


def load(name):
    with open(os.path.join(DATA, name)) as f:
        return [json.loads(line) for line in f if line.strip()]


def errors(pairs, label):
    """APE = |predicted - actual| / actual over (predicted, actual) pairs."""
    pairs = [(p, a) for p, a in pairs if p and a]
    if not pairs:
        print(f"{label}: no data")
        return
    ape = [abs(p - a) / a for p, a in pairs]
    bias = [(p - a) / a for p, a in pairs]
    print(f"{label}: n={len(pairs)} MAPE={100 * st.mean(ape):.1f}% "
          f"medianAPE={100 * st.median(ape):.1f}% bias={100 * st.mean(bias):+.1f}% "
          f"within20%={100 * sum(x <= .2 for x in ape) / len(ape):.0f}% "
          f"within50%={100 * sum(x <= .5 for x in ape) / len(ape):.0f}%")


def main():
    runs = load("manager_feedback.jsonl")
    ok = [r for r in runs if r["final_status"] in ("feedback", "completed")]
    print(f"== Managed runs: {len(runs)} total, {len(ok)} succeeded "
          f"({100 * len(ok) / len(runs):.1f}%), planner fallbacks "
          f"{sum(1 for r in runs if r.get('used_fallback'))}")

    print("\n== Runtime prediction vs measured execution time (successful runs)")
    errors([(r["predicted_duration_s"], r["actual_duration_s"]) for r in ok],
           "Resource heuristic")
    for src in ("formula", "ml_model"):
        errors([(r.get("perf_predicted_total_s"), r["actual_duration_s"])
                for r in ok if r.get("prediction_source") == src], f"Performance {src}")

    print("\n== Cost estimate with vs without the learned correction (paired)")
    paired = [r for r in ok if r.get("cost_uncorrected_estimated_usd")]
    errors([(r["cost_uncorrected_estimated_usd"], r["actual_cost_usd"]) for r in paired],
           "without learning (raw)")
    errors([(r["estimated_cost_usd"], r["actual_cost_usd"]) for r in paired],
           "with learning (corrected)")

    print("\n== Resource per-stage duration: raw vs self-corrected (paired)")
    stages = load("resource_feedback.jsonl")
    for t in ("copy", "notebook"):
        rows = [r for r in stages if r["stage_type"] == t and r.get("raw_predicted_duration_s")]
        errors([(r["raw_predicted_duration_s"], r["actual_duration_s"]) for r in rows],
               f"{t} without correction")
        errors([(r["predicted_duration_s"], r["actual_duration_s"]) for r in rows],
               f"{t} with correction")

    print("\n== Real durations by stage count")
    for k in sorted({r["stage_count"] for r in ok}):
        d = [r["actual_duration_s"] for r in ok if r["stage_count"] == k]
        print(f"  {k} stage(s): n={len(d)} mean={st.mean(d):.1f}s sd={st.pstdev(d):.1f}s")

    print("\n== Monitor history (demo rows excluded)")
    db = sqlite3.connect(os.path.join(DATA, "adf_monitor.db"))
    for row in db.execute(
        "SELECT pipeline_name, status, COUNT(*), ROUND(AVG(duration_ms)/1000.0, 1) "
        "FROM pipeline_runs WHERE run_id NOT LIKE 'demo%' GROUP BY 1, 2"
    ):
        print("  ", row)

    print("\n== Local LLM latency (all /api/chat calls in ollama.log)")
    secs = []
    with open(os.path.join(DATA, "ollama.log"), errors="ignore") as f:
        for line in f:
            m = re.search(r'\|\s*200\s*\|\s*([\d.]+)(ms|s)\s*\|.*POST\s+"/api/chat"', line)
            if m:
                secs.append(float(m.group(1)) / (1000 if m.group(2) == "ms" else 1))
    if secs:
        s = sorted(secs)
        print(f"  calls={len(s)} median={st.median(s):.1f}s mean={st.mean(s):.1f}s "
              f"p90={s[int(.9 * len(s)) - 1]:.1f}s max={max(s):.1f}s")


if __name__ == "__main__":
    main()
