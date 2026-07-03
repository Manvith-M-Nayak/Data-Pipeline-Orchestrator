"""
Exercises every Resource Agent responsibility end-to-end and self-verifies the
result against the student-tier hard limits.

    python -m resource_agent.examples.run_examples

No Azure / network access needed — the agent is pure Python. Each section prints
what it produced and asserts the invariants that must always hold; the script
exits non-zero if any invariant is violated.
"""

import os
import sys
import tempfile

# Windows consoles default to cp1252 and choke on any non-latin glyph the agent
# may emit (e.g. the "x" multiplier in a rationale). Prefer UTF-8 when possible.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from ..resource_agent import (
    ResourceAgent,
    StageAllocation,
    MAX_WORKERS,
    MAX_DIU,
    MAX_CONCURRENT,
    MAX_TOTAL_MEM_GB,
)
from .. import resource_agent as ra


# A demo plan the Planner might emit: one ADF copy plus four Databricks
# notebooks. `ingest` over-requests DIU and `clean` over-requests workers so we
# can watch the agent clamp them; the three silver-tier notebooks share a
# parallel group that busts the combined-worker limit.
DEMO_PLAN = {
    "num_containers": 4,
    "containers_to_create": ["bronze", "silver", "gold", "gold_features"],
    "recommended_settings": {"num_workers": 3, "node_type": "Standard_D4s_v3", "diu": 8, "shuffle_partitions": 8},
    "execution_order": ["ingest", "clean", "enrich", "features", "aggregate"],
    "stages": [
        {"name": "ingest", "type": "copy",
         "source_dataset": "DS_Raw", "sink_dataset": "DS_Bronze", "diu": 12},
        {"name": "clean", "type": "notebook",
         "source_container": "bronze", "sink_container": "silver",
         "num_workers": 6, "transformations": ["a", "b", "c", "d", "e", "f", "g", "h"],
         "filter_condition": "amount > 0"},
        {"name": "enrich", "type": "notebook",
         "source_container": "bronze", "sink_container": "silver_enriched",
         "num_workers": 3, "transformations": ["a", "b", "c", "d", "e", "f"]},
        {"name": "features", "type": "notebook",
         "source_container": "bronze", "sink_container": "gold_features",
         "num_workers": 3, "transformations": ["a", "b", "c", "d", "e"]},
        {"name": "aggregate", "type": "notebook",
         "source_container": "silver", "sink_container": "gold",
         "num_workers": 3, "aggregations": {"agg_exprs": ["sum(amount)", "avg(amount)"]}},
    ],
}
DEMO_SCHEMA = {"row_count": 5_000_000}
# clean/enrich/features all read `bronze` -> they run as one parallel group.
DEMO_GROUPS = [["ingest"], ["clean", "enrich", "features"], ["aggregate"]]


def _hr(title):
    print("=" * 72)
    print(title)
    print("-" * 72)


def _assert(cond, msg):
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {msg}")
    if not cond:
        raise AssertionError(msg)


def section_analyze(agent):
    _hr("A. Full analyze() pipeline — predict -> feasibility -> right-size -> contention -> enforce")
    rp = agent.analyze(DEMO_PLAN, csv_size_bytes=40 * 1024 * 1024,
                       schema=DEMO_SCHEMA, execution_groups=DEMO_GROUPS)

    print(f"  feasible={rp['feasible']}  total_workers={rp['total_workers']}  "
          f"peak_concurrent={rp['peak_concurrent_workers']}  est_total={rp['estimated_total_s']}s")
    for a in rp["allocations"]:
        flags = []
        if a["right_sized"]:
            flags.append("right-sized")
        if a["contention_adjusted"]:
            flags.append("contention-adjusted")
        unit = f"{a['workers']}w" if a["stage_type"] == "notebook" else f"{a['diu']} DIU"
        print(f"    - {a['stage_name']:<10} {a['stage_type']:<9} {unit:<7} "
              f"{a['memory_gb']:>6} GB  ~{a['duration_s']}s  {','.join(flags)}")
    print(f"  execution_groups: {rp['execution_groups']}")
    for w in rp["warnings"]:
        print(f"    warn: {w}")

    # Invariants that must hold for every emitted plan.
    _assert(rp["feasible"], "plan is feasible")
    _assert(all(a["workers"] <= MAX_WORKERS for a in rp["allocations"]),
            f"no stage exceeds {MAX_WORKERS} workers")
    _assert(all(a["diu"] <= MAX_DIU for a in rp["allocations"]),
            f"no stage exceeds {MAX_DIU} DIU")

    amap = {a["stage_name"]: a for a in rp["allocations"]}
    for g in rp["execution_groups"]:
        _assert(len(g) <= MAX_CONCURRENT, f"group {g} within {MAX_CONCURRENT} concurrent stages")
        gw = sum(amap[n]["workers"] for n in g if n in amap)
        gm = sum(amap[n]["memory_gb"] for n in g if n in amap)
        _assert(gw <= MAX_WORKERS, f"group {g} combined workers {gw} within {MAX_WORKERS}")
        _assert(gm <= MAX_TOTAL_MEM_GB, f"group {g} combined memory {gm:.1f} within {MAX_TOTAL_MEM_GB}")

    _assert(any("requested 12 DIU" in w for w in rp["warnings"]),
            "over-requested DIU surfaced as a clamp warning")
    _assert(any("requested 6 workers" in w for w in rp["warnings"]),
            "over-requested workers surfaced as a clamp warning")


def section_duration(agent):
    _hr("B. estimate_stage_duration() — fixed cold-start floor does not parallelize")
    reqs = {r.stage_name: r for r in
            [agent.predict_stage(s, 40 * 1024 * 1024, DEMO_SCHEMA) for s in DEMO_PLAN["stages"]]}
    clean = reqs["clean"]
    at_base = agent.estimate_stage_duration(clean, workers=clean.estimated_workers)
    at_half = agent.estimate_stage_duration(clean, workers=max(clean.estimated_workers // 2, 1))
    at_more = agent.estimate_stage_duration(clean, workers=clean.estimated_workers + 2)
    print(f"  clean @ {clean.estimated_workers}w = {at_base}s   "
          f"@ fewer workers = {at_half}s   @ more workers = {at_more}s")
    _assert(at_base == clean.estimated_duration_s, "baseline allocation reproduces predicted duration")
    _assert(at_half > at_base, "fewer workers -> longer")
    _assert(at_more < at_base, "more workers -> shorter")
    _assert(at_half < at_base * 2, "scaling holds the cold-start floor constant (not linear)")


def section_right_size(agent):
    _hr("C. right_size() — short runs collapse to driver-only")
    short = agent.predict_stage(
        {"name": "tiny", "type": "notebook", "source_container": "a",
         "sink_container": "b", "num_workers": 4},
        csv_size_bytes=0, schema={"row_count": 100})
    alloc = agent.right_size(short, rec_workers=3, rec_diu=8)
    print(f"  tiny predicted {short.estimated_workers}w / {short.estimated_duration_s}s  "
          f"-> allocated {alloc.workers}w  right_sized={alloc.right_sized}")
    _assert(alloc.workers == 0, "a sub-2-minute notebook is right-sized to driver-only")
    _assert(alloc.right_sized, "right_sized flag is set")


def section_enforce(agent):
    _hr("D. enforce_constraints() — an oversized parallel group is split")
    # Five 2-worker notebooks placed in a single group: 10 workers, 5 stages —
    # both bust the caps and must be split into sequential sub-groups.
    allocs = [
        StageAllocation(stage_name=f"s{i}", stage_type="notebook", workers=2, diu=0,
                        memory_gb=36.0, cpu=8.0, duration_s=200,
                        right_sized=False, contention_adjusted=False)
        for i in range(5)
    ]
    group = [[f"s{i}" for i in range(5)]]
    allocs, groups, notes = agent.enforce_constraints(allocs, group)
    print(f"  1 group of 5 -> {len(groups)} sub-group(s): {groups}")
    for n in notes:
        print(f"    note: {n}")
    amap = {a.stage_name: a for a in allocs}
    for g in groups:
        gw = sum(amap[n].workers for n in g)
        gm = sum(amap[n].memory_gb for n in g)
        _assert(len(g) <= MAX_CONCURRENT and gw <= MAX_WORKERS and gm <= MAX_TOTAL_MEM_GB,
                f"sub-group {g} respects all hard limits (workers={gw}, mem={gm:.0f})")
    _assert(len(groups) > 1, "the oversized group was split")


def section_reallocate(agent):
    _hr("E. dynamic_reallocate() — reacts to live Monitor data")
    allocs = [StageAllocation(stage_name="clean", stage_type="notebook", workers=2, diu=0,
                              memory_gb=36.0, cpu=8.0, duration_s=200,
                              right_sized=False, contention_adjusted=False)]
    live = [{"pipelineName": "clean", "status": "InProgress", "elapsedSec": 700, "anomaly": ""}]
    recs = agent.dynamic_reallocate(live, allocs, elapsed_s=700)
    print(f"  clean elapsed 700s vs predicted 200s -> {recs[0]['action']} "
          f"(->{recs[0]['recommended_workers']}w): {recs[0]['reason']}")
    _assert(recs[0]["action"] == "scale_up", "a run 3.5x over prediction recommends scale_up")
    _assert(recs[0]["recommended_workers"] == 3, "scale_up bumps workers by one within the cap")


def section_feedback():
    _hr("F. Feedback loop — record_actual() drives a damped correction factor")
    # Redirect the feedback log to a temp file so the demo never touches real data.
    tmp = tempfile.mkdtemp(prefix="resource_demo_")
    orig_dir, orig_log = ra._DATA_DIR, ra._FEEDBACK_LOG
    ra._DATA_DIR = tmp
    ra._FEEDBACK_LOG = os.path.join(tmp, "resource_feedback.jsonl")
    try:
        agent = ResourceAgent()
        # Five notebook runs that each took 1.6x the predicted time.
        for i in range(5):
            agent.record_actual(f"nb{i}", "notebook", predicted_duration_s=100,
                                actual_duration_s=160, predicted_workers=2, actual_workers=2)
        cf = agent.get_correction_factor("notebook")
        report = agent.get_accuracy_report()
        print(f"  5 runs @ 1.6x -> correction_factor={cf}  "
              f"accuracy={report['by_type']['notebook']['accuracy_pct']}%")
        # mean ratio 1.6, damped 50% -> 1.0 + 0.6*0.5 = 1.3
        _assert(abs(cf - 1.3) < 1e-6, "correction factor is damped halfway toward the observed 1.6x")
        _assert(report["total_records"] == 5, "accuracy report counts every recorded run")
    finally:
        ra._DATA_DIR, ra._FEEDBACK_LOG = orig_dir, orig_log


def main():
    agent = ResourceAgent()
    section_analyze(agent)
    section_duration(agent)
    section_right_size(agent)
    section_enforce(agent)
    section_reallocate(agent)
    section_feedback()
    print("=" * 72)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
