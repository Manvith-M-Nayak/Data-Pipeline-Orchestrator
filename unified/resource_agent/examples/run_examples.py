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
    _hr("C. right_size() heuristic fallback — short runs collapse to driver-only")
    # Force the heuristic path: build a requirement by hand (no ML overlay) so
    # this section validates the fallback shrink logic regardless of whether the
    # trained model bundle is present.
    from resource_agent.resource_agent import StageRequirements
    big = StageRequirements(
        stage_name="tiny", stage_type="notebook",
        estimated_cpu=16.0, estimated_mem_gb=68.0, estimated_workers=4,
        estimated_diu=0, estimated_duration_s=90, confidence=0.6,
        rationale="hand-built heuristic requirement", requested_workers=4,
        requested_diu=0, ml_sized=False,
    )
    alloc = agent.right_size(big, rec_workers=3, rec_diu=8)
    print(f"  requirement {big.estimated_workers}w / {big.estimated_duration_s}s  "
          f"-> allocated {alloc.workers}w  right_sized={alloc.right_sized}")
    _assert(alloc.workers == 0, "a sub-2-minute notebook is right-sized to driver-only")
    _assert(alloc.right_sized, "right_sized flag is set (shrunk from 4 workers)")


def section_ml(agent):
    _hr("C2. ML recommender — settings scale with workload")
    from resource_agent.ml_predictor import ResourceMLPredictor
    if not ResourceMLPredictor.is_available():
        print("  [SKIP] no model bundle present (heuristic fallback active) — "
              "run training/generate_resource_dataset.py + train_resource_model.py")
        return
    small = agent.predict_stage(
        {"name": "s", "type": "notebook", "transformations": ["x=1"]},
        csv_size_bytes=20_000 * 140, schema={"row_count": 20_000, "columns": list("abc")},
        stage_index=1, n_stages=3)
    heavy = agent.predict_stage(
        {"name": "h", "type": "notebook", "transformations": ["x=1"],
         "aggregations": {"group_by": ["g"], "agg_exprs": ["sum(a)", "avg(b)"]}},
        csv_size_bytes=40_000_000 * 140,
        schema={"row_count": 40_000_000, "columns": list("abcdefgh")},
        stage_index=2, n_stages=3)
    print(f"  small notebook -> {small.estimated_workers}w (ml_sized={small.ml_sized})")
    print(f"  heavy  agg     -> {heavy.estimated_workers}w node={heavy.recommended_node} "
          f"shuffle={heavy.recommended_shuffle} (ml_sized={heavy.ml_sized})")
    _assert(small.ml_sized and heavy.ml_sized, "ML recommender sized both stages")
    _assert(heavy.estimated_workers >= small.estimated_workers,
            "a heavy aggregation gets at least as many workers as a small stage")


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


def _temp_agent():
    """ResourceAgent whose feedback log is a fresh temp file."""
    tmp = tempfile.mkdtemp(prefix="resource_demo_")
    ra._DATA_DIR = tmp
    ra._FEEDBACK_LOG = os.path.join(tmp, "resource_feedback.jsonl")
    return ResourceAgent()


def section_feedback():
    _hr("F. Feedback loop — one ratio per run, by file size, and it actually shortens the estimate")
    # Redirect the feedback log to a temp file so the demo never touches real data.
    orig_dir, orig_log = ra._DATA_DIR, ra._FEEDBACK_LOG
    try:
        agent = _temp_agent()
        # Five notebook calls with no run_id: each stands alone. They took 1.6x.
        for i in range(5):
            agent.record_actual(f"nb{i}", "notebook", predicted_duration_s=100,
                                actual_duration_s=160, predicted_workers=2, actual_workers=2)
        cf = agent.get_correction_factor("notebook")
        report = agent.get_accuracy_report()
        print(f"  5 calls @ 1.6x -> correction_factor={cf}  "
              f"accuracy={report['by_type']['notebook']['accuracy_pct']}%")
        _assert(abs(cf - 1.6) < 1e-6, "correction factor is the median raw ratio 1.6, not a damped halfway step")
        _assert(report["total_records"] == 5, "accuracy report counts every recorded run")

        # Stages that share a run_id are one execution split by the manager,
        # so they must count once. Two stages at 2.0x plus two runs at 1.0x
        # -> median 1.0. Counting the duplicated stage would pull it to 1.5.
        agent = _temp_agent()
        agent.record_actual("a", "notebook", 100, 200, 1, 1, run_id="run-a")
        agent.record_actual("b", "notebook", 100, 200, 1, 1, run_id="run-a")
        agent.record_actual("c", "notebook", 100, 100, 1, 1, run_id="run-b")
        agent.record_actual("d", "notebook", 100, 100, 1, 1, run_id="run-c")
        collapsed = agent.get_correction_factor("notebook")
        print(f"  one run recorded twice + two 1.0x runs -> {collapsed}")
        _assert(abs(collapsed - 1.0) < 1e-6, "stages that share a run_id count as one ratio")

        # Small files and larger files keep separate factors once each band
        # has 3 runs. An unseen size falls back to the overall median.
        agent = _temp_agent()
        for i in range(4):
            agent.record_actual(f"s{i}", "notebook", 100, 40, 1, 1, run_id=f"small-{i}", size_mb=1.0)
            agent.record_actual(f"m{i}", "notebook", 100, 80, 1, 1, run_id=f"med-{i}", size_mb=20.0)
        small = agent.get_correction_factor("notebook", size_mb=0.5)
        medium = agent.get_correction_factor("notebook", size_mb=24.0)
        huge = agent.get_correction_factor("notebook", size_mb=200.0)
        print(f"  size bands -> small={small} medium={medium} unseen={huge}")
        _assert(abs(small - 0.4) < 1e-6, "files under 5 MB use the small-file factor")
        _assert(abs(medium - 0.8) < 1e-6, "files from 5 to 50 MB use the medium-file factor")
        _assert(abs(huge - 0.6) < 1e-6, "a size band with no history falls back to the overall median")

        # Predictions that were snapped to the 120s cold start stored
        # raw = 120/factor, which is not the formula. They must not drag the
        # factor down onto the 0.33 floor when real raws are known.
        agent = _temp_agent()
        for i in range(3):
            agent.record_actual(f"clean{i}", "notebook", 140, 70, 1, 1,
                                run_id=f"clean-{i}", correction_factor=1.0)
        for i in range(5):
            agent.record_actual(f"pin{i}", "notebook", 120, 60, 1, 1,
                                run_id=f"pin-{i}", correction_factor=0.33)
        pinned = agent.get_correction_factor("notebook")
        print(f"  3 real raws + 5 cold-start snaps -> {pinned}")
        _assert(abs(pinned - 0.429) < 0.01, "snapped rows are read against the real raw, not floor/factor")

        # The learned factor has to change the duration that gets allocated.
        # A 0.4 factor on a ~2 minute notebook used to come back out as 120s.
        bare = {"name": "nb", "type": "notebook"}
        req = agent.predict_stage(
            bare, csv_size_bytes=1024, schema={"row_count": 100}, correction_factor=0.4,
        )
        alloc = agent.right_size(req, rec_workers=1, rec_diu=4)
        print(f"  notebook × 0.4 -> requirement {req.estimated_duration_s}s, "
              f"allocation {alloc.duration_s}s")
        _assert(req.estimated_duration_s < 100, "the requirement itself is shortened")
        _assert(alloc.duration_s < 100, "worker rescale does not restore the 120s cold start")
        _assert(alloc.duration_s > 20, "the correction does not wipe the estimate")
    finally:
        ra._DATA_DIR, ra._FEEDBACK_LOG = orig_dir, orig_log


def main():
    agent = ResourceAgent()
    section_analyze(agent)
    section_duration(agent)
    section_right_size(agent)
    section_ml(agent)
    section_enforce(agent)
    section_reallocate(agent)
    section_feedback()
    print("=" * 72)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
