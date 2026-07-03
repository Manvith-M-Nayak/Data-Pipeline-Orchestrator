"""
Resource Agent

All nine responsibilities implemented as one cohesive class:

  1.  predict_stage()          → CPU, memory, worker, DIU estimates
  2.  estimate_stage_duration()→ wall-clock time at given allocation
  3.  check_feasibility()      → validates plan fits hard limits
  4.  propose_allocations()    → concrete worker/DIU assignment per stage
  5.  right_size()             → shrinks over-allocated stages
  6.  resolve_contention()     → serializes parallel groups that exceed limits
  7.  dynamic_reallocate()     → mid-run adjustment from Monitor data
  8.  enforce_constraints()    → final hard-cap pass before returning plan
  9.  record_feedback() /
      get_correction_factor()  → prediction self-correction via JSONL log

Integration:
  - Central Manager calls analyze() in Phase 2 (pre_checks).
  - Manager passes resource_plan into RunState for UI display.
  - After execution, Manager calls record_actual() for learning loop.
  - Monitor Agent data fed into dynamic_reallocate() mid-run.

Student-tier hard limits (Azure free / trial):
  MAX_WORKERS      = 4    Databricks workers beyond driver
  MAX_DIU          = 8    ADF data-integration units
  MAX_CONCURRENT   = 3    max parallel stages in one group
  MAX_TOTAL_MEM_GB = 64   sum of all workers in any parallel group
"""

import json
import math
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_FEEDBACK_LOG = os.path.join(_DATA_DIR, "resource_feedback.jsonl")

# ── Student-tier hard limits ─────────────────────────────────────────────────
MAX_WORKERS      = 4
MAX_DIU          = 8
MAX_CONCURRENT   = 3
MAX_TOTAL_MEM_GB = 64.0

# ── Node catalogue (Azure VM sizes used by Databricks) ──────────────────────
NODE_SPECS: Dict[str, Dict] = {
    "Standard_D4s_v3":  {"cpu": 4,  "memory_gb": 16.0},
    "Standard_D4_v3":   {"cpu": 4,  "memory_gb": 16.0},
    "Standard_DS3_v2":  {"cpu": 4,  "memory_gb": 14.0},
    "Standard_DS4_v2":  {"cpu": 8,  "memory_gb": 28.0},
    "Standard_DS2_v2":  {"cpu": 2,  "memory_gb": 7.0},
    "Standard_D8s_v3":  {"cpu": 8,  "memory_gb": 32.0},
}
DEFAULT_NODE = "Standard_D4s_v3"
DEFAULT_NODE_MEM_GB = NODE_SPECS[DEFAULT_NODE]["memory_gb"]

# ── Throughput constants ─────────────────────────────────────────────────────
ADF_MB_PER_DIU_PER_S   = 5.0    # ADF copy throughput per DIU (rough)
ADF_STARTUP_S          = 30     # ADF pipeline trigger + propagation
DBX_COLD_START_S       = 90     # Databricks serverless cold start + pip install
DBX_PIP_INSTALL_S      = 30     # azure-storage-blob install
DBX_ROWS_PER_S         = 50_000 # rows/s the SDK read/write achieves on student tier
DBX_TRANSFORM_S        = 3      # seconds per PySpark column transformation
DBX_AGG_S              = 10     # seconds per groupBy aggregation


# ── Data classes ─────────────────────────────────────────────────────────────
@dataclass
class StageRequirements:
    stage_name:        str
    stage_type:        str          # "copy" | "notebook"
    estimated_cpu:     float        # total vCPUs needed
    estimated_mem_gb:  float        # total GB needed across all workers
    estimated_workers: int          # Databricks workers (0 = driver-only), clamped to MAX_WORKERS
    estimated_diu:     int          # ADF DIU (copy stages only), clamped to MAX_DIU
    estimated_duration_s: int       # wall-clock seconds at this allocation
    confidence:        float        # 0–1 based on data richness
    rationale:         str          # human-readable reasoning
    # Raw amounts the plan *requested* before clamping to hard limits.
    # Lets check_feasibility surface a clamp instead of swallowing it silently.
    requested_workers: int = 0
    requested_diu:     int = 0


@dataclass
class StageAllocation:
    stage_name:    str
    stage_type:    str
    workers:       int
    diu:           int
    memory_gb:     float
    cpu:           float
    duration_s:    int
    right_sized:   bool             # True if shrunk from raw prediction
    contention_adjusted: bool       # True if moved due to group conflict


@dataclass
class ResourcePlan:
    feasible:            bool
    constraint_violations: List[str]
    warnings:            List[str]
    stage_requirements:  List[StageRequirements]
    allocations:         List[StageAllocation]
    execution_groups:    List[List[str]]          # after contention resolution
    total_workers:       int
    total_memory_gb:     float
    peak_concurrent_workers: int
    estimated_total_s:   int
    correction_factors:  Dict[str, float]


# ── Resource Agent ────────────────────────────────────────────────────────────
class ResourceAgent:

    # ── 1 + 2: Predict requirements + duration for one stage ─────────────────
    def predict_stage(
        self,
        stage: dict,
        csv_size_bytes: int = 0,
        schema: dict = None,
        correction_factor: float = 1.0,
    ) -> StageRequirements:
        """
        Translate a stage definition into concrete resource requirements.
        Uses correction_factor derived from historical feedback (function 9).
        """
        name  = stage.get("name", "unknown")
        stype = stage.get("type", "notebook")
        mb    = csv_size_bytes / (1024 * 1024) if csv_size_bytes else 0.0
        schema = schema or {}

        if stype == "copy":
            return self._predict_copy(name, stage, mb, correction_factor)
        return self._predict_notebook(name, stage, mb, schema, correction_factor)

    def _predict_copy(
        self, name: str, stage: dict, mb: float, cf: float
    ) -> StageRequirements:
        requested_diu = int(stage.get("diu", 4))
        diu  = min(requested_diu, MAX_DIU)
        raw_s = ADF_STARTUP_S + max(20, int(mb / max(diu * ADF_MB_PER_DIU_PER_S, 0.1)))
        dur_s = max(30, int(raw_s * cf))
        cpu   = float(diu)        # ADF DIU ≈ 1 vCPU each
        mem   = diu * 1.5         # ~1.5 GB per DIU for shuffle buffers

        conf  = 0.85 if mb > 0 else 0.5
        clamp_note = f" (clamped from requested {requested_diu})" if requested_diu > diu else ""
        return StageRequirements(
            stage_name=name, stage_type="copy",
            estimated_cpu=cpu, estimated_mem_gb=round(mem, 2),
            estimated_workers=0, estimated_diu=diu,
            estimated_duration_s=dur_s, confidence=conf,
            rationale=f"ADF copy: {diu} DIU{clamp_note} × {ADF_MB_PER_DIU_PER_S} MB/s, {mb:.1f} MB input → ~{dur_s}s",
            requested_workers=0, requested_diu=requested_diu,
        )

    def _predict_notebook(
        self, name: str, stage: dict, mb: float, schema: dict, cf: float
    ) -> StageRequirements:
        requested_workers = int(stage.get("num_workers", 0))
        workers = min(requested_workers, MAX_WORKERS)
        node    = stage.get("node_type", DEFAULT_NODE)
        spec    = NODE_SPECS.get(node, NODE_SPECS[DEFAULT_NODE])

        rows          = int(schema.get("row_count", 0) or 0)
        transforms    = stage.get("transformations", []) or []
        has_filter    = bool(stage.get("filter_condition"))
        aggs          = stage.get("aggregations") or {}
        agg_count     = len(aggs.get("agg_exprs", [])) if isinstance(aggs, dict) else 0
        transform_count = len(transforms)

        # Duration components
        startup_s    = DBX_COLD_START_S + DBX_PIP_INSTALL_S
        data_load_s  = max(5, int(rows / DBX_ROWS_PER_S)) if rows else max(5, int(mb / 2))
        transform_s  = transform_count * DBX_TRANSFORM_S + agg_count * DBX_AGG_S
        filter_s     = 5 if has_filter else 0
        write_s      = max(10, int(rows / DBX_ROWS_PER_S)) if rows else 15

        raw_s  = startup_s + data_load_s + transform_s + filter_s + write_s
        dur_s  = max(60, int(raw_s * cf))

        # Memory: driver (~4 GB overhead) + workers
        cpu    = spec["cpu"] * max(workers, 1)
        mem_gb = 4.0 + workers * spec["memory_gb"]

        conf = 0.75 if rows > 0 else 0.55
        rationale = (
            f"Databricks notebook: {workers}w × {spec['memory_gb']}GB, "
            f"{transform_count} transforms, {agg_count} aggs, "
            f"~{rows} rows → startup {startup_s}s + data {data_load_s}s "
            f"+ ops {transform_s+filter_s}s + write {write_s}s = {dur_s}s"
        )
        return StageRequirements(
            stage_name=name, stage_type="notebook",
            estimated_cpu=cpu, estimated_mem_gb=round(mem_gb, 2),
            estimated_workers=workers, estimated_diu=0,
            estimated_duration_s=dur_s, confidence=conf,
            rationale=rationale,
            requested_workers=requested_workers, requested_diu=0,
        )

    # ── 2: Duration at a given allocation ────────────────────────────────────
    @staticmethod
    def _scale_duration(base_s: float, base_units: int, new_units: int, stype: str) -> int:
        """
        Re-estimate wall-clock time when parallelism changes.

        Only the *variable* portion of the run scales with parallelism — the
        fixed startup / cold-start floor does not (spinning up a cluster or
        triggering an ADF pipeline costs the same regardless of DIU/worker
        count). Scaling the whole duration (as the old inline formulas did)
        over-penalized down-sizing; here the floor is held constant and only
        the work above it scales inversely with parallelism.
        """
        if stype == "copy":
            floor, min_s = float(ADF_STARTUP_S), 30
        else:
            floor, min_s = float(DBX_COLD_START_S + DBX_PIP_INSTALL_S), 60

        variable = max(0.0, base_s - floor)
        bu = max(base_units, 1)          # driver-only / 0-DIU → treat as 1 unit
        nu = max(new_units, 1)
        scaled = floor + variable * (bu / nu)
        return max(min_s, int(round(scaled)))

    def estimate_stage_duration(
        self,
        req: StageRequirements,
        workers: Optional[int] = None,
        diu: Optional[int] = None,
    ) -> int:
        """
        Function 2 — wall-clock seconds for a stage at a *given* allocation.

        Uses the stage's predicted duration (computed at its predicted
        allocation) as the baseline and re-scales it for the requested worker
        (notebook) or DIU (copy) count. Passing the predicted allocation back
        in returns the baseline unchanged.
        """
        if req.stage_type == "copy":
            target = req.estimated_diu if diu is None else diu
            return self._scale_duration(
                req.estimated_duration_s, req.estimated_diu, target, "copy"
            )
        target = req.estimated_workers if workers is None else workers
        return self._scale_duration(
            req.estimated_duration_s, req.estimated_workers, target, "notebook"
        )

    # ── 3: Feasibility ────────────────────────────────────────────────────────
    def check_feasibility(
        self,
        requirements: List[StageRequirements],
        execution_groups: List[List[str]],
    ) -> Tuple[bool, List[str], List[str]]:
        """
        Check whether the raw predictions fit within hard limits.
        Returns (feasible, violations, warnings).
        """
        req_by_name = {r.stage_name: r for r in requirements}
        violations: List[str] = []
        warnings:   List[str] = []

        # Per-stage checks.
        #   Workers/DIU are auto-clamped during prediction, so an over-request is
        #   still runnable — surface it as a warning (the plan asked for more than
        #   the tier allows and we quietly reduced it) rather than a hard failure.
        #   Memory is NOT clamped, so a single stage exceeding the cap is a true
        #   infeasibility that must abort the run.
        for r in requirements:
            if r.stage_type == "copy" and r.requested_diu > MAX_DIU:
                warnings.append(
                    f"Stage '{r.stage_name}': requested {r.requested_diu} DIU > limit "
                    f"{MAX_DIU} — clamped to {r.estimated_diu}"
                )
            if r.stage_type == "notebook" and r.requested_workers > MAX_WORKERS:
                warnings.append(
                    f"Stage '{r.stage_name}': requested {r.requested_workers} workers > limit "
                    f"{MAX_WORKERS} — clamped to {r.estimated_workers}"
                )
            if r.estimated_mem_gb > MAX_TOTAL_MEM_GB:
                violations.append(
                    f"Stage '{r.stage_name}': memory {r.estimated_mem_gb:.1f} GB > limit {MAX_TOTAL_MEM_GB} GB"
                )

        # Per-group (parallel) checks
        for group in execution_groups:
            if len(group) > MAX_CONCURRENT:
                warnings.append(
                    f"Group {group} has {len(group)} parallel stages > "
                    f"recommended max {MAX_CONCURRENT} — will serialize excess"
                )
            group_workers = sum(
                req_by_name[n].estimated_workers for n in group if n in req_by_name
            )
            group_mem = sum(
                req_by_name[n].estimated_mem_gb for n in group if n in req_by_name
            )
            if group_workers > MAX_WORKERS:
                warnings.append(
                    f"Group {group}: combined workers {group_workers} > {MAX_WORKERS} — contention"
                )
            if group_mem > MAX_TOTAL_MEM_GB:
                warnings.append(
                    f"Group {group}: combined memory {group_mem:.1f} GB > {MAX_TOTAL_MEM_GB} GB — contention"
                )

        return len(violations) == 0, violations, warnings

    # ── 5: Right-size one stage ──────────────────────────────────────────────
    def right_size(
        self,
        r: StageRequirements,
        rec_workers: int,
        rec_diu: int,
        node: str = DEFAULT_NODE,
    ) -> StageAllocation:
        """
        Function 5 — shrink a single over-allocated stage.

        Caps the raw prediction at the Planner's recommended_settings and the
        hard limits, applies a driver-only / reduce-by-one heuristic for short
        notebook runs, and re-estimates duration for the smaller allocation.
        """
        if r.stage_type == "copy":
            alloc_diu   = min(r.estimated_diu, rec_diu, MAX_DIU)
            right_sized = alloc_diu < r.estimated_diu
            return StageAllocation(
                stage_name=r.stage_name, stage_type="copy",
                workers=0, diu=alloc_diu,
                memory_gb=round(alloc_diu * 1.5, 2), cpu=float(alloc_diu),
                duration_s=self.estimate_stage_duration(r, diu=alloc_diu),
                right_sized=right_sized, contention_adjusted=False,
            )

        # notebook
        raw_w   = r.estimated_workers
        alloc_w = raw_w
        if r.estimated_duration_s < 120:
            alloc_w = 0                       # driver-only — no need for workers
        elif r.estimated_duration_s < 300 and raw_w > 0:
            alloc_w = max(0, raw_w - 1)       # reduce by one
        alloc_w = min(alloc_w, rec_workers, MAX_WORKERS)
        right_sized = alloc_w < raw_w

        spec   = NODE_SPECS.get(node, NODE_SPECS[DEFAULT_NODE])
        mem_gb = round(4.0 + alloc_w * spec["memory_gb"], 2)
        cpu    = float(spec["cpu"] * max(alloc_w, 1))
        return StageAllocation(
            stage_name=r.stage_name, stage_type="notebook",
            workers=alloc_w, diu=0,
            memory_gb=mem_gb, cpu=cpu,
            duration_s=self.estimate_stage_duration(r, workers=alloc_w),
            right_sized=right_sized, contention_adjusted=False,
        )

    # ── 4 + 5: Allocate + right-size ─────────────────────────────────────────
    def propose_allocations(
        self,
        requirements: List[StageRequirements],
        plan: dict,
    ) -> List[StageAllocation]:
        """
        Translate raw predictions into right-sized concrete allocations.
        Caps at recommended_settings and hard limits.
        """
        rec   = plan.get("recommended_settings", {})
        rec_w = min(int(rec.get("num_workers", 0)), MAX_WORKERS)
        rec_d = min(int(rec.get("diu", 4)), MAX_DIU)
        node  = rec.get("node_type", DEFAULT_NODE)
        return [self.right_size(r, rec_w, rec_d, node) for r in requirements]

    # ── 6: Contention + 8: Constraint enforcement ─────────────────────────────
    def resolve_contention(
        self,
        allocations: List[StageAllocation],
        execution_groups: List[List[str]],
    ) -> Tuple[List[StageAllocation], List[List[str]]]:
        """
        For each parallel group, if combined workers or memory exceeds limits:
          - Try proportional reduction first.
          - If still over limit, serialize the least-critical stage to the next group.
        Returns updated allocations and updated execution_groups.
        """
        alloc_map = {a.stage_name: a for a in allocations}
        new_groups: List[List[str]] = []
        overflow:   List[str]       = []

        for group in execution_groups:
            # Combine any spillover from previous group's overflow
            combined = overflow + group
            overflow = []

            # Check combined workers
            group_workers = sum(
                alloc_map[n].workers for n in combined if n in alloc_map
            )
            group_mem = sum(
                alloc_map[n].memory_gb for n in combined if n in alloc_map
            )

            if (group_workers <= MAX_WORKERS
                    and group_mem <= MAX_TOTAL_MEM_GB
                    and len(combined) <= MAX_CONCURRENT):
                new_groups.append(combined)
                continue

            # Try proportional worker reduction
            notebook_names = [
                n for n in combined
                if n in alloc_map and alloc_map[n].stage_type == "notebook"
            ]
            if group_workers > MAX_WORKERS and notebook_names:
                excess   = group_workers - MAX_WORKERS
                per_stage = math.ceil(excess / max(len(notebook_names), 1))
                for n in notebook_names:
                    a = alloc_map[n]
                    new_w = max(0, a.workers - per_stage)
                    node_spec = NODE_SPECS.get(DEFAULT_NODE)
                    new_mem = round(4.0 + new_w * node_spec["memory_gb"], 2)
                    alloc_map[n] = StageAllocation(
                        stage_name=a.stage_name, stage_type=a.stage_type,
                        workers=new_w, diu=a.diu,
                        memory_gb=new_mem, cpu=float(new_w * node_spec["cpu"]),
                        duration_s=self._scale_duration(a.duration_s, a.workers, new_w, a.stage_type),
                        right_sized=True, contention_adjusted=True,
                    )

                # Re-check after reduction
                group_workers = sum(alloc_map[n].workers for n in combined if n in alloc_map)
                group_mem     = sum(alloc_map[n].memory_gb for n in combined if n in alloc_map)

            # If still over limit, serialize the most resource-hungry stage
            if (group_workers > MAX_WORKERS
                    or group_mem > MAX_TOTAL_MEM_GB
                    or len(combined) > MAX_CONCURRENT):
                # Sort by memory desc; spill the heaviest
                spill = sorted(
                    combined,
                    key=lambda n: alloc_map.get(n, StageAllocation("", "", 0, 0, 0.0, 0.0, 0, False, False)).memory_gb,
                    reverse=True,
                )
                keep   = spill[1:]  # keep all but heaviest
                spilled = spill[0]
                overflow.append(spilled)
                if spilled in alloc_map:
                    a = alloc_map[spilled]
                    alloc_map[spilled] = StageAllocation(
                        stage_name=a.stage_name, stage_type=a.stage_type,
                        workers=a.workers, diu=a.diu,
                        memory_gb=a.memory_gb, cpu=a.cpu,
                        duration_s=a.duration_s, right_sized=a.right_sized,
                        contention_adjusted=True,
                    )
                new_groups.append(keep)
            else:
                new_groups.append(combined)

        if overflow:
            new_groups.append(overflow)

        # Filter empty groups
        new_groups = [g for g in new_groups if g]

        updated_allocs = list(alloc_map.values())
        return updated_allocs, new_groups

    # ── 8: Final constraint enforcement (hard-cap pass) ──────────────────────
    def enforce_constraints(
        self,
        allocations: List[StageAllocation],
        execution_groups: List[List[str]],
    ) -> Tuple[List[StageAllocation], List[List[str]], List[str]]:
        """
        Function 8 — the last gate before the plan is returned.

        Guarantees the emitted plan honors every hard limit no matter what the
        upstream heuristics produced:
          * per-stage workers ≤ MAX_WORKERS, DIU ≤ MAX_DIU (re-scaling duration);
          * every execution group ≤ MAX_CONCURRENT stages AND ≤ MAX_WORKERS
            combined workers AND ≤ MAX_TOTAL_MEM_GB combined memory — oversized
            groups are greedily split into sequential sub-groups.

        Returns (allocations, execution_groups, notes). `notes` describes any
        adjustment made here and is merged into the plan's warnings.
        """
        notes: List[str] = []
        alloc_map = {a.stage_name: a for a in allocations}

        # 1) Per-stage hard caps (belt-and-suspenders behind right_size).
        for name, a in list(alloc_map.items()):
            capped_w = min(a.workers, MAX_WORKERS)
            capped_d = min(a.diu, MAX_DIU)
            if capped_w == a.workers and capped_d == a.diu:
                continue
            spec    = NODE_SPECS.get(DEFAULT_NODE)
            new_mem = round(4.0 + capped_w * spec["memory_gb"], 2) if a.stage_type == "notebook" \
                else round(capped_d * 1.5, 2)
            new_cpu = float(capped_w * spec["cpu"]) if a.stage_type == "notebook" \
                else float(capped_d)
            base_units = a.workers if a.stage_type == "notebook" else a.diu
            new_units  = capped_w if a.stage_type == "notebook" else capped_d
            alloc_map[name] = StageAllocation(
                stage_name=a.stage_name, stage_type=a.stage_type,
                workers=capped_w, diu=capped_d,
                memory_gb=new_mem, cpu=new_cpu,
                duration_s=self._scale_duration(a.duration_s, base_units, new_units, a.stage_type),
                right_sized=True, contention_adjusted=a.contention_adjusted,
            )
            notes.append(
                f"Stage '{name}': hard-capped to {capped_w} workers / {capped_d} DIU"
            )

        # 2) Split any group that exceeds concurrency, worker, or memory limits.
        enforced_groups: List[List[str]] = []
        for group in execution_groups:
            subgroups: List[List[str]] = []
            cur: List[str] = []
            cur_w, cur_m = 0, 0.0
            for name in group:
                a  = alloc_map.get(name)
                w  = a.workers   if a else 0
                m  = a.memory_gb if a else 0.0
                if cur and (
                    len(cur) >= MAX_CONCURRENT
                    or cur_w + w > MAX_WORKERS
                    or cur_m + m > MAX_TOTAL_MEM_GB
                ):
                    subgroups.append(cur)
                    cur, cur_w, cur_m = [], 0, 0.0
                cur.append(name)
                cur_w += w
                cur_m += m
            if cur:
                subgroups.append(cur)

            if len(subgroups) > 1:
                notes.append(
                    f"Group {group} split into {len(subgroups)} sequential sub-group(s) "
                    f"to respect hard limits"
                )
                # Every stage past the first sub-group was moved due to contention.
                for sg in subgroups[1:]:
                    for name in sg:
                        if name in alloc_map and not alloc_map[name].contention_adjusted:
                            a = alloc_map[name]
                            alloc_map[name] = StageAllocation(
                                stage_name=a.stage_name, stage_type=a.stage_type,
                                workers=a.workers, diu=a.diu,
                                memory_gb=a.memory_gb, cpu=a.cpu,
                                duration_s=a.duration_s, right_sized=a.right_sized,
                                contention_adjusted=True,
                            )
            enforced_groups.extend(subgroups)

        enforced_groups = [g for g in enforced_groups if g]
        return list(alloc_map.values()), enforced_groups, notes

    # ── 7: Dynamic re-allocation ──────────────────────────────────────────────
    def dynamic_reallocate(
        self,
        live_runs: List[dict],
        allocations: List[StageAllocation],
        elapsed_s: float,
    ) -> List[dict]:
        """
        React to Monitor data during execution.
        live_runs: list of {pipelineName, status, elapsedSec, anomaly}
        Returns recommendations: [{stage, action, reason}]
        """
        alloc_map = {a.stage_name: a for a in allocations}
        recommendations: List[dict] = []

        for run in live_runs:
            name    = run.get("pipelineName", "")
            elapsed = float(run.get("elapsedSec", elapsed_s))
            anomaly = run.get("anomaly", "")
            alloc   = alloc_map.get(name)

            if not alloc:
                continue

            predicted = alloc.duration_s
            ratio     = elapsed / predicted if predicted > 0 else 1.0

            if ratio > 2.5:
                # Running way over prediction — recommend scale up
                new_workers = min(alloc.workers + 1, MAX_WORKERS)
                recommendations.append({
                    "stage": name,
                    "action": "scale_up",
                    "reason": f"elapsed {elapsed:.0f}s ≈ {ratio:.1f}× predicted {predicted}s",
                    "recommended_workers": new_workers,
                    "recommended_diu": min(alloc.diu + 2, MAX_DIU) if alloc.diu else 0,
                })
            elif ratio < 0.4 and elapsed > 30:
                # Finished much faster than predicted — reclaim resources
                recommendations.append({
                    "stage": name,
                    "action": "reclaim",
                    "reason": f"completed at {ratio:.1f}× prediction — resources can be freed",
                    "recommended_workers": max(0, alloc.workers - 1),
                    "recommended_diu": alloc.diu,
                })
            elif anomaly:
                recommendations.append({
                    "stage": name,
                    "action": "investigate",
                    "reason": f"Monitor anomaly detected: {anomaly}",
                    "recommended_workers": alloc.workers,
                    "recommended_diu": alloc.diu,
                })
            else:
                recommendations.append({
                    "stage": name,
                    "action": "ok",
                    "reason": f"on track at {ratio:.1f}× prediction",
                    "recommended_workers": alloc.workers,
                    "recommended_diu": alloc.diu,
                })

        return recommendations

    # ── 9: Feedback / self-correction ────────────────────────────────────────
    @staticmethod
    def _load_feedback(stage_type: Optional[str]) -> List[dict]:
        """Return recorded feedback rows, optionally filtered by stage_type."""
        return [
            r for r in _load_feedback_raw()
            if stage_type is None or r.get("stage_type") == stage_type
        ]

    def get_correction_factor(self, stage_type: str) -> float:
        """
        Load historical feedback and compute a damped correction multiplier.
        1.0  = predictions are accurate.
        >1.0 = predictions were consistently too short (actual > predicted).
        <1.0 = predictions were consistently too long.
        """
        records = self._load_feedback(stage_type)
        if len(records) < 3:
            return 1.0
        ratios = [
            r["actual_duration_s"] / r["predicted_duration_s"]
            for r in records
            if r.get("predicted_duration_s", 0) > 0
        ]
        if not ratios:
            return 1.0
        recent = ratios[-10:]  # last 10 runs
        avg    = sum(recent) / len(recent)
        # Damped: move 50% toward observed ratio to avoid over-correction
        return round(1.0 + (avg - 1.0) * 0.5, 3)

    def record_actual(
        self,
        stage_name: str,
        stage_type: str,
        predicted_duration_s: float,
        actual_duration_s: float,
        predicted_workers: int,
        actual_workers: int,
        run_id: str = "",
    ):
        """Record actual vs predicted for this stage (function 9)."""
        try:
            os.makedirs(_DATA_DIR, exist_ok=True)
            record = {
                "ts":                   _ts(),
                "run_id":               run_id,
                "stage_name":           stage_name,
                "stage_type":           stage_type,
                "predicted_duration_s": round(predicted_duration_s, 1),
                "actual_duration_s":    round(actual_duration_s, 1),
                "ratio":                round(actual_duration_s / max(predicted_duration_s, 1), 3),
                "predicted_workers":    predicted_workers,
                "actual_workers":       actual_workers,
            }
            with open(_FEEDBACK_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception as exc:
            print(f"[ResourceAgent] feedback write failed (non-fatal): {exc}")

    # Documented name in the responsibility list (function 9) — same behavior.
    record_feedback = record_actual

    def get_accuracy_report(self) -> dict:
        """Summarize prediction accuracy across all recorded runs."""
        all_records = self._load_feedback(None)
        if not all_records:
            return {"total_records": 0, "by_type": {}}

        by_type: Dict[str, list] = {}
        for r in all_records:
            stype = r.get("stage_type", "unknown")
            by_type.setdefault(stype, []).append(r.get("ratio", 1.0))

        summary: Dict[str, dict] = {}
        for stype, ratios in by_type.items():
            mean_ratio = sum(ratios) / len(ratios)
            summary[stype] = {
                "count":            len(ratios),
                "mean_ratio":       round(mean_ratio, 3),
                "correction_factor": round(1.0 + (mean_ratio - 1.0) * 0.5, 3),
                "accuracy_pct":     round(max(0, 100 - abs(mean_ratio - 1.0) * 100), 1),
                "recent_ratios":    [round(r, 3) for r in ratios[-5:]],
            }
        return {"total_records": len(all_records), "by_type": summary}

    # ── Main entry point ──────────────────────────────────────────────────────
    def analyze(
        self,
        plan: dict,
        csv_size_bytes: int = 0,
        schema: dict = None,
        execution_groups: Optional[List[List[str]]] = None,
    ) -> dict:
        """
        Full resource analysis pipeline.
        Called by Central Manager in Phase 2 (pre_checks).

        Returns a serializable dict with all resource decisions.
        """
        stages = plan.get("stages", [])
        if not stages:
            return _empty_plan("No stages in plan")

        schema = schema or {}

        # Build execution groups if not provided (fallback: one sequential chain)
        if execution_groups is None:
            execution_groups = [[s["name"]] for s in stages]

        # 9 — Load correction factors before predicting
        corr_copy     = self.get_correction_factor("copy")
        corr_notebook = self.get_correction_factor("notebook")
        correction_factors = {"copy": corr_copy, "notebook": corr_notebook}

        # 1 + 2 — Predict per stage
        requirements: List[StageRequirements] = []
        for s in stages:
            cf = corr_copy if s.get("type") == "copy" else corr_notebook
            requirements.append(self.predict_stage(s, csv_size_bytes, schema, cf))

        # 3 — Feasibility check
        feasible, violations, warnings = self.check_feasibility(
            requirements, execution_groups
        )

        # 4 + 5 — Right-sized allocations
        allocations = self.propose_allocations(requirements, plan)

        # 6 — Contention resolution across parallel groups
        allocations, execution_groups = self.resolve_contention(
            allocations, execution_groups
        )

        # 8 — Final hard-cap pass: the emitted plan is now guaranteed to honor
        #     every hard limit regardless of upstream heuristics.
        allocations, execution_groups, enforce_notes = self.enforce_constraints(
            allocations, execution_groups
        )
        warnings = warnings + enforce_notes

        # Summary metrics
        total_workers = sum(a.workers for a in allocations)
        total_mem     = sum(a.memory_gb for a in allocations)
        peak_concurrent = max(
            (sum(
                next((a.workers for a in allocations if a.stage_name == n), 0)
                for n in group
            ) for group in execution_groups),
            default=0,
        )
        # Critical-path duration (sum of sequential groups' slowest stage each)
        total_s = sum(
            max(
                (next((a.duration_s for a in allocations if a.stage_name == n), 0)
                 for n in group),
                default=0,
            )
            for group in execution_groups
        )

        plan_out = ResourcePlan(
            feasible=feasible,
            constraint_violations=violations,
            warnings=warnings,
            stage_requirements=requirements,
            allocations=allocations,
            execution_groups=execution_groups,
            total_workers=total_workers,
            total_memory_gb=round(total_mem, 2),
            peak_concurrent_workers=peak_concurrent,
            estimated_total_s=total_s,
            correction_factors=correction_factors,
        )
        return _serialize(plan_out)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _ts() -> str:
    import datetime
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _empty_plan(reason: str) -> dict:
    return {
        "feasible": False,
        "constraint_violations": [reason],
        "warnings": [],
        "stage_requirements": [],
        "allocations": [],
        "execution_groups": [],
        "total_workers": 0,
        "total_memory_gb": 0.0,
        "peak_concurrent_workers": 0,
        "estimated_total_s": 0,
        "correction_factors": {},
    }


def _serialize(plan: ResourcePlan) -> dict:
    d = asdict(plan)
    return d


def _load_feedback_raw() -> List[dict]:
    if not os.path.exists(_FEEDBACK_LOG):
        return []
    records = []
    try:
        with open(_FEEDBACK_LOG) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except Exception:
        pass
    return records
