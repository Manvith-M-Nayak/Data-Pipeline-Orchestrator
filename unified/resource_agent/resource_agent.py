"""
Resource Agent — owns RESOURCE MANAGEMENT for the pipeline.

Given a plan + its data, it recommends the compute SETTINGS every stage should
run with and guarantees they fit the student-tier hard limits. It deliberately
does NOT own runtime / outcome prediction — that is the Performance
Prediction Agent's job (see docs/RESPONSIBILITIES.md). The per-stage `duration_s`
fields here are an INTERNAL sizing aid only (used to compare allocation options
and to drive mid-run reallocation); they are never the plan's authoritative
runtime.

Sizing is ML-first with a transparent heuristic fallback (mirrors the Planner
and Performance agents):
  * Primary  : models/resource_models.pkl — multi-target HistGradientBoosting
               trained on 500k synthetic-but-real-telemetry-grounded stages
               (see training/ and ml/). Recommends workers / DIU / peak memory /
               shuffle partitions / node type per stage.
  * Fallback : the heuristic in predict_stage() / right_size(), used whenever the
               model bundle is missing or inference fails.

Responsibilities (all as one cohesive class):

  1.  predict_stage()          → ML-recommended (or heuristic) settings per stage
  2.  estimate_stage_duration()→ INTERNAL sizing estimate (not the plan runtime)
  3.  check_feasibility()      → validates recommendations fit hard limits
  4.  propose_allocations()    → concrete worker/DIU/node/shuffle per stage
  5.  right_size()             → finalizes one stage (ML authoritative when present)
  6.  resolve_contention()     → serializes parallel groups that exceed limits
  7.  dynamic_reallocate()     → mid-run adjustment from Monitor data
  8.  enforce_constraints()    → final hard-cap pass before returning plan
  9.  record_feedback() /
      get_correction_factor()  → prediction self-correction via JSONL log

Integration:
  - Central Manager calls analyze() in Phase 2 (pre_checks).
  - Manager passes resource_plan into RunState for UI display.
  - The Performance Prediction Agent consumes these settings to forecast runtime.
  - After execution, Manager calls record_actual() for the learning loop.
  - Monitor Agent data fed into dynamic_reallocate() mid-run.

Student-tier hard limits (Azure free / trial):
  MAX_WORKERS      = 4    Databricks workers beyond driver
  MAX_DIU          = 8    ADF data-integration units
  MAX_CONCURRENT   = 3    max parallel stages in one group
  MAX_TOTAL_MEM_GB = 64   sum of all workers in any parallel group
"""

import math
import os
import threading
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_FEEDBACK_LOG = os.path.join(_DATA_DIR, "resource_feedback.jsonl")
# Serializes concurrent feedback appends so records from parallel runs don't interleave.
_FEEDBACK_LOCK = threading.Lock()

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

# A size band is used for the duration correction only when it already has
# this many completed runs. Otherwise the factor falls back to every size.
_MIN_BAND_RUNS = 3


def _size_band(size_mb: float) -> str:
    """Same cuts as ml/feature_spec.size_hint_to_ord: <5, <50, else."""
    if size_mb < 5:
        return "small"
    if size_mb < 50:
        return "medium"
    return "large"


def _startup_floor_s(stage_type: str) -> float:
    """Cold-start constant _scale_duration used to paste back onto a shortened estimate."""
    if stage_type == "copy":
        return float(ADF_STARTUP_S)
    return float(DBX_COLD_START_S + DBX_PIP_INSTALL_S)


# ── Data classes ─────────────────────────────────────────────────────────────
@dataclass
class StageRequirements:
    stage_name:        str
    stage_type:        str          # "copy" | "notebook"
    estimated_cpu:     float        # total vCPUs needed
    estimated_mem_gb:  float        # total GB needed across all workers
    estimated_workers: int          # Databricks workers (0 = driver-only), clamped to MAX_WORKERS
    estimated_diu:     int          # ADF DIU (copy stages only), clamped to MAX_DIU
    estimated_duration_s: int       # INTERNAL sizing aid only — NOT the plan's
                                    # runtime. Runtime is owned by the
                                    # Performance Prediction Agent (see docs/RESPONSIBILITIES.md).
    confidence:        float        # 0–1 based on data richness
    rationale:         str          # human-readable reasoning
    # Raw amounts the plan *requested* before clamping to hard limits.
    # Lets check_feasibility surface a clamp instead of swallowing it silently.
    requested_workers: int = 0
    requested_diu:     int = 0
    # Extra settings the ML recommender proposes (Databricks stages).
    recommended_node:    str = DEFAULT_NODE
    recommended_shuffle: int = 8
    # True when the ML model (not the heuristic) sized this stage.
    ml_sized: bool = False


@dataclass
class StageAllocation:
    stage_name:    str
    stage_type:    str
    workers:       int
    diu:           int
    memory_gb:     float
    cpu:           float
    duration_s:    int              # INTERNAL sizing/reallocation input only —
                                    # the authoritative runtime is the Performance
                                    # Prediction Agent's predicted_total_s.
    right_sized:   bool             # True if shrunk from raw prediction
    contention_adjusted: bool       # True if moved due to group conflict
    # ── Recommended compute settings (the Resource Agent's real output) ──
    shuffle_partitions: int = 8
    node_type:          str = DEFAULT_NODE
    ml_sized:           bool = False


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
        stage_index: int = 0,
        n_stages: int = 1,
    ) -> StageRequirements:
        """
        Translate a stage definition into concrete resource requirements.

        Primary path : the trained ML recommender (models/resource_models.pkl)
                       proposes workers / DIU / peak memory / shuffle / node.
        Fallback path: the transparent heuristic below, used when the model is
                       missing or inference fails (mirrors the Planner/Perf agents).

        `correction_factor` (from historical feedback, function 9) scales the
        internal sizing estimate. It is applied here and must survive the
        later worker / DIU rescale.
        """
        name  = stage.get("name", "unknown")
        stype = stage.get("type", "notebook")
        mb    = csv_size_bytes / (1024 * 1024) if csv_size_bytes else 0.0
        schema = schema or {}

        if stype == "copy":
            req = self._predict_copy(name, stage, mb, correction_factor)
        else:
            req = self._predict_notebook(name, stage, mb, schema, correction_factor)

        # ── ML override (settings only) ──────────────────────────────────────
        self._apply_ml_recommendation(req, stage, schema, csv_size_bytes, stage_index, n_stages)
        return req

    def _apply_ml_recommendation(
        self, req: "StageRequirements", stage: dict, schema: dict,
        csv_size_bytes: int, stage_index: int, n_stages: int,
    ) -> None:
        """
        Overlay the ML recommender's compute settings onto a heuristic
        requirement in place. Silently no-ops (leaving the heuristic values) if
        the model can't be loaded — so the agent always produces an answer.
        """
        try:
            from .ml_predictor import ResourceMLPredictor, MLNotAvailable
        except Exception:
            return
        try:
            rec = ResourceMLPredictor.predict_settings(
                stage, schema, csv_size_bytes, stage_index, n_stages
            )
        except MLNotAvailable:
            return
        except Exception as exc:   # never let inference crash the pre-checks
            print(f"[ResourceAgent] ML recommend failed (non-fatal): {exc}")
            return

        if req.stage_type == "copy":
            req.estimated_diu = rec["diu"]
            req.estimated_cpu = float(rec["diu"])
            req.estimated_mem_gb = rec["memory_gb"]
        else:
            req.estimated_workers = rec["workers"]
            req.estimated_mem_gb = rec["memory_gb"]
            spec = NODE_SPECS.get(rec["node_type"], NODE_SPECS[DEFAULT_NODE])
            req.estimated_cpu = float(spec["cpu"] * max(rec["workers"], 1))
            req.recommended_node = rec["node_type"]
            req.recommended_shuffle = rec["shuffle_partitions"]
        req.ml_sized = True
        req.rationale = f"ML recommender ({rec['source']}): " + req.rationale

    def _predict_copy(
        self, name: str, stage: dict, mb: float, cf: float
    ) -> StageRequirements:
        requested_diu = int(stage.get("diu", 4))
        diu  = min(requested_diu, MAX_DIU)
        raw_s = ADF_STARTUP_S + max(20, int(mb / max(diu * ADF_MB_PER_DIU_PER_S, 0.1)))
        dur_s = max(10, int(round(raw_s * cf)))
        cpu   = float(diu)        # ADF DIU ≈ 1 vCPU each
        mem   = diu * 1.5         # ~1.5 GB per DIU for shuffle buffers

        conf  = 0.85 if mb > 0 else 0.5
        clamp_note = f" (clamped from requested {requested_diu})" if requested_diu > diu else ""
        return StageRequirements(
            stage_name=name, stage_type="copy",
            estimated_cpu=cpu, estimated_mem_gb=round(mem, 2),
            estimated_workers=0, estimated_diu=diu,
            estimated_duration_s=dur_s, confidence=conf,
            rationale=(
                f"ADF copy: {diu} DIU{clamp_note} × {ADF_MB_PER_DIU_PER_S} MB/s, "
                f"{mb:.1f} MB input → ~{dur_s}s"
                + (f" × {cf:.2f} learned" if abs(cf - 1.0) > 0.001 else "")
            ),
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
        # Planner emits aggregation={group_by, aggregations}; older callers use
        # aggregations={agg_exprs}. The shared counter accepts both.
        from .ml.feature_spec import stage_agg_count

        agg_count     = stage_agg_count(stage)
        transform_count = len(transforms)

        # Duration components
        startup_s    = DBX_COLD_START_S + DBX_PIP_INSTALL_S
        data_load_s  = max(5, int(rows / DBX_ROWS_PER_S)) if rows else max(5, int(mb / 2))
        transform_s  = transform_count * DBX_TRANSFORM_S + agg_count * DBX_AGG_S
        filter_s     = 5 if has_filter else 0
        write_s      = max(10, int(rows / DBX_ROWS_PER_S)) if rows else 15

        raw_s  = startup_s + data_load_s + transform_s + filter_s + write_s
        dur_s  = max(10, int(round(raw_s * cf)))

        # Memory: driver (~4 GB overhead) + workers
        cpu    = spec["cpu"] * max(workers, 1)
        mem_gb = 4.0 + workers * spec["memory_gb"]

        conf = 0.75 if rows > 0 else 0.55
        rationale = (
            f"Databricks notebook: {workers}w × {spec['memory_gb']}GB, "
            f"{transform_count} transforms, {agg_count} aggs, "
            f"~{rows} rows → startup {startup_s}s + data {data_load_s}s "
            f"+ ops {transform_s+filter_s}s + write {write_s}s = {dur_s}s"
            + (f" × {cf:.2f} learned" if abs(cf - 1.0) > 0.001 else "")
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

        Only the variable portion of the run scales with parallelism. The
        startup floor is the part of *this* estimate that does not shrink
        with more workers or DIU. It must not be a fresh 120s (or 30s) pasted
        on top: once the correction factor had shortened a notebook below the
        cold-start constant, that paste pinned every notebook at 120s and the
        learned factor stopped affecting the duration the rest of the
        pipeline uses.
        """
        if stype == "copy":
            nominal = float(ADF_STARTUP_S)
        else:
            nominal = float(DBX_COLD_START_S + DBX_PIP_INSTALL_S)

        floor = min(nominal, float(base_s))
        variable = max(0.0, float(base_s) - floor)
        bu = max(base_units, 1)          # driver-only / 0-DIU → treat as 1 unit
        nu = max(new_units, 1)
        scaled = floor + variable * (bu / nu)
        return max(10, int(round(scaled)))

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
        Function 5 — turn a requirement into a concrete allocation.

        When the ML recommender sized the stage, its numbers are AUTHORITATIVE:
        they already reflect a demand-driven right-sizing, so we only cap them at
        the hard limits (the Planner's recommended_settings is just a hint that
        the Resource Agent may override — see docs/RESPONSIBILITIES.md).

        Fallback (heuristic) path: cap at recommended_settings + hard limits and
        apply a size-based driver-only / reduce-by-one shrink.
        """
        if r.stage_type == "copy":
            if r.ml_sized:
                alloc_diu = min(r.estimated_diu, MAX_DIU)      # ML is authoritative
            else:
                alloc_diu = min(r.estimated_diu, rec_diu, MAX_DIU)
            return StageAllocation(
                stage_name=r.stage_name, stage_type="copy",
                workers=0, diu=alloc_diu,
                memory_gb=round(alloc_diu * 1.5, 2), cpu=float(alloc_diu),
                duration_s=self.estimate_stage_duration(r, diu=alloc_diu),
                right_sized=alloc_diu < r.estimated_diu, contention_adjusted=False,
                shuffle_partitions=8, node_type=DEFAULT_NODE, ml_sized=r.ml_sized,
            )

        # ── notebook ────────────────────────────────────────────────────────
        raw_w = r.estimated_workers
        if r.ml_sized:
            alloc_w = min(raw_w, MAX_WORKERS)                  # ML is authoritative
            node    = r.recommended_node
            shuffle = r.recommended_shuffle
        else:
            alloc_w = raw_w
            if r.estimated_duration_s < 120:
                alloc_w = 0                    # driver-only — no need for workers
            elif r.estimated_duration_s < 300 and raw_w > 0:
                alloc_w = max(0, raw_w - 1)    # reduce by one
            alloc_w = min(alloc_w, rec_workers, MAX_WORKERS)
            shuffle = 8

        spec   = NODE_SPECS.get(node, NODE_SPECS[DEFAULT_NODE])
        # Prefer the ML-recommended peak memory; else derive from the node.
        mem_gb = r.estimated_mem_gb if r.ml_sized else round(4.0 + alloc_w * spec["memory_gb"], 2)
        cpu    = float(spec["cpu"] * max(alloc_w, 1))
        return StageAllocation(
            stage_name=r.stage_name, stage_type="notebook",
            workers=alloc_w, diu=0,
            memory_gb=round(mem_gb, 2), cpu=cpu,
            duration_s=self.estimate_stage_duration(r, workers=alloc_w),
            right_sized=alloc_w < raw_w, contention_adjusted=False,
            shuffle_partitions=shuffle, node_type=node, ml_sized=r.ml_sized,
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

        for group in execution_groups:
            combined = list(group)
            overflow: List[str] = []

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
                        shuffle_partitions=a.shuffle_partitions, node_type=a.node_type,
                        ml_sized=a.ml_sized,
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
                        shuffle_partitions=a.shuffle_partitions, node_type=a.node_type,
                        ml_sized=a.ml_sized,
                    )
                new_groups.append(keep)
            else:
                new_groups.append(combined)

            # A spilled stage runs in its own group right after this one — not
            # merged into the next group, whose stages may depend on it.
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
                shuffle_partitions=a.shuffle_partitions, node_type=a.node_type,
                ml_sized=a.ml_sized,
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
                                shuffle_partitions=a.shuffle_partitions, node_type=a.node_type,
                                ml_sized=a.ml_sized,
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

    # A few odd runs must not shrink a prediction below a third of the
    # formula, or stretch it past 3x.
    CORRECTION_BOUNDS = (0.33, 3.0)

    @staticmethod
    def _manager_feedback_index() -> Tuple[set, Dict[str, float]]:
        """Failed run ids, and file size (MB) by run_id, from the manager log.

        Failed runs' per-stage "actuals" are abort time or retry backoff.
        Size is joined from here because older resource rows did not store it.
        """
        from jsonl_log import read_jsonl

        try:
            recs = read_jsonl(os.path.join(_DATA_DIR, "manager_feedback.jsonl"))
        except Exception:
            return set(), {}
        failed = {r.get("run_id") for r in recs if r.get("final_status") == "failed"}
        sizes: Dict[str, float] = {}
        for r in recs:
            rid = r.get("run_id")
            mb = r.get("file_size_mb")
            if rid and mb:
                sizes[rid] = float(mb)
        return failed, sizes

    @staticmethod
    def _stored_raw_s(row: dict) -> float:
        raw = row.get("raw_predicted_duration_s")
        if raw:
            return float(raw)
        pred = row.get("predicted_duration_s") or 0
        cf = row.get("correction_factor") or 1.0
        return float(pred / cf) if cf else float(pred)

    @classmethod
    def _floor_pinned(cls, row: dict) -> bool:
        """True when the recorded duration was snapped back to the cold-start floor.

        The fingerprint is a prediction sitting on that floor while the
        stored raw is floor/factor — several times the real formula. Those
        rows are not usable as raw ratios.
        """
        cf = row.get("correction_factor") or 1.0
        pred = row.get("predicted_duration_s") or 0
        if cf >= 0.999 or pred <= 0:
            return False
        floor = _startup_floor_s(row.get("stage_type") or "notebook")
        return pred <= floor + 0.5 and cls._stored_raw_s(row) > floor * 1.5

    def _run_ratios(self, stage_type: str) -> List[Tuple[Optional[float], float]]:
        """One (size_mb, actual/raw) per completed run, in log order.

        Stages that share a run_id are one observation: the manager splits
        a single execution time across them by predicted share, so counting
        each stage would weigh a wide plan more than a narrow one. Rows
        with no run_id (direct feedback calls) each stand alone.

        A duration that was pinned to the cold-start floor does not carry
        the formula's raw value (it was stored as floor/factor). When this
        stage type has at least two real raws, that pinned row uses their
        median instead, so the old rows still teach the factor.
        """
        failed, sizes = self._manager_feedback_index()
        rows = [
            r for r in self._load_feedback(stage_type)
            if r.get("run_id") not in failed and r.get("success") is not False
        ]
        clean_raws = [
            self._stored_raw_s(r) for r in rows
            if not self._floor_pinned(r) and self._stored_raw_s(r) > 0
        ]
        typical_raw = _median(clean_raws) if len(clean_raws) >= 2 else None

        groups: Dict[str, List[dict]] = {}
        order: List[str] = []
        for i, r in enumerate(rows):
            rid = r.get("run_id") or ""
            key = rid if rid else f"__row_{i}"
            if key not in groups:
                order.append(key)
                groups[key] = []
            groups[key].append(r)

        out: List[Tuple[Optional[float], float]] = []
        for key in order:
            stages = groups[key]
            actual_s = 0.0
            raw_s = 0.0
            size_mb = None
            for r in stages:
                actual = r.get("actual_duration_s") or 0
                raw = self._stored_raw_s(r)
                if self._floor_pinned(r) and typical_raw and raw > 1.6 * typical_raw:
                    raw = typical_raw
                if actual > 0 and raw > 0:
                    actual_s += actual
                    raw_s += raw
                if size_mb is None and r.get("size_mb"):
                    size_mb = float(r["size_mb"])
            if raw_s <= 0 or actual_s <= 0:
                continue
            if size_mb is None:
                rid = stages[0].get("run_id") or ""
                if rid and rid in sizes:
                    size_mb = sizes[rid]
            out.append((size_mb, actual_s / raw_s))
        return out

    def get_correction_factor(self, stage_type: str, size_mb: Optional[float] = None) -> float:
        """
        Multiplier for the raw heuristic duration, learned from feedback.
        1.0  = predictions are accurate.
        >1.0 = predictions were consistently too short (actual > predicted).
        <1.0 = predictions were consistently too long.

        The ratio is actual / raw prediction. The median of the last 10 runs
        is used directly — a fixed damping factor would never converge.
        When `size_mb` is given and that size band has at least 3 runs, only
        those runs are used. Small files and larger files do not share one
        factor. Fewer than 3 runs in the band, or no size, uses every run.
        """
        points = self._run_ratios(stage_type)
        if size_mb:
            band = _size_band(size_mb)
            banded = [ratio for sz, ratio in points if sz is not None and _size_band(sz) == band]
            if len(banded) >= _MIN_BAND_RUNS:
                points = [(size_mb, ratio) for ratio in banded]
        ratios = [ratio for _, ratio in points]
        if len(ratios) < _MIN_BAND_RUNS:
            return 1.0
        recent = sorted(ratios[-10:])
        lo, hi = self.CORRECTION_BOUNDS
        return round(min(hi, max(lo, _median(recent))), 3)

    def record_actual(
        self,
        stage_name: str,
        stage_type: str,
        predicted_duration_s: float,
        actual_duration_s: float,
        predicted_workers: int,
        actual_workers: int,
        run_id: str = "",
        correction_factor: float = 1.0,
        size_mb: Optional[float] = None,
    ):
        """Record actual vs predicted for this stage (function 9).

        correction_factor: the multiplier already applied to
        predicted_duration_s, so learning can recover the raw prediction.
        size_mb: input file size, so a later factor can be chosen by size
        band without joining the manager log.
        Only call this for runs that actually executed successfully."""
        try:
            os.makedirs(_DATA_DIR, exist_ok=True)
            cf = correction_factor or 1.0
            record = {
                "ts":                   _ts(),
                "run_id":               run_id,
                "stage_name":           stage_name,
                "stage_type":           stage_type,
                "predicted_duration_s": round(predicted_duration_s, 1),
                "raw_predicted_duration_s": round(predicted_duration_s / cf, 1),
                "correction_factor":    cf,
                "actual_duration_s":    round(actual_duration_s, 1),
                "ratio":                round(actual_duration_s / max(predicted_duration_s, 1), 3),
                "predicted_workers":    predicted_workers,
                "actual_workers":       actual_workers,
            }
            if size_mb and size_mb > 0:
                record["size_mb"] = round(float(size_mb), 6)
            from jsonl_log import append_jsonl

            with _FEEDBACK_LOCK:
                append_jsonl(_FEEDBACK_LOG, record)
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
                # the factor actually applied (failed runs excluded, raw ratios)
                "correction_factor": self.get_correction_factor(stype),
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

        # 9 — Load correction factors before predicting. Size bands keep a
        # small file from inheriting the factor learned on a much larger one.
        size_mb = (csv_size_bytes / (1024 * 1024)) if csv_size_bytes else None
        corr_copy     = self.get_correction_factor("copy", size_mb)
        corr_notebook = self.get_correction_factor("notebook", size_mb)
        correction_factors = {"copy": corr_copy, "notebook": corr_notebook}
        band = _size_band(size_mb) if size_mb else "all"

        # Log the Resource Agent's own self-correction state. Unlike the
        # Learning & Policy Update Agent's duration_correction_factor /
        # cost_correction_factor (which persist an old→new value across
        # cycles), get_correction_factor() recomputes fresh from
        # resource_feedback.jsonl on every call — so there's no "old value"
        # to report here, only the current factor and how much evidence
        # backs it. Printed every analyze() call to mirror the Manager's
        # PERF PREDICT / COST OPTIMIZATION log style.
        copy_n = len(self._load_feedback("copy"))
        notebook_n = len(self._load_feedback("notebook"))
        size_note = f"{size_mb:.2f} MB {band}" if size_mb else "size unknown"
        print(
            f"[Learning Agent] Resource Agent — "
            f"copy factor={corr_copy:.3f} ({copy_n} record(s)) · "
            f"notebook factor={corr_notebook:.3f} ({notebook_n} record(s)) · "
            f"{size_note}"
        )

        # 1 + 2 — Predict per stage (ML recommender first, heuristic fallback)
        n_stages = len(stages)
        requirements: List[StageRequirements] = []
        for i, s in enumerate(stages):
            cf = corr_copy if s.get("type") == "copy" else corr_notebook
            requirements.append(
                self.predict_stage(s, csv_size_bytes, schema, cf, stage_index=i, n_stages=n_stages)
            )

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
        out = _serialize(plan_out)
        # Surface which sizing path ran so the Manager/UI can label it (mirrors
        # the Performance agent's prediction_source). Not a dataclass field to
        # keep the ResourcePlan contract backward-compatible.
        out["sizing_source"] = "ml_model" if any(a.ml_sized for a in allocations) else "heuristic"
        out["correction_size_band"] = band
        return out


# ── Helpers ───────────────────────────────────────────────────────────────────
def _median(values: List[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _ts() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        "sizing_source": "none",
    }


def _serialize(plan: ResourcePlan) -> dict:
    d = asdict(plan)
    return d


def _load_feedback_raw() -> List[dict]:
    from jsonl_log import read_jsonl

    try:
        return read_jsonl(_FEEDBACK_LOG)
    except Exception as exc:
        print(f"[ResourceAgent] feedback read failed: {exc}")
        return []