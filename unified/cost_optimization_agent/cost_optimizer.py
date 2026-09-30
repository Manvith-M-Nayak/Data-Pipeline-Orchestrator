"""
Cost Optimization Agent — Core Logic (ML-first, rule fallback).

Primary path:  trained ML model (cost_models.pkl) predicts cost-optimal
               configuration per stage; suggestions are derived by comparing
               current plan against ML recommendation.
Fallback path: rule-based heuristics used when model is unavailable.

Phases:
  1. Cost Model — formula converting resource-hours to estimated cost
  2. ML Optimization — compare current plan vs ML-recommended config
  3. Constraint Enforcement — safety checks before returning suggestions
  4. Ranking & Explanation — best-value ordering

Design:
  - ML model is multi-target HistGradientBoosting (same as Resource Agent)
  - Training labels come from brute-force cost minimization
  - Rule fallback preserves the original 5 rules from Phase 2
  - Deterministic: same inputs -> same outputs regardless of path taken
"""

import copy
import math
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

NODE_HOURLY_RATES: Dict[str, float] = {
    "Standard_DS2_v2": 0.14,
    "Standard_D4s_v3": 0.28,
    "Standard_D4_v3": 0.28,
    "Standard_DS3_v2": 0.28,
    "Standard_DS4_v2": 0.56,
    "Standard_D8s_v3": 0.56,
}
_DEFAULT_NODE_RATE = 0.28
_DEFAULT_NODE = "Standard_D4s_v3"

COST_MODEL_ASSUMPTIONS = {
    "node_hourly_rates": NODE_HOURLY_RATES,
    "databricks_markup": "~2.5x compute (DBU pricing), baked into DBU rate",
    "dbu_per_worker_per_hour": 1.5,
    "dbu_price_per_unit": 0.55,
    "adf_activity_price": 0.001,
    "adf_diu_hour_estimate": 0.25,
    "storage_gb_per_month": 0.018,
    "note": "All costs are estimates. No real billing data was available at build time.",
}


@dataclass
class CostBreakdown:
    compute_usd: float
    databricks_dbu_usd: float
    adf_usd: float
    storage_usd: float
    total_usd: float
    currency: str = "USD"


@dataclass
class OptimizationSuggestion:
    change: str
    estimated_saving: str
    trade_off: str
    reason: str
    new_cost: CostBreakdown
    risk_level: str
    value_score: float = 0.0
    source: str = "ml"  # "ml" or "rule"


@dataclass
class OptimizationResult:
    estimated_cost: CostBreakdown
    recommendations: List[OptimizationSuggestion]
    chosen_option: Optional[int]
    optimization_source: str = "ml_model"  # "ml_model" or "heuristic"


class CostOptimizationAgent:
    def optimize(
        self,
        plan: dict,
        performance_prediction: dict,
        resource_plan: dict,
        constraints: Optional[dict] = None,
    ) -> dict:
        constraints = constraints or {}

        current_cost = self._estimate_cost(plan, performance_prediction, resource_plan)

        suggestions: List[OptimizationSuggestion] = []

        # ── Primary path: ML model ──────────────────────────────────────
        ml_used = self._try_ml_suggestions(
            plan,
            performance_prediction,
            resource_plan,
            current_cost,
            suggestions,
            constraints,
        )

        # Fail closed: legacy heuristics lack candidate runtime/resource validation.
        # Return the current cost and no recommendation when ML cannot prove safety.

        safe = self._enforce_constraints(
            suggestions, constraints, performance_prediction
        )
        ranked = self._rank_suggestions(safe)

        result = OptimizationResult(
            estimated_cost=current_cost,
            recommendations=ranked,
            chosen_option=0 if ranked else None,
            optimization_source="ml_model" if ml_used else "heuristic",
        )
        return asdict(result)

    # ── Auto-apply: returns a modified resource_plan with best recommendation applied ──

    def apply_optimization(self, plan, performance_prediction, resource_plan, constraints=None):
        """Apply only a cost-saving candidate that passes runtime/resource checks."""
        candidate = self._validated_ml_candidate(plan, performance_prediction, resource_plan, constraints or {})
        return candidate if candidate is not None else copy.deepcopy(resource_plan)

    def _validated_ml_candidate(self, plan, perf, resource_plan, constraints):
        from cost_optimization_agent.ml_predictor import CostMLPredictor, MLNotAvailable
        from cost_optimization_agent.ml.feature_spec import estimate_stage_duration
        from resource_agent.ml.feature_spec import stage_features
        from resource_agent import NODE_SPECS, MAX_WORKERS, MAX_DIU, MAX_TOTAL_MEM_GB
        if not CostMLPredictor.is_available():
            return None
        stages = {s.get("name"): s for s in plan.get("stages", [])}
        rp = copy.deepcopy(resource_plan)
        allocations = rp.get("allocations", [])
        if not allocations:
            return None
        deadline = float(constraints.get("deadline_s", 0) or 0)
        baseline = float(perf.get("predicted_total_s", 0) or rp.get("estimated_total_s", 0) or 0)
        if not math.isfinite(deadline) or not math.isfinite(baseline) or baseline <= 0:
            return None
        if deadline > 0 and baseline > deadline:
            return None
        # Conservatively reserve the baseline's other-stage time for each stage.
        slack = max(0, deadline - baseline) if deadline else 0
        ratio = 1.0
        changed = False
        for i, alloc in enumerate(allocations):
            stage = stages.get(alloc.get("stage_name"))
            if stage is None:
                return None
            old_duration = float(alloc.get("duration_s", 0) or 0)
            if not math.isfinite(old_duration) or old_duration <= 0:
                return None
            try:
                opt = CostMLPredictor.predict_optimal_config(stage, plan.get("schema", {}),
                    int(plan.get("csv_size_bytes", 0)), stage_index=i, n_stages=len(stages),
                    deadline_s=old_duration + slack if deadline else 0)
            except MLNotAvailable:
                return None
            if not 0 <= opt["workers"] <= MAX_WORKERS or not 0 <= opt["diu"] <= MAX_DIU:
                return None
            feat = stage_features(stage, plan.get("schema", {}), int(plan.get("csv_size_bytes", 0)), i, len(stages))
            if feat["row_count"] <= 0 or feat["csv_size_mb"] <= 0:
                return None
            before = estimate_stage_duration(alloc.get("workers", 0), alloc.get("diu", 0), alloc.get("node_type", _DEFAULT_NODE), feat)
            after = estimate_stage_duration(opt["workers"], opt["diu"], opt["node_type"], feat)
            # Do not claim better timing than either calibrated scaling or the labeler's estimate.
            duration = max(old_duration * after / before, after)
            if not math.isfinite(duration):
                return None
            memory = float(alloc.get("memory_gb", 0) or 0)
            if feat["stage_is_copy"]:
                # A copy stage's "memory" is derived from its DIU (diu × 1.5),
                # not a workload requirement — comparing it against the new
                # DIU's capacity rejected every DIU reduction. Recompute it.
                capacity = opt["diu"] * 1.5
                memory = capacity
            else:
                capacity = 4 + max(1, opt["workers"]) * NODE_SPECS[opt["node_type"]]["memory_gb"]
            if not math.isfinite(memory) or memory < 0 or memory > capacity or capacity > MAX_TOTAL_MEM_GB:
                return None
            ratio = max(ratio, duration / old_duration)
            changed |= any(alloc.get(k) != opt[k] for k in ("workers", "diu", "node_type", "shuffle_partitions"))
            alloc.update({k: opt[k] for k in ("workers", "diu", "node_type", "shuffle_partitions")})
            alloc["memory_gb"] = max(memory, opt["memory_gb"])
            if alloc["memory_gb"] > capacity:
                return None
            alloc["duration_s"] = duration
        # Without an explicit deadline, never automatically trade away runtime.
        projected = baseline * ratio
        if projected > (deadline if deadline > 0 else baseline):
            return None
        # Unknown execution grouping: reserve for the worst case (all simultaneous).
        if sum(a.get("workers", 0) for a in allocations) > MAX_WORKERS or sum(a.get("memory_gb", 0) for a in allocations) > MAX_TOTAL_MEM_GB:
            return None
        rp["estimated_total_s"] = projected
        rp["peak_concurrent_workers"] = sum(a.get("workers", 0) for a in allocations)
        old_cost = self._estimate_cost(plan, perf, resource_plan).total_usd
        new_cost = self._estimate_cost(plan, {**perf, "predicted_total_s": projected}, rp).total_usd
        return rp if changed and old_cost > 0 and new_cost <= old_cost * .97 else None

    # ── ML Primary Path ──────────────────────────────────────────────────────

    def _try_ml_suggestions(self, plan, perf, resource_plan, current_cost, suggestions, constraints):
        candidate = self._validated_ml_candidate(plan, perf, resource_plan, constraints)
        if candidate is None:
            return False
        duration = candidate["estimated_total_s"]
        new_cost = self._estimate_cost(plan, {**perf, "predicted_total_s": duration}, candidate)
        saving = round((1 - new_cost.total_usd / current_cost.total_usd) * 100, 1)
        suggestions.append(OptimizationSuggestion(
            change="apply validated ML resource configuration",
            estimated_saving=f"~{saving}%",
            trade_off=f"Estimated runtime {duration:.1f}s; synthetic estimates require production validation",
            reason="Candidate passed deadline, memory, worker and cost checks",
            new_cost=new_cost, risk_level="medium", source="ml"))
        return True

    # ── Cost Model ───────────────────────────────────────────────────────────

    def _estimate_cost(
        self,
        plan: dict,
        performance_prediction: dict,
        resource_plan: dict,
        override_cluster: Optional[dict] = None,
        override_duration_s: Optional[float] = None,
    ) -> CostBreakdown:
        stages = plan.get("stages", [])
        recommended = plan.get("recommended_settings", {})
        allocations = (
            resource_plan.get("allocations", [])
            if override_cluster is None
            else override_cluster.get(
                "allocations", resource_plan.get("allocations", [])
            )
        )

        predicted_duration_s = (
            override_duration_s
            or performance_prediction.get("predicted_total_s", 0)
            or resource_plan.get("estimated_total_s", 0)
        )
        duration_h = max(predicted_duration_s / 3600.0, 1 / 3600.0)

        compute_cost = dbu_cost = adf_cost = 0.0
        if not allocations and stages:
            allocations = [{"stage_type": st.get("type", "notebook"),
                            "workers": resource_plan.get("peak_concurrent_workers", 1),
                            "diu": 1} for st in stages]
        for alloc in allocations:
            seconds = float(alloc.get("duration_s", predicted_duration_s) or predicted_duration_s)
            hours = max(seconds, 0) / 3600
            if alloc.get("stage_type") == "copy":
                adf_cost += .001 + max(1, alloc.get("diu", 1)) * .25 * hours
                continue
            node = alloc.get("node_type", recommended.get("node_type", _DEFAULT_NODE))
            if override_cluster and override_cluster.get("node_type"):
                node = override_cluster["node_type"]
            workers = max(1, alloc.get("workers", 0))
            compute_cost += workers * NODE_HOURLY_RATES.get(node, _DEFAULT_NODE_RATE) * hours
            dbu_cost += workers * 1.5 * .55 * hours

        file_size_mb = performance_prediction.get("throughput_mb_per_s", 0) or 0
        if file_size_mb and predicted_duration_s > 0:
            file_size_mb = file_size_mb * predicted_duration_s
        else:
            file_size_mb = resource_plan.get("file_size_mb", 0) or 0
        storage_cost = (file_size_mb / 1024.0) * 0.018 * (duration_h / 730.0)

        total = round(compute_cost + dbu_cost + adf_cost + storage_cost, 6)
        return CostBreakdown(
            compute_usd=round(compute_cost, 6),
            databricks_dbu_usd=round(dbu_cost, 6),
            adf_usd=round(adf_cost, 6),
            storage_usd=round(storage_cost, 6),
            total_usd=total,
        )

    # ── Actual Cost (post-execution) ────────────────────────────────────────

    def estimate_actual_cost(
        self,
        plan: dict,
        performance_prediction: dict,
        resource_plan: dict,
        actual_duration_s: float,
    ) -> dict:
        """
        Estimate the real cost of a completed run, using the same allocations
        (workers/node_type/DIU) that were planned pre-execution, but with the
        actual observed duration substituted for the predicted one.

        This is intentionally duration-only: the Executor Agent does not
        surface actual worker/allocation usage (execute_with_retry only
        returns status/run_id/stages/sink_container), so allocations can't be
        corrected here without fabricating data. Swapping in the real
        duration is the only honest signal available post-execution.

        Called from CentralManager.record_feedback() to populate
        actual_cost_usd in manager_feedback.jsonl, alongside the pre-execution
        cost_estimate_usd, so the Learning Agent can measure real cost error.

        Returns the CostBreakdown as a dict (compute_usd, databricks_dbu_usd,
        adf_usd, storage_usd, total_usd, currency).
        """
        # _estimate_cost bills every allocation for its own predicted
        # duration_s, so the run-level override alone changed nothing and
        # "actual" always equalled the estimate. Scale each allocation by how
        # long the run really took relative to the Resource Agent's estimate
        # (the critical path those allocation durations add up to).
        rp = copy.deepcopy(resource_plan or {})
        planned_total = float(rp.get("estimated_total_s") or 0)
        if actual_duration_s and planned_total > 0:
            scale = float(actual_duration_s) / planned_total
            for alloc in rp.get("allocations", []):
                if alloc.get("duration_s"):
                    alloc["duration_s"] = float(alloc["duration_s"]) * scale
        breakdown = self._estimate_cost(
            plan,
            performance_prediction,
            rp,
            override_duration_s=actual_duration_s,
        )
        return asdict(breakdown)

    # ── Constraint Enforcement ──────────────────────────────────────────────

    def _enforce_constraints(self, suggestions, constraints, perf):
        deadline_s = constraints.get("deadline_s", 0)
        priority = constraints.get("priority", "normal")
        predicted_s = perf.get("predicted_total_s", 0)

        safe: List[OptimizationSuggestion] = []
        for s in suggestions:
            if priority == "critical" and "off-peak" in s.change.lower():
                continue
            if deadline_s > 0 and predicted_s > 0:
                if s.source != "ml":
                    new_duration = predicted_s * (1 + 0.20)
                    if new_duration > deadline_s:
                        continue
            safe.append(s)

        return safe

    # ── Ranking ─────────────────────────────────────────────────────────────

    def _rank_suggestions(self, suggestions):
        risk_penalties = {"low": 1.0, "medium": 1.5, "high": 3.0}
        for s in suggestions:
            saving = float(s.estimated_saving.replace("~", "").replace("%", ""))
            risk = risk_penalties.get(s.risk_level, 2.0)
            trade_off_len = len(s.trade_off)
            trade_off_penalty = 1.0 + (trade_off_len / 200.0)
            s.value_score = round(saving / (risk * trade_off_penalty), 4)
        return sorted(suggestions, key=lambda x: x.value_score, reverse=True)
