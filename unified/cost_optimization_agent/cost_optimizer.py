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

UTILIZATION_LOW_THRESHOLD = 0.40
TINY_STAGE_THRESHOLD_S = 60
OFF_PEAK_DISCOUNT = 0.30
MERGE_SAVING_FACTOR = 0.15


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

    def _apply_cluster_downsize(self, plan, perf, rp, constraints) -> bool:
        allocations = rp.get("allocations", [])
        if not allocations:
            return False

        utilization = perf.get("adjustment_factor", 1.0)
        if utilization > UTILIZATION_LOW_THRESHOLD:
            return False

        notebook_allocs = [
            a
            for a in allocations
            if a.get("stage_type") == "notebook" and a.get("workers", 0) > 0
        ]
        if not notebook_allocs:
            return False

        for alloc in allocations:
            if alloc.get("stage_type") == "notebook":
                w = alloc.get("workers", 0)
                if w >= 2:
                    alloc["workers"] = w - 1

        rp["peak_concurrent_workers"] = max(
            (a.get("workers", 0) for a in allocations), default=0
        )
        return True

    def _apply_node_downgrade(self, plan, perf, rp, constraints) -> bool:
        recommended = plan.get("recommended_settings", {})
        current_node = recommended.get("node_type", _DEFAULT_NODE)
        current_rate = NODE_HOURLY_RATES.get(current_node, _DEFAULT_NODE_RATE)
        allocations = rp.get("allocations", [])
        peak_mem = max((a.get("memory_gb", 0) for a in allocations), default=0)
        cheaper_options = [n for n, r in NODE_HOURLY_RATES.items() if r < current_rate]
        if not cheaper_options:
            return False

        node_specs = {
            "Standard_DS2_v2": {"memory_gb": 7.0, "cpu": 2},
            "Standard_D4s_v3": {"memory_gb": 16.0, "cpu": 4},
            "Standard_D4_v3": {"memory_gb": 16.0, "cpu": 4},
            "Standard_DS3_v2": {"memory_gb": 14.0, "cpu": 4},
            "Standard_DS4_v2": {"memory_gb": 28.0, "cpu": 8},
            "Standard_D8s_v3": {"memory_gb": 32.0, "cpu": 8},
        }
        best_node = None
        for node in sorted(cheaper_options, key=lambda n: NODE_HOURLY_RATES[n]):
            spec = node_specs.get(node, {"memory_gb": 16.0, "cpu": 4})
            if spec["memory_gb"] >= peak_mem * 0.8:
                best_node = node
                break

        if best_node is None or best_node == current_node:
            return False

        saving_pct = round((1 - NODE_HOURLY_RATES[best_node] / current_rate) * 100, 1)
        if saving_pct < 5:
            return False

        for alloc in allocations:
            alloc["node_type"] = best_node
        rp["node_type"] = best_node
        return True

    def _apply_shuffle_tuning(self, plan, perf, rp, constraints) -> bool:
        recommended = plan.get("recommended_settings", {})
        current_shuffle = recommended.get("shuffle_partitions", 200)
        allocations = rp.get("allocations", [])
        notebook_allocs = [a for a in allocations if a.get("stage_type") == "notebook"]
        if not notebook_allocs:
            return False

        max_workers = max((a.get("workers", 1) for a in notebook_allocs), default=1)
        optimal_shuffle = max(8, min(current_shuffle, max_workers * 12))

        if optimal_shuffle >= current_shuffle or current_shuffle <= 100:
            return False

        for alloc in allocations:
            if alloc.get("stage_type") == "notebook":
                alloc["shuffle_partitions"] = optimal_shuffle
        return True

    def _enforce_constraints_single(
        self, rule_name: str, modified_rp: dict, constraints: dict, perf: dict
    ) -> bool:
        deadline_s = constraints.get("deadline_s", 0)
        predicted_s = perf.get("predicted_total_s", 0)


        if deadline_s > 0 and predicted_s > 0:
            if "downsize" in rule_name or "downgrade" in rule_name:
                new_duration = predicted_s * (1 + 0.20)
                if new_duration > deadline_s:
                    return False
        return True

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

    # ── Rule-based Fallback Suggestions ──────────────────────────────────────

    def _suggest_cluster_downsize(
        self, plan, perf, resource_plan, current_cost, suggestions
    ):
        allocations = resource_plan.get("allocations", [])
        if not allocations:
            return

        utilization_from_history = perf.get("adjustment_factor", 1.0)
        if utilization_from_history > UTILIZATION_LOW_THRESHOLD:
            return

        notebook_allocs = [
            a
            for a in allocations
            if a.get("stage_type") == "notebook" and a.get("workers", 0) > 0
        ]
        if not notebook_allocs:
            return

        reduced_allocations = copy.deepcopy(allocations)
        for alloc in reduced_allocations:
            if alloc.get("stage_type") == "notebook":
                current_w = alloc.get("workers", 0)
                if current_w >= 2:
                    alloc["workers"] = current_w - 1

        reduced_peak = max(
            (a.get("workers", 0) for a in reduced_allocations), default=0
        )
        override = {
            "allocations": reduced_allocations,
            "peak_concurrent_workers": reduced_peak,
        }

        new_cost = self._estimate_cost(
            plan, perf, resource_plan, override_cluster=override
        )
        saving_pct = (
            round((1 - new_cost.total_usd / current_cost.total_usd) * 100, 1)
            if current_cost.total_usd > 0
            else 0
        )

        if saving_pct < 3:
            return

        duration_increase_pct = round(
            (
                perf.get("predicted_total_s", 0)
                / max(resource_plan.get("estimated_total_s", 1), 1)
                - 1
            )
            * 100,
            1,
        )
        trade_off = (
            f"~{duration_increase_pct}% longer runtime, still within typical deadlines"
            if duration_increase_pct > 0
            else "negligible runtime impact"
        )
        worker_diff = sum(
            a.get("workers", 0)
            for a in allocations
            if a.get("stage_type") == "notebook"
        ) - sum(
            a.get("workers", 0)
            for a in reduced_allocations
            if a.get("stage_type") == "notebook"
        )

        suggestions.append(
            OptimizationSuggestion(
                change=f"reduce cluster from {worker_diff + reduced_peak} to {reduced_peak} nodes",
                estimated_saving=f"~{saving_pct}%",
                trade_off=trade_off,
                reason=f"predicted utilization is low ({utilization_from_history:.0%}) — fewer workers suffice",
                new_cost=new_cost,
                risk_level="low",
                value_score=0.0,
                source="rule",
            )
        )

    def _suggest_node_downgrade(
        self, plan, perf, resource_plan, current_cost, suggestions
    ):
        recommended = plan.get("recommended_settings", {})
        current_node = recommended.get("node_type", _DEFAULT_NODE)
        current_rate = NODE_HOURLY_RATES.get(current_node, _DEFAULT_NODE_RATE)
        allocations = resource_plan.get("allocations", [])
        peak_mem = max((a.get("memory_gb", 0) for a in allocations), default=0)
        cheaper_options = [n for n, r in NODE_HOURLY_RATES.items() if r < current_rate]
        if not cheaper_options:
            return

        best_node = None
        best_rate = current_rate
        node_specs_map = {
            "Standard_DS2_v2": {"memory_gb": 7.0, "cpu": 2},
            "Standard_D4s_v3": {"memory_gb": 16.0, "cpu": 4},
            "Standard_D4_v3": {"memory_gb": 16.0, "cpu": 4},
            "Standard_DS3_v2": {"memory_gb": 14.0, "cpu": 4},
            "Standard_DS4_v2": {"memory_gb": 28.0, "cpu": 8},
            "Standard_D8s_v3": {"memory_gb": 32.0, "cpu": 8},
        }
        for node in sorted(cheaper_options, key=lambda n: NODE_HOURLY_RATES[n]):
            spec = node_specs_map.get(node, {"memory_gb": 16.0, "cpu": 4})
            if spec["memory_gb"] >= peak_mem * 0.8:
                best_node = node
                best_rate = NODE_HOURLY_RATES[node]
                break

        if best_node is None or best_node == current_node:
            return

        saving_pct_estimate = round((1 - best_rate / current_rate) * 100, 1)
        if saving_pct_estimate < 5:
            return

        override = {
            "allocations": allocations,
            "peak_concurrent_workers": resource_plan.get("peak_concurrent_workers", 0),
            "node_type": best_node,
        }
        new_cost = self._estimate_cost(
            plan, perf, resource_plan, override_cluster=override
        )
        saving_pct = (
            round((1 - new_cost.total_usd / current_cost.total_usd) * 100, 1)
            if current_cost.total_usd > 0
            else saving_pct_estimate
        )

        suggestions.append(
            OptimizationSuggestion(
                change=f"downgrade node type from {current_node} to {best_node}",
                estimated_saving=f"~{saving_pct}%",
                trade_off="minimal performance impact — memory capacity still adequate",
                reason=f"predicted peak memory ({peak_mem:.0f} GB) fits {best_node} — current node is over-provisioned",
                new_cost=new_cost,
                risk_level="low",
                value_score=0.0,
                source="rule",
            )
        )

    def _suggest_off_peak(self, perf, constraints, current_cost, suggestions):
        deadline_s = constraints.get("deadline_s", 0)
        priority = constraints.get("priority", "normal")
        predicted_s = perf.get("predicted_total_s", 0)
        if priority == "critical":
            return
        if deadline_s > 0 and deadline_s < predicted_s * 3:
            return

        new_cost = CostBreakdown(
            compute_usd=round(current_cost.compute_usd * (1 - OFF_PEAK_DISCOUNT), 6),
            databricks_dbu_usd=round(
                current_cost.databricks_dbu_usd * (1 - OFF_PEAK_DISCOUNT), 6
            ),
            adf_usd=current_cost.adf_usd,
            storage_usd=current_cost.storage_usd,
            total_usd=round(current_cost.total_usd * (1 - OFF_PEAK_DISCOUNT), 6),
        )

        suggestions.append(
            OptimizationSuggestion(
                change="schedule during off-peak hours (e.g., 8 PM - 6 AM)",
                estimated_saving=f"~{OFF_PEAK_DISCOUNT * 100:.0f}%",
                trade_off="no runtime impact — only execution time shifts",
                reason=f"job priority is '{priority}' with no tight deadline — off-peak pricing applies",
                new_cost=new_cost,
                risk_level="low",
                value_score=0.0,
                source="rule",
            )
        )

    def _suggest_merge_stages(
        self, plan, perf, resource_plan, current_cost, suggestions
    ):
        stages = plan.get("stages", [])
        allocations = resource_plan.get("allocations", [])
        alloc_map = {a.get("stage_name"): a for a in allocations}

        tiny_stages = []
        for s in stages:
            name = s.get("name", "")
            alloc = alloc_map.get(name)
            dur = alloc.get("duration_s", 0) if alloc else 0
            if 0 < dur < TINY_STAGE_THRESHOLD_S:
                tiny_stages.append(name)

        if len(tiny_stages) < 2:
            return

        merge_saving = len(tiny_stages) * MERGE_SAVING_FACTOR * 0.01
        saving_pct = round(min(merge_saving * 100, 20), 1)
        if saving_pct < 2:
            return

        new_total = current_cost.total_usd * (1 - saving_pct / 100)
        new_cost = CostBreakdown(
            compute_usd=round(current_cost.compute_usd * (1 - saving_pct / 100), 6),
            databricks_dbu_usd=round(
                current_cost.databricks_dbu_usd * (1 - saving_pct / 100), 6
            ),
            adf_usd=current_cost.adf_usd,
            storage_usd=current_cost.storage_usd,
            total_usd=round(new_total, 6),
        )

        suggestions.append(
            OptimizationSuggestion(
                change=f"merge {len(tiny_stages)} tiny stages (<60s each) into fewer stages",
                estimated_saving=f"~{saving_pct}%",
                trade_off="reduced observability at per-stage granularity, same total work",
                reason=f"stages {tiny_stages} are each predicted to finish in <60s — startup overhead dominates",
                new_cost=new_cost,
                risk_level="medium",
                value_score=0.0,
                source="rule",
            )
        )

    def _suggest_shuffle_tuning(
        self, plan, perf, resource_plan, current_cost, suggestions
    ):
        recommended = plan.get("recommended_settings", {})
        current_shuffle = recommended.get("shuffle_partitions", 200)
        allocations = resource_plan.get("allocations", [])
        notebook_allocs = [a for a in allocations if a.get("stage_type") == "notebook"]
        if not notebook_allocs:
            return

        max_workers = max((a.get("workers", 1) for a in notebook_allocs), default=1)
        max_workers = max(max_workers, 1)
        optimal_shuffle = max(8, min(current_shuffle, max_workers * 12))

        if optimal_shuffle >= current_shuffle or current_shuffle <= 100:
            return

        saving_fraction = min(
            (current_shuffle - optimal_shuffle) / current_shuffle * 0.10, 0.05
        )
        saving_pct = round(saving_fraction * 100, 1)

        new_total = current_cost.total_usd * (1 - saving_fraction)
        new_cost = CostBreakdown(
            compute_usd=round(current_cost.compute_usd * (1 - saving_fraction), 6),
            databricks_dbu_usd=round(
                current_cost.databricks_dbu_usd * (1 - saving_fraction), 6
            ),
            adf_usd=current_cost.adf_usd,
            storage_usd=current_cost.storage_usd,
            total_usd=round(new_total, 6),
        )

        suggestions.append(
            OptimizationSuggestion(
                change=f"reduce shuffle partitions from {current_shuffle} to {optimal_shuffle}",
                estimated_saving=f"~{saving_pct}%",
                trade_off="minor shuffle tuning risk — data skew may cause OOM in extreme cases",
                reason=f"{current_shuffle} partitions with {max_workers} workers -> ~{current_shuffle // max_workers} tasks/worker; optimal is ~12/worker",
                new_cost=new_cost,
                risk_level="medium",
                value_score=0.0,
                source="rule",
            )
        )

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
