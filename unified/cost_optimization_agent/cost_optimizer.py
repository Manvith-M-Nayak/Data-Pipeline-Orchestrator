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
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

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

        # ── Fallback: rule-based heuristics ─────────────────────────────
        if not ml_used:
            self._suggest_cluster_downsize(
                plan, performance_prediction, resource_plan, current_cost, suggestions
            )
            self._suggest_node_downgrade(
                plan, performance_prediction, resource_plan, current_cost, suggestions
            )
            self._suggest_off_peak(
                performance_prediction, constraints, current_cost, suggestions
            )
            self._suggest_merge_stages(
                plan, performance_prediction, resource_plan, current_cost, suggestions
            )
            self._suggest_shuffle_tuning(
                plan, performance_prediction, resource_plan, current_cost, suggestions
            )

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

    # ── ML Primary Path ──────────────────────────────────────────────────────

    def _try_ml_suggestions(
        self,
        plan: dict,
        perf: dict,
        resource_plan: dict,
        current_cost: CostBreakdown,
        suggestions: List[OptimizationSuggestion],
        constraints: dict,
    ) -> bool:
        """Try ML predictions. Returns True if ML was used."""
        try:
            from cost_optimization_agent.ml_predictor import (
                CostMLPredictor,
                MLNotAvailable,
            )

            if not CostMLPredictor.is_available():
                return False
        except Exception:
            return False

        stages = plan.get("stages", [])
        allocations = resource_plan.get("allocations", [])
        alloc_map = {a.get("stage_name"): a for a in allocations}
        schema = plan.get("schema", {})
        csv_size_bytes = int(plan.get("csv_size_bytes", 0))
        n_stages = len(stages)

        ml_allocations = copy.deepcopy(allocations)
        total_saving = 0.0

        for i, stage in enumerate(stages):
            name = stage.get("name", "")
            current_alloc = alloc_map.get(name)
            if not current_alloc:
                continue

            try:
                opt = CostMLPredictor.predict_optimal_config(
                    stage, schema, csv_size_bytes, stage_index=i, n_stages=n_stages
                )
            except MLNotAvailable:
                continue

            for ml_alloc in ml_allocations:
                if ml_alloc.get("stage_name") == name:
                    old_workers = ml_alloc.get("workers", 0)
                    old_node = ml_alloc.get("node_type", _DEFAULT_NODE)
                    new_workers = opt["workers"]
                    new_node = opt["node_type"]

                    if ml_alloc.get("stage_type") == "copy":
                        old_diu = ml_alloc.get("diu", 2)
                        new_diu = opt["diu"]
                        if new_diu < old_diu:
                            ml_alloc["diu"] = new_diu
                            ml_alloc["memory_gb"] = opt["memory_gb"]
                    elif new_workers < old_workers or new_node != old_node:
                        ml_alloc["workers"] = new_workers
                        ml_alloc["node_type"] = new_node
                        ml_alloc["shuffle_partitions"] = opt["shuffle_partitions"]
                        ml_alloc["memory_gb"] = opt["memory_gb"]
                    break

        new_cost = self._estimate_cost(
            plan,
            perf,
            resource_plan,
            override_cluster={
                "allocations": ml_allocations,
                "peak_concurrent_workers": max(
                    a.get("workers", 0) for a in ml_allocations
                ),
            },
        )
        saving_pct = (
            round((1 - new_cost.total_usd / current_cost.total_usd) * 100, 1)
            if current_cost.total_usd > 0
            else 0
        )

        if saving_pct >= 3:
            changes = []
            for i, stage in enumerate(stages):
                name = stage.get("name", "")
                old_a = alloc_map.get(name)
                new_a = next(
                    (a for a in ml_allocations if a.get("stage_name") == name), None
                )
                if not old_a or not new_a:
                    continue
                if old_a.get("workers", 0) != new_a.get("workers", 0):
                    changes.append(
                        f"{name}: {old_a.get('workers', 0)}w->{new_a.get('workers', 0)}w"
                    )
                if old_a.get("diu", 0) != new_a.get("diu", 0):
                    changes.append(
                        f"{name}: {old_a.get('diu', 0)}DIU->{new_a.get('diu', 0)}DIU"
                    )
                if old_a.get("node_type") != new_a.get("node_type"):
                    changes.append(
                        f"{name}: {old_a.get('node_type', '?')}->{new_a.get('node_type', '?')}"
                    )

            reason = "ML model predicted cost-optimal config: " + "; ".join(changes[:3])
            if len(changes) > 3:
                reason += f" (+{len(changes) - 3} more)"

            suggestions.append(
                OptimizationSuggestion(
                    change="; ".join(changes[:2])
                    if changes
                    else "apply ML-recommended resource adjustments",
                    estimated_saving=f"~{saving_pct}%",
                    trade_off="negligible runtime impact — model optimizes for cost within feasible configs",
                    reason=reason,
                    new_cost=new_cost,
                    risk_level="low",
                    value_score=0.0,
                    source="ml",
                )
            )
            return True

        return False

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

        notebook_workers = 0
        total_workers = resource_plan.get("peak_concurrent_workers", 0)
        if override_cluster:
            total_workers = override_cluster.get(
                "peak_concurrent_workers", total_workers
            )

        for alloc in allocations:
            if alloc.get("stage_type") == "notebook":
                notebook_workers = max(notebook_workers, alloc.get("workers", 0))
        notebook_workers = max(notebook_workers, total_workers, 1)

        node_type = recommended.get("node_type", _DEFAULT_NODE)
        if override_cluster and override_cluster.get("node_type"):
            node_type = override_cluster["node_type"]
        node_rate = NODE_HOURLY_RATES.get(node_type, _DEFAULT_NODE_RATE)

        compute_cost = notebook_workers * node_rate * duration_h
        dbu_cost = notebook_workers * 1.5 * 0.55 * duration_h

        copy_stages = sum(1 for s in stages if s.get("type") == "copy")
        adf_cost = copy_stages * 0.001

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

    # ── Actual-cost tracking ──────────────────────────────────────────────────

    def estimate_actual_cost(
        self,
        plan: dict,
        performance_prediction: dict,
        resource_plan: dict,
        actual_duration_s: float,
    ) -> dict:
        """Recompute the cost formula using actual elapsed runtime.

        This is NOT real Azure billing.  Node rates, worker counts, and DIU
        are still plan assumptions — only the duration is replaced with the
        measured wall-clock time.  Labeled in the feedback log as
        "cost recomputed with actual runtime".
        """
        return asdict(
            self._estimate_cost(
                plan,
                performance_prediction,
                resource_plan,
                override_duration_s=actual_duration_s,
            )
        )

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
                if "cluster" in s.change.lower() or "node" in s.change.lower():
                    new_duration = predicted_s * (1 + 0.20)
                    if new_duration > deadline_s:
                        continue
            safe.append(s)

        if not safe and suggestions:
            safe.append(suggestions[0])
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
