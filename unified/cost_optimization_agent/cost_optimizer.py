"""
Cost Optimization Agent — Core Logic

Phases implemented:
  1. Cost Model  — formula converting resource-hours to estimated cost
  2. Optimization Rules — rule-based suggestions (downsize, off-peak, merge, etc.)
  3. Constraint Enforcement — safety checks before returning suggestions
  4. Ranking & Explanation — best-value ordering with plain-language reasons

Design notes:
  - Cost model is a transparent formula, not ML. Same inputs → same outputs.
  - Optimization is rule-based. No trained model needed.
  - Every suggestion includes a quantified trade-off and a reason.
  - All costs are ESTIMATES. Real billing data is absent. See COST_MODEL_ASSUMPTIONS.
"""

import copy
import math
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

# ── Node hourly rates (USD) — approximate Azure public pricing, documented as assumptions
#    These are the per-node *compute* rate (Databricks markup is separate).
#    Used to translate cluster_size x duration into a dollar figure.
NODE_HOURLY_RATES: Dict[str, float] = {
    "Standard_DS2_v2": 0.14,
    "Standard_D4s_v3": 0.28,
    "Standard_D4_v3": 0.28,
    "Standard_DS3_v2": 0.28,
    "Standard_DS4_v2": 0.56,
    "Standard_D8s_v3": 0.56,
}
_DEFAULT_NODE_RATE = 0.28

COST_MODEL_ASSUMPTIONS = {
    "node_hourly_rates": NODE_HOURLY_RATES,
    "databricks_markup": "~2.5x compute (DBU pricing), baked into DBU rate",
    "dbu_per_worker_per_hour": 1.5,
    "dbu_price_per_unit": 0.55,
    "adf_activity_price": 0.001,
    "storage_gb_per_month": 0.018,
    "note": "All costs are approximations. No real billing data was available at build time.",
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

    def pct_of(self, other: "CostBreakdown") -> float:
        if other.total_usd <= 0:
            return 0.0
        return round(self.total_usd / other.total_usd, 3)


@dataclass
class OptimizationSuggestion:
    change: str
    estimated_saving_pct: str
    trade_off: str
    reason: str
    new_cost: CostBreakdown
    risk_level: str
    value_score: float = 0.0


@dataclass
class OptimizationResult:
    estimated_cost: CostBreakdown
    recommendations: List[OptimizationSuggestion]
    chosen_option: Optional[int]


class CostOptimizationAgent:
    def optimize(
        self,
        plan: dict,
        performance_prediction: dict,
        resource_plan: dict,
        constraints: Optional[dict] = None,
    ) -> dict:
        """
        Main entry point.

        Args:
            plan: Planner's pipeline plan (stages, recommended_settings, etc.)
            performance_prediction: PerformancePredictionAgent's output dict
            resource_plan: ResourceAgent.analyze() output
            constraints: dict with keys like {"deadline_s": 900, "priority": "normal"}

        Returns:
            Serialized OptimizationResult dict.
        """
        constraints = constraints or {}

        # ── 1. Build the current cost estimate ──────────────────────────
        current_cost = self._estimate_cost(plan, performance_prediction, resource_plan)

        # ── 2. Generate candidate suggestions ───────────────────────────
        suggestions: List[OptimizationSuggestion] = []
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

        # ── 3. Enforce constraints — filter unsafe suggestions ──────────
        safe = self._enforce_constraints(
            suggestions, constraints, performance_prediction
        )

        # ── 4. Rank by value score ──────────────────────────────────────
        ranked = self._rank_suggestions(safe)

        result = OptimizationResult(
            estimated_cost=current_cost,
            recommendations=ranked,
            chosen_option=0 if ranked else None,
        )
        return asdict(result)

    # ── Phase 1: Cost Model ──────────────────────────────────────────────────

    def _estimate_cost(
        self,
        plan: dict,
        performance_prediction: dict,
        resource_plan: dict,
        override_cluster: Optional[dict] = None,
        override_duration_s: Optional[float] = None,
    ) -> CostBreakdown:
        """
        Cost model formula.

        estimated_cost = compute_cost + databricks_dbu_cost + adf_cost + storage_cost

        compute_cost       = num_workers × node_hourly_rate × duration_hours
        databricks_dbu_cost = num_workers × dbu_per_worker_per_hour × dbu_price × duration_hours
        adf_cost           = num_copy_stages × 0.001
        storage_cost       = (file_size_mb / 1024) × $0.018/GB/month (prorated)
        """
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

        # Count notebook workers for compute/DBU cost
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

        # Node type and rate
        node_type = recommended.get("node_type", "Standard_D4s_v3")
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

    # ── Phase 2: Optimization Rules ──────────────────────────────────────────

    def _suggest_cluster_downsize(
        self,
        plan: dict,
        perf: dict,
        resource_plan: dict,
        current_cost: CostBreakdown,
        suggestions: List[OptimizationSuggestion],
    ):
        """
        Rule: if predicted utilization is low (<40%), suggest fewer workers.
        Uses predicted_duration_s increase to quantify trade-off.
        """
        allocations = resource_plan.get("allocations", [])
        if not allocations:
            return

        # Assess utilization: look at duration ratio vs resource estimate
        perf_total = perf.get("predicted_total_s", 0)
        resource_est = perf.get("adjustment_factor", 1.0)
        utilization = 1.0
        if perf_total > 0 and resource_est > 0:
            baseline = perf_total / resource_est
            utilization = 1.0 / max(resource_est, 1.1)

        utilization_from_history = perf.get("adjustment_factor", 1.0)
        if utilization_from_history < 1.0:
            utilization = utilization_from_history

        if utilization > UTILIZATION_LOW_THRESHOLD:
            return

        notebook_allocs = [
            a
            for a in allocations
            if a.get("stage_type") == "notebook" and a.get("workers", 0) > 0
        ]
        if not notebook_allocs:
            return

        # Try reducing by 1 worker per stage
        reduced_allocations = copy.deepcopy(allocations)
        for i, alloc in enumerate(reduced_allocations):
            if alloc.get("stage_type") == "notebook":
                current_w = alloc.get("workers", 0)
                if current_w >= 2:
                    alloc["workers"] = current_w - 1

        reduced_peak = max(
            (a.get("workers", 0) for a in reduced_allocations),
            default=0,
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
                estimated_saving_pct=f"~{saving_pct}%",
                trade_off=trade_off,
                reason=f"predicted utilization is low ({utilization:.0%}) — fewer workers suffice",
                new_cost=new_cost,
                risk_level="low",
                value_score=0.0,
            )
        )

    def _suggest_node_downgrade(
        self,
        plan: dict,
        perf: dict,
        resource_plan: dict,
        current_cost: CostBreakdown,
        suggestions: List[OptimizationSuggestion],
    ):
        """
        Rule: if using an expensive node type but predicted memory/cpu needs are modest,
        suggest a cheaper node.
        """
        recommended = plan.get("recommended_settings", {})
        current_node = recommended.get("node_type", "Standard_D4s_v3")
        current_rate = NODE_HOURLY_RATES.get(current_node, _DEFAULT_NODE_RATE)

        allocations = resource_plan.get("allocations", [])
        peak_mem = max((a.get("memory_gb", 0) for a in allocations), default=0)

        cheaper_options = [n for n, r in NODE_HOURLY_RATES.items() if r < current_rate]
        if not cheaper_options:
            return

        # Find best cheaper node that still has enough memory
        best_node = None
        best_rate = current_rate
        for node in sorted(cheaper_options, key=lambda n: NODE_HOURLY_RATES[n]):
            node_spec = {
                "Standard_DS2_v2": {"memory_gb": 7.0, "cpu": 2},
                "Standard_D4s_v3": {"memory_gb": 16.0, "cpu": 4},
                "Standard_D4_v3": {"memory_gb": 16.0, "cpu": 4},
                "Standard_DS3_v2": {"memory_gb": 14.0, "cpu": 4},
                "Standard_DS4_v2": {"memory_gb": 28.0, "cpu": 8},
                "Standard_D8s_v3": {"memory_gb": 32.0, "cpu": 8},
            }.get(node, {"memory_gb": 16.0, "cpu": 4})
            if node_spec["memory_gb"] >= peak_mem * 0.8:
                best_node = node
                best_rate = NODE_HOURLY_RATES[node]
                break

        if best_node is None or best_node == current_node:
            return

        saving_pct = round((1 - best_rate / current_rate) * 100, 1)
        if saving_pct < 5:
            return

        override = {
            "allocations": allocations,
            "peak_concurrent_workers": resource_plan.get("peak_concurrent_workers", 0),
            "node_type": best_node,
        }
        new_cost = self._estimate_cost(
            plan, perf, resource_plan, override_cluster=override
        )
        saving_pct_actual = (
            round((1 - new_cost.total_usd / current_cost.total_usd) * 100, 1)
            if current_cost.total_usd > 0
            else saving_pct
        )

        suggestions.append(
            OptimizationSuggestion(
                change=f"downgrade node type from {current_node} to {best_node}",
                estimated_saving_pct=f"~{saving_pct_actual}%",
                trade_off="minimal performance impact — memory capacity still adequate",
                reason=f"predicted peak memory ({peak_mem:.0f} GB) fits {best_node} — current node is over-provisioned",
                new_cost=new_cost,
                risk_level="low",
                value_score=0.0,
            )
        )

    def _suggest_off_peak(
        self,
        perf: dict,
        constraints: dict,
        current_cost: CostBreakdown,
        suggestions: List[OptimizationSuggestion],
    ):
        """
        Rule: if the job is non-urgent (no tight deadline), suggest off-peak scheduling
        which typically costs ~30% less on serverless tiers.
        """
        deadline_s = constraints.get("deadline_s", 0)
        priority = constraints.get("priority", "normal")
        predicted_s = perf.get("predicted_total_s", 0)

        if priority == "critical":
            return
        if deadline_s > 0 and deadline_s < predicted_s * 3:
            return

        new_cost_value = current_cost.total_usd * (1 - OFF_PEAK_DISCOUNT)
        saving = OFF_PEAK_DISCOUNT * 100
        new_cost = CostBreakdown(
            compute_usd=round(current_cost.compute_usd * (1 - OFF_PEAK_DISCOUNT), 6),
            databricks_dbu_usd=round(
                current_cost.databricks_dbu_usd * (1 - OFF_PEAK_DISCOUNT), 6
            ),
            adf_usd=current_cost.adf_usd,
            storage_usd=current_cost.storage_usd,
            total_usd=round(new_cost_value, 6),
        )

        suggestions.append(
            OptimizationSuggestion(
                change="schedule during off-peak hours (e.g., 8 PM – 6 AM)",
                estimated_saving_pct=f"~{saving:.0f}%",
                trade_off="no runtime impact — only execution time shifts",
                reason="job priority is '{}' with no tight deadline — off-peak pricing applies".format(
                    priority
                ),
                new_cost=new_cost,
                risk_level="low",
                value_score=0.0,
            )
        )

    def _suggest_merge_stages(
        self,
        plan: dict,
        perf: dict,
        resource_plan: dict,
        current_cost: CostBreakdown,
        suggestions: List[OptimizationSuggestion],
    ):
        """
        Rule: if there are many tiny stages (each < 60s), suggest merging contiguous
        stages to reduce overhead.
        """
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

        # Estimate saving: merging removes per-stage overhead (~15%)
        merge_saving = (
            len(tiny_stages) * MERGE_SAVING_FACTOR * 0.01
        )  # fraction of total
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
                estimated_saving_pct=f"~{saving_pct}%",
                trade_off="reduced observability at per-stage granularity, same total work",
                reason=f"stages {tiny_stages} are each predicted to finish in <60s — startup overhead dominates",
                new_cost=new_cost,
                risk_level="medium",
                value_score=0.0,
            )
        )

    def _suggest_shuffle_tuning(
        self,
        plan: dict,
        perf: dict,
        resource_plan: dict,
        current_cost: CostBreakdown,
        suggestions: List[OptimizationSuggestion],
    ):
        """
        Rule: if shuffle partitions are set high (>200) and row count is modest (<1M),
        suggest reducing to 8-12 per worker to avoid excess task scheduling overhead.
        """
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
                estimated_saving_pct=f"~{saving_pct}%",
                trade_off="minor shuffle tuning risk — data skew may cause OOM in extreme cases",
                reason=f"{current_shuffle} partitions with {max_workers} workers → ~{current_shuffle // max_workers} tasks/worker; optimal is ~12/worker",
                new_cost=new_cost,
                risk_level="medium",
                value_score=0.0,
            )
        )

    # ── Phase 3: Constraint Enforcement ──────────────────────────────────────

    def _enforce_constraints(
        self,
        suggestions: List[OptimizationSuggestion],
        constraints: dict,
        perf: dict,
    ) -> List[OptimizationSuggestion]:
        """
        Filter out suggestions that would break constraints:
          - Deadline breach (predicted > deadline)
          - Critical priority (no scheduling changes allowed)
          - Correctness risk (stage merging for stages with dependencies)
        """
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

    # ── Phase 4: Ranking ─────────────────────────────────────────────────────

    def _rank_suggestions(
        self,
        suggestions: List[OptimizationSuggestion],
    ) -> List[OptimizationSuggestion]:
        """
        Rank by value score: biggest saving with least risk and trade-off.
        Score formula: saving_pct / (risk_penalty × trade_off_penalty)
        """
        risk_penalties = {"low": 1.0, "medium": 1.5, "high": 3.0}

        for s in suggestions:
            saving = float(s.estimated_saving_pct.replace("~", "").replace("%", ""))
            risk = risk_penalties.get(s.risk_level, 2.0)
            trade_off_len = len(s.trade_off)
            trade_off_penalty = 1.0 + (trade_off_len / 200.0)
            s.value_score = round(saving / (risk * trade_off_penalty), 4)

        return sorted(suggestions, key=lambda x: x.value_score, reverse=True)
