"""
Cost Optimization Agent — reduces pipeline cost without breaking correctness or deadlines.

Uses a cost model formula (cluster_size x node_rate x duration) to estimate
cost from predicted resource usage, then applies rule-based optimization to
suggest cheaper alternatives. Only safe, constraint-respecting suggestions
are returned, ranked by best value.

Quick use:
    from cost_optimization_agent import CostOptimizationAgent
    result = CostOptimizationAgent().optimize(
        plan=plan,
        performance_prediction=perf_pred,
        resource_plan=resource_plan,
        constraints={"deadline_s": 900},
    )
"""

from .cost_optimizer import (
    CostOptimizationAgent,
    OptimizationResult,
    OptimizationSuggestion,
    CostBreakdown,
    NODE_HOURLY_RATES,
    COST_MODEL_ASSUMPTIONS,
)

__all__ = [
    "CostOptimizationAgent",
    "OptimizationResult",
    "OptimizationSuggestion",
    "CostBreakdown",
    "NODE_HOURLY_RATES",
    "COST_MODEL_ASSUMPTIONS",
]
