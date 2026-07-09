"""
Cost Optimization Agent — ML-driven cost reduction for data pipelines.

Uses a trained HistGradientBoosting model (cost_models.pkl) to predict
cost-optimal resource configurations, with rule-based heuristic fallback
when the model is unavailable.

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
from .ml_predictor import CostMLPredictor, MLNotAvailable

__all__ = [
    "CostOptimizationAgent",
    "OptimizationResult",
    "OptimizationSuggestion",
    "CostBreakdown",
    "CostMLPredictor",
    "MLNotAvailable",
    "NODE_HOURLY_RATES",
    "COST_MODEL_ASSUMPTIONS",
]
