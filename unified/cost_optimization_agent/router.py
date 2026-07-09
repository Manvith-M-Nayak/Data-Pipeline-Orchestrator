"""Cost Optimization Agent — FastAPI router."""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Any, Dict, List, Optional

from .cost_optimizer import (
    CostOptimizationAgent,
    NODE_HOURLY_RATES,
    COST_MODEL_ASSUMPTIONS,
)

router = APIRouter()
_agent = CostOptimizationAgent()


class OptimizeRequest(BaseModel):
    plan: Dict[str, Any]
    performance_prediction: Dict[str, Any]
    resource_plan: Dict[str, Any]
    constraints: Optional[Dict[str, Any]] = None


class CostEstimateRequest(BaseModel):
    plan: Dict[str, Any]
    performance_prediction: Dict[str, Any]
    resource_plan: Dict[str, Any]


@router.post("/optimize")
def optimize(req: OptimizeRequest):
    """Full cost optimization: estimate → suggest → rank → return."""
    return _agent.optimize(
        plan=req.plan,
        performance_prediction=req.performance_prediction,
        resource_plan=req.resource_plan,
        constraints=req.constraints,
    )


@router.post("/estimate")
def estimate_cost(req: CostEstimateRequest):
    """Estimate cost only (no optimization), using the cost model."""
    cost = _agent._estimate_cost(
        plan=req.plan,
        performance_prediction=req.performance_prediction,
        resource_plan=req.resource_plan,
    )
    return {
        "estimated_cost": {
            "compute_usd": cost.compute_usd,
            "databricks_dbu_usd": cost.databricks_dbu_usd,
            "adf_usd": cost.adf_usd,
            "storage_usd": cost.storage_usd,
            "total_usd": cost.total_usd,
            "currency": cost.currency,
        },
        "model_assumptions": COST_MODEL_ASSUMPTIONS,
    }


@router.get("/node-rates")
def node_rates():
    """Current node hourly rates used by the cost model."""
    return {
        "node_hourly_rates": NODE_HOURLY_RATES,
        "assumptions": COST_MODEL_ASSUMPTIONS,
    }
