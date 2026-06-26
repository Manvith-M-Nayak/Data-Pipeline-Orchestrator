"""
Performance Prediction Agent — FastAPI router.

Exposes:
  POST /performance-prediction/predict   → run a full prediction given a resource_plan
  GET  /performance-prediction/history   → summary of historical prediction accuracy
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List, Optional

from .performance_agent import PerformancePredictionAgent, DEFAULT_SLA_TARGET_S

router = APIRouter()
_agent = PerformancePredictionAgent()


# ── Request / response models ─────────────────────────────────────────────────
class PredictRequest(BaseModel):
    resource_plan: Dict[str, Any]
    predictions:   Dict[str, Any]           # Manager's state.predictions dict
    plan:          Dict[str, Any]           # raw Planner plan
    sla_target_s:  int = DEFAULT_SLA_TARGET_S


# ── Routes ────────────────────────────────────────────────────────────────────
@router.post("/predict")
def predict(req: PredictRequest):
    """
    Full performance prediction for a plan.

    Call after ResourceAgent.analyze() has run (i.e. resource_plan is populated).
    Returns predicted_total_s, bottleneck_stage, outcome, confidence, sla_breach_risk,
    per-stage forecasts, and a plain-English rationale.
    """
    if not req.resource_plan.get("allocations"):
        raise HTTPException(
            status_code=422,
            detail="resource_plan must contain 'allocations' — run ResourceAgent.analyze() first.",
        )
    return _agent.predict(
        resource_plan=req.resource_plan,
        predictions=req.predictions,
        plan=req.plan,
        sla_target_s=req.sla_target_s,
    )


@router.get("/history")
def prediction_history():
    """
    Returns the last 50 entries from manager_feedback.jsonl that have both
    actual_duration_s and predicted_duration_s, for dashboard display.
    """
    import json, os
    _DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
    log_path = os.path.join(_DATA_DIR, "manager_feedback.jsonl")

    if not os.path.exists(log_path):
        return {"records": [], "total": 0}

    records = []
    try:
        with open(log_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    if r.get("actual_duration_s") and r.get("predicted_duration_s"):
                        records.append(r)
                except json.JSONDecodeError:
                    pass
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    recent = records[-50:]
    return {"records": recent, "total": len(records)}