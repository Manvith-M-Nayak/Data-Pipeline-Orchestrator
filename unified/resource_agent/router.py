"""Resource Agent — FastAPI router."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List, Optional

from .resource_agent import (
    ResourceAgent,
    MAX_WORKERS,
    MAX_DIU,
    MAX_CONCURRENT,
    MAX_TOTAL_MEM_GB,
    NODE_SPECS,
    DEFAULT_NODE,
    ADF_MB_PER_DIU_PER_S,
)

router = APIRouter()
_agent = ResourceAgent()


# ── Request / response models ─────────────────────────────────────────────────
class AnalyzeRequest(BaseModel):
    plan:           Dict[str, Any]
    csv_size_bytes: int                = 0
    schema:         Optional[Dict]     = None
    execution_groups: Optional[List[List[str]]] = None


class FeedbackRequest(BaseModel):
    run_id:               str   = ""
    stage_name:           str
    stage_type:           str
    predicted_duration_s: float
    actual_duration_s:    float
    predicted_workers:    int   = 0
    actual_workers:       int   = 0


class ReallocateRequest(BaseModel):
    live_runs:   List[Dict[str, Any]]
    allocations: List[Dict[str, Any]]
    elapsed_s:   float = 0.0


# ── Routes ────────────────────────────────────────────────────────────────────
@router.post("/analyze")
def analyze(req: AnalyzeRequest):
    """Full resource analysis: predict → feasibility → allocate → right-size → contention."""
    return _agent.analyze(
        plan=req.plan,
        csv_size_bytes=req.csv_size_bytes,
        schema=req.schema,
        execution_groups=req.execution_groups,
    )


@router.post("/reallocate")
def reallocate(req: ReallocateRequest):
    """Dynamic re-allocation recommendations from live Monitor data."""
    from .resource_agent import StageAllocation
    allocs = [
        StageAllocation(
            stage_name=a.get("stage_name", ""),
            stage_type=a.get("stage_type", "notebook"),
            workers=int(a.get("workers", 0)),
            diu=int(a.get("diu", 0)),
            memory_gb=float(a.get("memory_gb", 0)),
            cpu=float(a.get("cpu", 0)),
            duration_s=int(a.get("duration_s", 0)),
            right_sized=bool(a.get("right_sized", False)),
            contention_adjusted=bool(a.get("contention_adjusted", False)),
        )
        for a in req.allocations
    ]
    return {
        "recommendations": _agent.dynamic_reallocate(
            req.live_runs, allocs, req.elapsed_s
        )
    }


@router.post("/feedback")
def record_feedback(req: FeedbackRequest):
    """Record actual vs predicted duration for self-correction."""
    _agent.record_actual(
        stage_name=req.stage_name,
        stage_type=req.stage_type,
        predicted_duration_s=req.predicted_duration_s,
        actual_duration_s=req.actual_duration_s,
        predicted_workers=req.predicted_workers,
        actual_workers=req.actual_workers,
        run_id=req.run_id,
    )
    return {"status": "recorded"}


@router.get("/accuracy")
def accuracy():
    """Prediction accuracy report derived from feedback history."""
    return _agent.get_accuracy_report()


@router.get("/correction-factors")
def correction_factors():
    """Current correction factors per stage type."""
    return {
        "copy":     _agent.get_correction_factor("copy"),
        "notebook": _agent.get_correction_factor("notebook"),
    }


@router.get("/limits")
def limits():
    """Student-tier hard limits and node catalogue (single source of truth for the UI)."""
    return {
        "max_workers":          MAX_WORKERS,
        "max_diu":              MAX_DIU,
        "max_concurrent":       MAX_CONCURRENT,
        "max_total_mem_gb":     MAX_TOTAL_MEM_GB,
        "default_node":         DEFAULT_NODE,
        "adf_mb_per_diu_per_s": ADF_MB_PER_DIU_PER_S,
        "node_specs":           NODE_SPECS,
    }
