"""
Runtime prediction for the monitor's Predictions page.

Uses the Performance Prediction Agent's trained model instead of asking an
LLM: the most recent orchestrator run that produced this pipeline supplies the
plan / resource plan the model needs. Pipelines with no orchestrator plan
(e.g. created directly in ADF) fall back to plain historical statistics.

Output keeps the shape the frontend already renders:
    predicted_duration_sec, confidence (low|medium|high),
    range_min_sec, range_max_sec, reasoning, source
"""

import asyncio
from typing import Dict, List, Optional

from .db_service import DBService

# The ADF copy pipeline only runs the plan's copy stages, so its duration is
# compared against the copy-stage forecasts, not the whole-plan total.
_COPY_PIPELINE = "Orchestrator_Copy_Pipeline"


def _confidence_label(conf: float) -> str:
    return "high" if conf >= 0.75 else "medium" if conf >= 0.5 else "low"


def _from_history(runs: List[Dict]) -> Dict:
    durations = [r["duration_ms"] / 1000 for r in runs
                 if r.get("duration_ms") and r.get("status") == "Succeeded"]
    if not durations:
        return {
            "predicted_duration_sec": 0, "confidence": "low",
            "range_min_sec": 0, "range_max_sec": 0,
            "reasoning": "No successful historical runs to predict from.",
            "source": "none",
        }
    n = len(durations)
    avg = sum(durations) / n
    return {
        "predicted_duration_sec": round(avg, 1),
        "confidence": "high" if n >= 10 else "medium" if n >= 3 else "low",
        "range_min_sec": round(min(durations), 1),
        "range_max_sec": round(max(durations), 1),
        "reasoning": f"Historical average of {n} successful run(s); no orchestrator "
                     "plan was found for this pipeline, so the ML model can't be used.",
        "source": "history",
    }


def _find_state(states: List[Dict], run_ids: set) -> Optional[Dict]:
    """Most recent manager run whose executor produced one of run_ids."""
    for st in states:
        ex = st.get("executor_result") or {}
        if {ex.get("run_id"), ex.get("dbx_run_id"), ex.get("adf_run_id")} & run_ids:
            return st
    return None


def _model_prediction(state: Dict, pipeline_name: str) -> Optional[Dict]:
    from performance_prediction_agent.performance_agent import PerformancePredictionAgent

    resource_plan = state.get("resource_plan") or {}
    if not resource_plan.get("allocations"):
        return None
    pred = PerformancePredictionAgent().predict(
        resource_plan=resource_plan,
        predictions=state.get("predictions") or {},
        plan=state.get("plan") or {},
    )
    total = float(pred.get("predicted_total_s") or 0)
    scope = "whole run"
    if pipeline_name == _COPY_PIPELINE:
        copy_names = {s.get("name") for s in (state.get("plan") or {}).get("stages", [])
                      if s.get("type") == "copy"}
        copy_s = sum(f.get("predicted_s", 0) for f in pred.get("stage_forecasts", [])
                     if f.get("name") in copy_names)
        if copy_s:
            total, scope = float(copy_s), "copy stages"
    if total <= 0:
        return None
    conf = float(pred.get("confidence") or 0)
    spread = max(0.1, 1 - conf)   # lower confidence → wider range
    return {
        "predicted_duration_sec": round(total, 1),
        "confidence": _confidence_label(conf),
        "range_min_sec": round(total * (1 - spread), 1),
        "range_max_sec": round(total * (1 + spread), 1),
        "reasoning": f"Performance Prediction model ({scope}) on this pipeline's latest "
                     f"plan (run {str(state.get('run_id', ''))[:8]}): {pred.get('rationale', '')}".strip(),
        "source": "performance_model",
    }


async def predict_runtime(db: DBService, pipeline_name: str, runs: List[Dict]) -> Dict:
    run_ids = {r["run_id"] for r in runs if r.get("run_id")}
    if run_ids:
        try:
            state = _find_state(await db.recent_manager_states(limit=200), run_ids)
            if state is not None:
                result = await asyncio.to_thread(_model_prediction, state, pipeline_name)
                if result:
                    return result
        except Exception as exc:
            print(f"[predictions] model path non-fatal, using history: {exc}")
    return _from_history(runs)
