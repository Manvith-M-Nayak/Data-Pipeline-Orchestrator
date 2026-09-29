from fastapi import APIRouter
from monitor_agent.deps import get_db
from monitor_agent.services.runtime_predictor import predict_runtime

router = APIRouter()


@router.get("/{pipeline_name}")
async def get_prediction(pipeline_name: str):
    """Runtime forecast from the Performance Prediction model (history fallback)."""
    db = get_db()
    runs       = await db.get_historical_runs_for_prediction(pipeline_name)
    stats      = await db.get_historical_stats(pipeline_name)
    prediction = await predict_runtime(db, pipeline_name, runs)
    return {"pipeline_name": pipeline_name, "prediction": prediction, "stats": stats, "run_count": len(runs)}
