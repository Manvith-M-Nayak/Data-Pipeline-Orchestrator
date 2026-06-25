from fastapi import APIRouter
from monitor_agent.deps import get_db, get_groq

router = APIRouter()


@router.get("/{pipeline_name}")
async def get_prediction(pipeline_name: str):
    db, groq = get_db(), get_groq()
    runs       = await db.get_historical_runs_for_prediction(pipeline_name)
    stats      = await db.get_historical_stats(pipeline_name)
    prediction = await groq.predict_runtime(pipeline_name, runs)
    return {"pipeline_name": pipeline_name, "prediction": prediction, "stats": stats, "run_count": len(runs)}
