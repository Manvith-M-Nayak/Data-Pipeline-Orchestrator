from fastapi import APIRouter, Query
from monitor_agent.deps import get_db

router = APIRouter()


@router.get("/")
async def get_anomalies(limit: int = Query(default=100, ge=1, le=500)):
    return await get_db().get_anomaly_log(limit=limit)
