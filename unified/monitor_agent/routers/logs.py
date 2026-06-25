from fastapi import APIRouter, Query
from typing import Optional
from monitor_agent.deps import get_db

router = APIRouter()


@router.get("/")
async def get_logs(
    status:        Optional[str] = Query(default=None),
    pipeline_name: Optional[str] = Query(default=None),
    limit:         int           = Query(default=100, ge=1, le=500),
):
    return await get_db().get_pipeline_runs(status=status, pipeline_name=pipeline_name, limit=limit)


@router.get("/anomalies")
async def get_anomaly_logs(limit: int = Query(default=100, ge=1, le=500)):
    return await get_db().get_anomaly_log(limit=limit)
