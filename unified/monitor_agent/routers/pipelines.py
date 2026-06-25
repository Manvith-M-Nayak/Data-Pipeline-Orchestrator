from fastapi import APIRouter, Query
from monitor_agent.deps import get_db, get_monitor

router = APIRouter()


@router.get("/live")
async def live_pipelines():
    return get_monitor().get_live_runs()


@router.get("/names")
async def pipeline_names():
    return await get_db().get_known_pipeline_names()


@router.post("/sync")
async def sync_historical(hours: int = Query(default=48, ge=1, le=168)):
    count = await get_monitor().sync_historical(hours)
    return {"synced": count, "hours": hours}


@router.get("/stats/{pipeline_name}")
async def pipeline_stats(pipeline_name: str):
    return await get_db().get_historical_stats(pipeline_name)
