from fastapi import APIRouter, Query
from monitor_agent.deps import get_db

router = APIRouter()


@router.get("/")
async def get_anomalies(limit: int = Query(default=100, ge=1, le=500)):
    return await get_db().get_anomaly_log(limit=limit)


@router.get("/events")
async def get_anomaly_events(
    kind: str = Query(default=None, description="Filter by anomaly kind"),
    limit: int = Query(default=100, ge=1, le=500),
):
    """Real-time classified anomaly events (anomaly_detector.py) — one row per
    detected KIND per run: failure, timeout, retry_storm, slow_runtime,
    cold_start, zero_rows, cost_spike, schema_drift."""
    return await get_db().get_anomaly_events(kind=kind, limit=limit)
