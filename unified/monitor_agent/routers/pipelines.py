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


@router.get("/summary")
async def summary():
    """Aggregate counts for the home dashboard — single round-trip."""
    db = get_db()
    runs     = await db.get_pipeline_runs(limit=200)
    anomalies = await db.get_anomaly_log(limit=100)

    total     = len(runs)
    succeeded = sum(1 for r in runs if r.get("status") == "Succeeded")
    failed    = sum(1 for r in runs if r.get("status") == "Failed")

    # Anomaly count = anomaly_log entries (stuck pipelines) + all failed runs
    anomaly_count = len(anomalies) + failed

    recent_runs = runs[:5]
    recent_anomalies = anomalies[:3]

    # Recent failed runs that have AI analysis
    recent_failed = [
        r for r in runs if r.get("status") == "Failed"
    ][:3]

    return {
        "total_runs":       total,
        "succeeded":        succeeded,
        "failed":           failed,
        "anomaly_count":    anomaly_count,
        "recent_runs":      recent_runs,
        "recent_anomalies": recent_anomalies,
        "recent_failed":    recent_failed,
    }
