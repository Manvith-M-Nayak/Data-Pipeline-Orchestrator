import asyncio
import datetime
from typing import Dict

from fastapi import APIRouter, HTTPException

router = APIRouter()

# Legacy direct-run registry. Kept so /status and /jobs stay valid endpoints
# for old clients, but no new entries are created — all runs now flow through
# the Central Manager, which invokes the executor itself.
_jobs: Dict[str, Dict] = {}


@router.post("/run")
async def run_pipeline():
    """Direct executor runs are disabled.

    Every pipeline run must go through the Central Manager
    (POST /api/manager/run), which validates the plan, runs assurance and
    resource/cost pre-checks, and then hands off to the executor.
    """
    raise HTTPException(
        status_code=410,
        detail="Direct executor runs are disabled — start runs via POST /api/manager/run "
               "(the Central Manager invokes the executor).",
    )


async def _notify_monitor(result: dict, elapsed_ms: int):
    """After executor finishes: sync ADF runs + inject Databricks run records directly."""
    try:
        from monitor_agent.deps import get_db, get_monitor
        monitor_svc = get_monitor()
        db          = get_db()

        # Always sync recent ADF runs (copy pipeline runs appear here)
        if monitor_svc:
            await monitor_svc.sync_historical(2)

        if not isinstance(result, dict):
            return

        run_id  = result.get("run_id", "")
        stages  = result.get("stages", [])
        status  = result.get("status", "failed")
        now     = datetime.datetime.now(datetime.timezone.utc)
        start_dt = now - datetime.timedelta(milliseconds=elapsed_ms)

        # Databricks-only runs have no ADF run_id — inject synthetic DB record
        if db and run_id.startswith("dbx-"):
            adf_status = "Succeeded" if status == "ok" else "Failed"
            message    = result.get("message", f"Stages: {', '.join(stages)}")
            run_record = {
                "runId":        run_id,
                "pipelineName": "Databricks_Notebook_Pipeline",
                "status":       adf_status,
                "runStart":     start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "runEnd":       now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "durationMs":   elapsed_ms,
                "message":      message,
            }
            await db.upsert_run(run_record)

            # Trigger AI analysis directly — skip _handle_completed_run
            # which would try to call the ADF API with a dbx- run_id
            if monitor_svc:
                activities = []
                stats = await db.get_historical_stats("Databricks_Notebook_Pipeline")
                asyncio.create_task(
                    monitor_svc._analyze(run_id, "Databricks_Notebook_Pipeline", run_record, activities, stats)
                )
    except Exception as e:
        print(f"[monitor notify] non-fatal: {e}")


@router.get("/status/{job_id}")
async def job_status(job_id: str):
    if job_id not in _jobs:
        # 410 Gone = job existed but server restarted (vs 404 = never existed)
        raise HTTPException(status_code=410, detail="Job session expired — server was restarted. Please re-run.")
    return _jobs[job_id]


@router.get("/jobs")
async def list_jobs():
    return [{"job_id": k, **{f: v for f, v in v.items() if f != "result"}} for k, v in _jobs.items()]
