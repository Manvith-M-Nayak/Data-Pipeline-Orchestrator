import datetime

from fastapi import APIRouter, HTTPException

from background import spawn

router = APIRouter()

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

        # Databricks runs have no ADF record — inject a synthetic one under the
        # executor's dbx- id. This happens whenever compute stages ran, even if
        # the plan ALSO had an ADF copy stage (whose id is the result's run_id).
        # Streaming runs get their own pipeline name so the monitor keeps a
        # separate duration baseline (incremental triggers ≠ batch runs).
        mode = (result.get("mode") or "batch").lower()
        pipeline_name = (
            "Databricks_Streaming_Pipeline"
            if mode == "streaming"
            else "Databricks_Notebook_Pipeline"
        )
        dbx_id = result.get("dbx_run_id") or (run_id if run_id.startswith("dbx-") else None)
        if db and dbx_id:
            run_id     = dbx_id
            adf_status = "Succeeded" if status == "ok" else "Failed"
            message    = result.get("message", f"[{mode}] Stages: {', '.join(stages)}")
            run_record = {
                "runId":        run_id,
                "pipelineName": pipeline_name,
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
                stats = await db.get_historical_stats(pipeline_name)
                spawn(
                    monitor_svc._analyze(run_id, pipeline_name, run_record, activities, stats),
                    name=f"executor.analyze:{run_id}",
                )
    except Exception as e:
        print(f"[monitor notify] non-fatal: {e}")
