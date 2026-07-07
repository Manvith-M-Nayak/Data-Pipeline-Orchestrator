import asyncio
import datetime
import json
import os
import tempfile
import time
import uuid
from typing import Dict

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.concurrency import run_in_threadpool

router = APIRouter()

_jobs: Dict[str, Dict] = {}


@router.post("/run")
async def run_pipeline(
    csv_file:        UploadFile = File(...),
    pipeline_config: str        = Form(...),
    schema:          str        = Form(...),
):
    from .executor import execute_pipeline

    contents = await csv_file.read()
    # Keep the user's original filename — it becomes the blob name in the
    # source container (a NamedTemporaryFile would upload as "tmpXXXX.csv").
    orig_name = os.path.basename(csv_file.filename or "") or "input.csv"
    if not orig_name.lower().endswith(".csv"):
        orig_name += ".csv"
    tmp_dir  = tempfile.mkdtemp(prefix="orchestrator_")
    tmp_path = os.path.join(tmp_dir, orig_name)
    with open(tmp_path, "wb") as f:
        f.write(contents)

    def _cleanup():
        try:
            os.unlink(tmp_path)
            os.rmdir(tmp_dir)
        except OSError:
            pass

    try:
        config_dict = json.loads(pipeline_config)
        schema_dict = json.loads(schema)
    except json.JSONDecodeError as e:
        _cleanup()
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {e}")

    # The UI lets users edit execution_groups by hand — repair any data-flow
    # violations (a stage grouped with its dependency) before spending cloud
    # resources on a run that would read an empty container.
    try:
        from planner_agent.planner_common import sanitize_execution_groups
        config_dict = sanitize_execution_groups(config_dict)
    except Exception as e:
        print(f"[executor] execution_groups sanitize skipped: {e}")

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "running", "step": "Starting…", "result": None, "error": None}

    def _progress(step_name: str, dbx_run_id: int = None):
        _jobs[job_id]["step"] = step_name
        if dbx_run_id is not None:
            _jobs[job_id]["dbx_run_id"] = dbx_run_id

    async def _run():
        start = time.time()
        try:
            result = await run_in_threadpool(execute_pipeline, tmp_path, config_dict, schema_dict, _progress)
            elapsed_ms = int((time.time() - start) * 1000)
            if isinstance(result, dict) and result.get("status") in ("failed", "error"):
                _jobs[job_id].update({
                    "status": "failed",
                    "error":  result.get("message", "Pipeline failed"),
                    "result": result,
                })
            else:
                _jobs[job_id].update({"status": "completed", "step": "Complete", "result": result})
            asyncio.create_task(_notify_monitor(result, elapsed_ms))
        except Exception as exc:
            _jobs[job_id].update({"status": "failed", "error": str(exc)})
        finally:
            _cleanup()

    asyncio.create_task(_run())
    return {"job_id": job_id, "status": "running"}


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
