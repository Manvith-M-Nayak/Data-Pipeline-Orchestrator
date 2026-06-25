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
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp.write(contents)
    tmp.close()

    try:
        config_dict = json.loads(pipeline_config)
        schema_dict = json.loads(schema)
    except json.JSONDecodeError as e:
        os.unlink(tmp.name)
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {e}")

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "running", "step": "Starting…", "result": None, "error": None}

    def _progress(step_name: str):
        _jobs[job_id]["step"] = step_name

    async def _run():
        start = time.time()
        try:
            result = await run_in_threadpool(execute_pipeline, tmp.name, config_dict, schema_dict, _progress)
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
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    asyncio.create_task(_run())
    return {"job_id": job_id, "status": "running"}


async def _notify_monitor(result: dict, elapsed_ms: int):
    """After executor finishes: sync ADF runs into monitor + inject Databricks-only runs."""
    try:
        from monitor_agent.deps import get_db, get_monitor
        monitor = get_monitor()
        db      = get_db()

        # Always sync recent ADF runs so copy-pipeline runs appear in monitor
        if monitor:
            await monitor.sync_historical(2)

        # For Databricks-only runs (no ADF), inject a synthetic record so it
        # appears in Run Logs and Predictions
        if db and isinstance(result, dict) and result.get("status") == "ok":
            run_id = result.get("run_id", "")
            if run_id.startswith("dbx-"):
                now = datetime.datetime.now(datetime.timezone.utc)
                start_dt = now - datetime.timedelta(milliseconds=elapsed_ms)
                stages = result.get("stages", [])
                await db.upsert_run({
                    "runId":        run_id,
                    "pipelineName": "Databricks_Notebook_Pipeline",
                    "status":       "Succeeded",
                    "runStart":     start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "runEnd":       now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "durationMs":   elapsed_ms,
                    "message":      f"Stages: {', '.join(stages)}",
                })
                # Trigger AI analysis for this synthetic run
                if monitor:
                    fake_run = {
                        "runId": run_id, "pipelineName": "Databricks_Notebook_Pipeline",
                        "status": "Succeeded", "durationMs": elapsed_ms, "message": "",
                    }
                    asyncio.create_task(monitor._handle_completed_run(fake_run))
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
