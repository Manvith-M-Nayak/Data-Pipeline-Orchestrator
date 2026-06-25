import asyncio
import json
import os
import tempfile
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
    """
    Upload a CSV + pipeline_config JSON + schema JSON.
    Returns a job_id to poll for status.
    """
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
        try:
            result = await run_in_threadpool(execute_pipeline, tmp.name, config_dict, schema_dict, _progress)
            if isinstance(result, dict) and result.get("status") in ("failed", "error"):
                _jobs[job_id].update({
                    "status": "failed",
                    "error":  result.get("message", "Pipeline failed"),
                    "result": result,
                })
            else:
                _jobs[job_id].update({"status": "completed", "step": "Complete", "result": result})
        except Exception as exc:
            _jobs[job_id].update({"status": "failed", "error": str(exc)})
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    asyncio.create_task(_run())
    return {"job_id": job_id, "status": "running"}


@router.get("/status/{job_id}")
async def job_status(job_id: str):
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return _jobs[job_id]


@router.get("/jobs")
async def list_jobs():
    return [{"job_id": k, **{f: v for f, v in v.items() if f != "result"}} for k, v in _jobs.items()]
