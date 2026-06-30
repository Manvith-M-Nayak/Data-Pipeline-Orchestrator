import asyncio
import json
import os
import tempfile

from fastapi import APIRouter, UploadFile, File, Form, HTTPException

from .manager import CentralManager

router = APIRouter()
_manager = CentralManager()


@router.post("/run")
async def start_managed_run(
    csv_file:        UploadFile = File(...),
    pipeline_config: str        = Form(...),
    schema:          str        = Form(...),
    user_request:    str        = Form(""),
):
    """
    Kick off a fully-managed pipeline run.
    Returns run_id immediately; client polls /status/{run_id}.
    """
    contents = await csv_file.read()
    csv_size = len(contents)

    try:
        config_dict = json.loads(pipeline_config)
        schema_dict = json.loads(schema)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {exc}")

    # Write CSV to temp file — executor needs a file path
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp.write(contents)
    tmp.close()

    # Pre-create the RunState so client gets run_id before async work starts
    run_id = _manager.pre_create(config_dict)

    async def _task():
        try:
            await _manager.execute_run(run_id, tmp.name, schema_dict, csv_size, user_request)
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    asyncio.create_task(_task())
    return {"run_id": run_id, "status": "started"}


@router.get("/status/{run_id}")
async def run_status(run_id: str):
    state = _manager.get_state_dict(run_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return state


@router.get("/runs")
async def list_runs():
    return _manager.list_runs()


@router.get("/feedback")
async def feedback_history():
    return _manager.get_feedback_history()
