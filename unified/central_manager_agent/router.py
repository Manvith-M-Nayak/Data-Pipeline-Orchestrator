import asyncio
import json
import os
import tempfile
import time

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

    # The UI lets users edit execution_groups by hand — repair any data-flow
    # violations (a stage grouped with its dependency) before spending cloud
    # resources on a run that would read an empty container.
    try:
        from planner_agent.planner_common import sanitize_execution_groups
        config_dict = sanitize_execution_groups(config_dict)
    except Exception as exc:
        print(f"[manager] execution_groups sanitize skipped: {exc}")

    # Write the input to a temp dir keeping the original filename — it becomes
    # the blob name in the source container (executor needs a file path).
    # CSV and JSON (array-of-objects / NDJSON) are both supported; the file
    # extension carries the format through to the executor.
    orig_name = os.path.basename(csv_file.filename or "") or "input.csv"
    if not orig_name.lower().endswith((".csv", ".json", ".jsonl", ".ndjson")):
        # No usable extension — sniff content to pick one
        head = contents.lstrip()[:1]
        orig_name += ".json" if head in (b"{", b"[") else ".csv"
    tmp_dir  = tempfile.mkdtemp(prefix="manager_")
    tmp_path = os.path.join(tmp_dir, orig_name)
    with open(tmp_path, "wb") as f:
        f.write(contents)

    # Pre-create the RunState so client gets run_id before async work starts
    run_id = _manager.pre_create(config_dict)

    async def _task():
        start = time.time()
        try:
            await _manager.execute_run(run_id, tmp_path, schema_dict, csv_size, user_request)
            # Managed runs bypass the executor router — feed the monitor the
            # same completion record it would have received there.
            try:
                state = _manager.get_state_dict(run_id) or {}
                result = state.get("executor_result")
                if result:
                    from executor_agent.router import _notify_monitor
                    await _notify_monitor(result, int((time.time() - start) * 1000))
            except Exception as exc:
                print(f"[manager] monitor notify non-fatal: {exc}")
        finally:
            try:
                os.unlink(tmp_path)
                os.rmdir(tmp_dir)
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
