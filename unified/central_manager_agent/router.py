import json
import os
import tempfile
import time

from fastapi import APIRouter, UploadFile, File, Form, HTTPException

from background import spawn
from .manager import CentralManager

router = APIRouter()
_manager = CentralManager()


@router.post("/run")
async def start_managed_run(
    csv_file:        UploadFile = File(...),
    pipeline_config: str        = Form(...),
    # Named input_schema (form field still "schema"): a field called `schema`
    # shadows pydantic BaseModel.schema in the generated body model.
    input_schema:    str        = Form(..., alias="schema"),
    user_request:    str        = Form(""),
):
    """
    Kick off a fully-managed pipeline run.
    Returns run_id immediately; client polls /status/{run_id}.
    """
    from app_security import read_upload_capped

    contents = await read_upload_capped(csv_file)
    csv_size = len(contents)

    try:
        config_dict = json.loads(pipeline_config)
        schema_dict = json.loads(input_schema)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {exc}") from exc

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
                elapsed_ms = int((time.time() - start) * 1000)
                if result:
                    from executor_agent.router import _notify_monitor
                    await _notify_monitor(result, elapsed_ms)
                # Real-time anomaly classification — runs for EVERY finished
                # run (also failures with no executor result) and persists
                # detected kinds to the anomaly_events table.
                try:
                    from monitor_agent.services.anomaly_detector import detect_and_store
                    await detect_and_store(state, result, elapsed_ms, schema_dict)
                except Exception as exc:
                    print(f"[manager] anomaly detect non-fatal: {exc}")
            except Exception as exc:
                print(f"[manager] monitor notify non-fatal: {exc}")
        finally:
            try:
                os.unlink(tmp_path)
                os.rmdir(tmp_dir)
            except OSError:
                pass

    spawn(_task(), name=f"manager.run:{run_id}")
    return {"run_id": run_id, "status": "started"}


@router.get("/status/{run_id}")
async def run_status(run_id: str):
    # In-memory is freshest for the live run; fall back to sqlite for runs from
    # before a restart (otherwise the frontend polling loop 404s on resume).
    state = _manager.get_state_dict(run_id)
    if state is None:
        from monitor_agent import deps

        db = deps.get_db()
        if db is not None:
            state = await db.get_manager_run(run_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return state


@router.get("/runs")
async def list_runs():
    # Merge live (in-memory) runs with persisted ones; memory wins on conflict.
    runs = _manager.list_runs()
    seen = {r["run_id"] for r in runs}
    from monitor_agent import deps

    db = deps.get_db()
    if db is not None:
        for r in await db.list_manager_runs():
            if r["run_id"] not in seen:
                runs.append(r)
    return sorted(runs, key=lambda x: x.get("started_at") or "", reverse=True)


@router.get("/feedback")
async def feedback_history():
    return _manager.get_feedback_history()


# ── Streaming console ─────────────────────────────────────────────────────────
# A streaming pipeline runs incrementally (availableNow). These endpoints keep it
# "live": drop data → process now, or auto-poll every interval_s for new data.
from .stream_manager import stream_manager


@router.post("/stream/start")
async def stream_start(body: dict):
    """Register a live stream. body: {config, schema, file_format?, interval_s?}."""
    config = body.get("config") or {}
    schema = body.get("schema") or {}
    file_format = body.get("file_format") or "csv"
    interval_s = body.get("interval_s") or 0
    try:
        return await stream_manager.start(config, schema, file_format, interval_s)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/stream/{stream_id}/data")
async def stream_add_data(stream_id: str, csv_file: UploadFile = File(...)):
    """Drop new data into a running stream's source and process it immediately."""
    from app_security import read_upload_capped

    data = await read_upload_capped(csv_file)
    if not data or not data.strip():
        raise HTTPException(status_code=422, detail="Dropped file is empty — nothing to process")
    try:
        return await stream_manager.add_data(stream_id, data, csv_file.filename or "data.csv")
    except KeyError:
        raise HTTPException(status_code=404, detail="Stream not found") from None


@router.post("/stream/{stream_id}/tick")
async def stream_tick(stream_id: str):
    """Manually run one incremental pass (process whatever new data is in source)."""
    try:
        return await stream_manager.tick(stream_id, reason="manual")
    except KeyError:
        raise HTTPException(status_code=404, detail="Stream not found") from None


@router.post("/stream/{stream_id}/stop")
async def stream_stop(stream_id: str):
    try:
        return await stream_manager.stop(stream_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Stream not found") from None


@router.get("/stream/list")
async def stream_list():
    return stream_manager.list()


@router.get("/stream/{stream_id}")
async def stream_get(stream_id: str):
    try:
        return stream_manager.get(stream_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Stream not found") from None


@router.get("/stream/{stream_id}/output")
async def stream_output(stream_id: str, limit: int = 200):
    try:
        return stream_manager.output_preview(stream_id, limit)
    except KeyError:
        raise HTTPException(status_code=404, detail="Stream not found") from None
