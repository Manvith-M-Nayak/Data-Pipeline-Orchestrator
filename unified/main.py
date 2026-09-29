"""
Unified backend for all three agents:
  - planner_agent  → /api/planner/*
  - executor_agent → /api/executor/*
  - monitor_agent  → /api/monitor/pipelines/*, /api/monitor/logs/*, etc.

Reads credentials from config.py and bridges them into environment variables
so that monitor_agent services (which use os.getenv) pick them up seamlessly.
"""

import asyncio
import csv
import io
import json
import os
import re
from contextlib import asynccontextmanager

# Load a .env file (if present) into the environment BEFORE any service reads
# os.getenv — this is the preferred secret store over the plaintext config.py.
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # python-dotenv optional; fall back to real env vars / config.py

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware

from app_security import APIKeyMiddleware, read_upload_capped, websocket_allowed
from background import spawn
from performance_prediction_agent.router import router as perf_router
from learning_policy_agent.router import router as learning_router





# ── Bridge config.py → environment before any service reads os.getenv ──────
try:
    import config as _cfg

    _BRIDGE = {
        "AZURE_TENANT_ID": getattr(_cfg, "AZURE_TENANT_ID", ""),
        "AZURE_CLIENT_ID": getattr(_cfg, "AZURE_CLIENT_ID", ""),
        "AZURE_CLIENT_SECRET": getattr(_cfg, "AZURE_CLIENT_SECRET", ""),
        "AZURE_SUBSCRIPTION_ID": getattr(_cfg, "AZURE_SUBSCRIPTION_ID", ""),
        "AZURE_RESOURCE_GROUP": getattr(_cfg, "AZURE_RESOURCE_GROUP", ""),
        "ADF_FACTORY_NAME": getattr(_cfg, "AZURE_DATA_FACTORY", ""),
        "GROQ_API_KEY": getattr(_cfg, "GROQ_API_KEY", ""),
        "PLANNER_BACKEND": getattr(_cfg, "PLANNER_BACKEND", "ollama"),
        "OLLAMA_HOST": getattr(_cfg, "OLLAMA_HOST", "http://localhost:11434"),
        "PLANNER_MODEL": getattr(_cfg, "PLANNER_MODEL", "planner-agent"),
    }
    for k, v in _BRIDGE.items():
        if v:
            os.environ.setdefault(k, str(v))
except ImportError:
    pass  # config.py not present — rely on actual environment variables

# ── Service imports (after env bridge) ─────────────────────────────────────
from monitor_agent.services.adf_service import ADFService
from monitor_agent.services.db_service import DBService
from monitor_agent.services.groq_service import GroqService
from monitor_agent.services.monitor_service import MonitorService
import monitor_agent.deps as _deps

from monitor_agent.routers import pipelines as mon_pipelines
from monitor_agent.routers import logs as mon_logs
from monitor_agent.routers import predictions as mon_predictions
from monitor_agent.routers import anomalies as mon_anomalies

from planner_agent.router import router as planner_router
from planner_agent.ollama_launcher import ensure_ollama
from executor_agent.router import router as executor_router
from central_manager_agent.router import router as manager_router
from resource_agent.router import router as resource_router
from assurance_agent.router import router as assurance_router
from cost_optimization_agent.router import router as cost_optimizer_router
from central_manager_agent.combined import router as combined_router

# ── Service singletons ──────────────────────────────────────────────────────
db_service = DBService()
adf_service = ADFService()
groq_service = GroqService()
monitor_service = MonitorService(adf_service, db_service, groq_service)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _deps.init(adf_service, db_service, groq_service, monitor_service)
    await db_service.initialize()
    # A run left mid-flight by the previous process can never finish — mark
    # those persisted runs failed so the UI shows a terminal state, not a spinner.
    try:
        n = await db_service.mark_interrupted_manager_runs()
        if n:
            print(f"[startup] marked {n} interrupted manager run(s) as failed")
    except Exception as exc:
        print(f"[startup] interrupted-run sweep skipped: {exc}")
    tasks = [
        spawn(ensure_ollama(), name="ollama.ensure"),
        spawn(monitor_service.start_polling(), name="monitor.start_polling"),
        spawn(
            monitor_service.backfill_missing_analyses(limit=75),
            name="monitor.backfill_missing_analyses",
        ),
    ]
    yield
    # ── Shutdown: stop the loops we started, close live sockets ─────────────
    # (Ollama is deliberately left running — see planner_agent/ollama_launcher.)
    from central_manager_agent.stream_manager import stream_manager

    await stream_manager.shutdown()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await monitor_service.close_clients()


app = FastAPI(title="Unified Agent Backend", version="1.0.0", lifespan=lifespan)

# ── Auth ─────────────────────────────────────────────────────────────────────
# Always installed. With API_KEY set, every request needs a matching x-api-key;
# without it, only loopback clients are served. Added before CORS so CORS
# remains the outermost layer (handles preflight OPTIONS).
_API_KEY = os.getenv("API_KEY", "").strip()
app.add_middleware(APIKeyMiddleware, api_key=_API_KEY)
if not _API_KEY:
    print(
        "[security] API_KEY not set — serving loopback (localhost) clients only. "
        "Set API_KEY to allow remote clients with an x-api-key header."
    )

# ── CORS ─────────────────────────────────────────────────────────────────────
_allowed_origins = [
    o.strip()
    for o in os.getenv(
        "ALLOWED_ORIGINS", "http://localhost:5173,http://localhost:3000"
    ).split(",")
    if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Containers the download endpoint is permitted to read (empty = unrestricted).
_DOWNLOAD_ALLOWLIST = [
    c.strip()
    for c in os.getenv("DOWNLOAD_CONTAINER_ALLOWLIST", "").split(",")
    if c.strip()
]

# ── Routers ─────────────────────────────────────────────────────────────────
app.include_router(planner_router, prefix="/api/planner", tags=["planner"])
app.include_router(executor_router, prefix="/api/executor", tags=["executor"])
app.include_router(manager_router, prefix="/api/manager", tags=["manager"])
app.include_router(resource_router, prefix="/api/resource", tags=["resource"])
app.include_router(assurance_router, prefix="/api/assurance", tags=["assurance"])
app.include_router(
    perf_router, prefix="/api/performance-prediction", tags=["performance-prediction"]
)
app.include_router(
    cost_optimizer_router, prefix="/api/cost-optimization", tags=["cost-optimization"]
)
app.include_router(
    mon_pipelines.router, prefix="/api/monitor/pipelines", tags=["monitor-pipelines"]
)
app.include_router(mon_logs.router, prefix="/api/monitor/logs", tags=["monitor-logs"])
app.include_router(
    mon_predictions.router,
    prefix="/api/monitor/predictions",
    tags=["monitor-predictions"],
)
app.include_router(
    mon_anomalies.router, prefix="/api/monitor/anomalies", tags=["monitor-anomalies"]
)
app.include_router(learning_router, prefix="/api/learning", tags=["learning"])
app.include_router(combined_router, prefix="/api/manager/combined", tags=["combined"])

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "agents": [
            "planner",
            "assurance",
            "executor",
            "monitor",
            "central_manager",
            "resource",
            "cost_optimization",
        ],
    }


# ── Schema detection ────────────────────────────────────────────────────────
def _is_int(v: str) -> bool:
    try:
        int(v)
        return True
    except ValueError:
        return False


def _is_float(v: str) -> bool:
    try:
        float(v)
        return True
    except ValueError:
        return False


def _infer_type(values: list) -> str:
    non_empty = [v.strip() for v in values if v.strip()]
    if not non_empty:
        return "string"
    n = len(non_empty)
    if sum(_is_int(v) for v in non_empty) / n > 0.8:
        return "integer"
    if sum(_is_float(v) for v in non_empty) / n > 0.8:
        return "double"
    return "string"


def _infer_json_type(values: list) -> str:
    """Infer a column type from native JSON values (already typed)."""
    non_null = [v for v in values if v is not None and v != ""]
    if not non_null:
        return "string"
    n = len(non_null)
    # bool is a subclass of int — exclude it explicitly
    ints = sum(isinstance(v, int) and not isinstance(v, bool) for v in non_null)
    floats = sum(isinstance(v, float) for v in non_null)
    if ints / n > 0.8:
        return "integer"
    if (ints + floats) / n > 0.8:
        return "double"
    # Numeric strings ("42", "3.14") get the same treatment as CSV cells
    strs = [str(v) for v in non_null if isinstance(v, str)]
    if len(strs) == n:
        return _infer_type(strs)
    return "string"


def detect_file_format(filename: str, text: str) -> str:
    """'json' or 'csv', by extension first, content sniff as fallback."""
    name = (filename or "").lower()
    if name.endswith(".json") or name.endswith(".jsonl") or name.endswith(".ndjson"):
        return "json"
    if name.endswith(".csv"):
        return "csv"
    head = text.lstrip()[:1]
    return "json" if head in ("{", "[") else "csv"


def _parse_json_rows(text: str, sample_limit: int = 200):
    """Parse a JSON array-of-objects, a single object, or NDJSON.

    Returns (sample_rows, row_count). Raises ValueError if nothing parses.
    """
    stripped = text.strip()
    if not stripped:
        return [], 0
    # Whole-document parse: array of objects or single object
    try:
        doc = json.loads(stripped)
        if isinstance(doc, dict):
            doc = [doc]
        if isinstance(doc, list):
            rows = [r for r in doc if isinstance(r, dict)]
            if not rows and doc:
                raise ValueError("JSON array does not contain objects")
            return rows[:sample_limit], len(rows)
        raise ValueError("JSON root must be an object or an array of objects")
    except json.JSONDecodeError:
        pass
    # NDJSON: one object per line
    sample, row_count = [], 0
    for line in io.StringIO(stripped):
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)  # raise on first malformed line
        if not isinstance(row, dict):
            raise ValueError("NDJSON lines must be JSON objects")
        row_count += 1
        if len(sample) < sample_limit:
            sample.append(row)
    return sample, row_count


@app.post("/api/schema/detect", tags=["schema"])
async def detect_schema(csv_file: UploadFile = File(...)):
    contents = await read_upload_capped(csv_file)
    size = len(contents)
    size_hint = (
        "small (< 5MB)"    if size < 5_242_880   else
        "medium (5–50MB)"  if size < 52_428_800  else
        "large (50–200MB)" if size < 209_715_200 else
        "xlarge (> 200MB)"
    )
    text = contents.decode("utf-8", errors="replace")
    file_format = detect_file_format(csv_file.filename, text)

    if file_format == "json":
        try:
            sample, row_count = _parse_json_rows(text)
        except (ValueError, json.JSONDecodeError) as exc:
            from fastapi import HTTPException

            raise HTTPException(status_code=422, detail=f"Invalid JSON file: {exc}")
        # Union of keys across the sample — JSON rows may be sparse
        headers: list = []
        for r in sample:
            for k in r.keys():
                if k not in headers:
                    headers.append(k)
        columns = {
            col: _infer_json_type([r.get(col) for r in sample]) for col in headers
        }
    else:
        reader = csv.DictReader(io.StringIO(text))
        # Count every row; keep only the first 200 in memory for type inference.
        sample, row_count = [], 0
        for row in reader:
            row_count += 1
            if len(sample) < 200:
                sample.append(row)
        headers = list(sample[0].keys()) if sample else []
        columns = {
            col: _infer_type([r.get(col, "") for r in sample]) for col in headers
        }

    if not sample:
        return {
            "columns": {},
            "preview": [],
            "row_count": 0,
            "row_count_sample": 0,
            "column_count": 0,
            "size_hint": size_hint,
            "file_format": file_format,
        }

    preview = [{k: r.get(k, "") for k in headers} for r in sample[:6]]

    return {
        "columns": columns,
        "preview": preview,
        "row_count": row_count,
        "row_count_sample": len(sample),
        "column_count": len(headers),
        "size_hint": size_hint,
        "file_format": file_format,
    }


@app.get("/api/executor/download/{container}", tags=["executor"])
async def download_output(container: str):
    """Return the pipeline output for a sink container.

    Batch writes a single output file; streaming appends one part file per
    trigger. This merges ALL output blobs into one response so the caller gets
    the complete result, not just the first (oldest) part — CSV keeps a single
    header, JSON merges into one array.
    """
    from fastapi.responses import Response
    from azure.storage.blob import BlobServiceClient
    from fastapi import HTTPException
    import settings

    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?", container):
        raise HTTPException(status_code=400, detail="Invalid container name")
    if _DOWNLOAD_ALLOWLIST and container not in _DOWNLOAD_ALLOWLIST:
        raise HTTPException(
            status_code=403, detail=f"Container '{container}' is not downloadable"
        )

    conn = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={settings.get('AZURE_STORAGE_ACCOUNT')};"
        f"AccountKey={settings.get('AZURE_STORAGE_KEY')};"
        f"EndpointSuffix=core.windows.net"
    )
    client = BlobServiceClient.from_connection_string(conn)
    container_client = client.get_container_client(container)

    # Collect every output data blob, prefer the "output/" prefix the notebooks
    # write to. Sorted by name so part files apply in a stable order.
    exts = (".csv", ".json", ".jsonl", ".ndjson")
    blobs = [
        b.name
        for b in container_client.list_blobs()
        if b.name.lower().endswith(exts) and b.size and b.size > 0
    ]
    output_blobs = sorted(n for n in blobs if n.lower().startswith("output/"))
    targets = output_blobs or sorted(blobs)  # fall back to any data blob

    if not targets:
        raise HTTPException(
            status_code=404, detail=f"No output CSV/JSON found in '{container}'"
        )

    ext = os.path.splitext(targets[0])[1].lower() or ".csv"
    is_json = ext in (".json", ".jsonl", ".ndjson")

    def _read(name: str) -> str:
        return container_client.get_blob_client(name).download_blob().readall().decode(
            "utf-8", errors="replace"
        )

    parts_used = 0
    if is_json:
        merged: list = []
        for name in targets:
            try:
                doc = json.loads(_read(name))
            except json.JSONDecodeError:
                continue
            merged.extend(doc if isinstance(doc, list) else [doc])
            parts_used += 1
        body = json.dumps(merged, indent=2).encode("utf-8")
        media_type = "application/json"
    else:
        # CSV: keep the first file's header, append only data rows from the rest.
        # Parts whose header differs belong to a DIFFERENT dataset (a container
        # that was reused across pipelines) — skip them rather than emit a
        # corrupt file with a stray header row mid-stream.
        lines: list = []
        header = None
        for name in targets:
            rows = _read(name).lstrip("﻿").splitlines()
            if not rows:
                continue
            if header is None:
                header = rows[0]
                lines.append(header)
                lines.extend(r for r in rows[1:] if r != "")
                parts_used += 1
            elif rows[0] == header:
                lines.extend(r for r in rows[1:] if r != "")
                parts_used += 1
            # else: schema mismatch — different dataset, skip this part.
        body = ("\n".join(lines) + "\n").encode("utf-8")
        media_type = "text/csv"

    filename = f"{container}-output{ext}"
    return Response(
        content=body,
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Output-Parts": str(parts_used),
        },
    )


@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket):
    if not websocket_allowed(websocket, _API_KEY):
        await websocket.close(code=1008)
        return
    await websocket.accept()
    monitor_service.ws_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        monitor_service.ws_clients.discard(websocket)
