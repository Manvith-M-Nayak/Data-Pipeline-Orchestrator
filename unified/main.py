"""
Unified backend for all three agents:
  - planner_agent  → /api/planner/*
  - executor_agent → /api/executor/*
  - monitor_agent  → /api/monitor/pipelines/*, /api/monitor/logs/*, etc.

Reads credentials from config.py and bridges them into environment variables
so that monitor_agent services (which use os.getenv) pick them up seamlessly.
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# ── Bridge config.py → environment before any service reads os.getenv ──────
try:
    import config as _cfg
    _BRIDGE = {
        "AZURE_TENANT_ID":       getattr(_cfg, "AZURE_TENANT_ID",       ""),
        "AZURE_CLIENT_ID":       getattr(_cfg, "AZURE_CLIENT_ID",       ""),
        "AZURE_CLIENT_SECRET":   getattr(_cfg, "AZURE_CLIENT_SECRET",   ""),
        "AZURE_SUBSCRIPTION_ID": getattr(_cfg, "AZURE_SUBSCRIPTION_ID", ""),
        "AZURE_RESOURCE_GROUP":  getattr(_cfg, "AZURE_RESOURCE_GROUP",  ""),
        "ADF_FACTORY_NAME":      getattr(_cfg, "AZURE_DATA_FACTORY",    ""),
        "GROQ_API_KEY":          getattr(_cfg, "GROQ_API_KEY",          ""),
    }
    for k, v in _BRIDGE.items():
        if v:
            os.environ.setdefault(k, str(v))
except ImportError:
    pass   # config.py not present — rely on actual environment variables

# ── Service imports (after env bridge) ─────────────────────────────────────
from monitor_agent.services.adf_service     import ADFService
from monitor_agent.services.db_service      import DBService
from monitor_agent.services.groq_service    import GroqService
from monitor_agent.services.monitor_service import MonitorService
import monitor_agent.deps as _deps

from monitor_agent.routers import pipelines as mon_pipelines
from monitor_agent.routers import logs      as mon_logs
from monitor_agent.routers import predictions as mon_predictions
from monitor_agent.routers import anomalies as mon_anomalies

from planner_agent.router  import router as planner_router
from executor_agent.router import router as executor_router

# ── Service singletons ──────────────────────────────────────────────────────
db_service      = DBService()
adf_service     = ADFService()
groq_service    = GroqService()
monitor_service = MonitorService(adf_service, db_service, groq_service)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _deps.init(adf_service, db_service, groq_service, monitor_service)
    await db_service.initialize()
    asyncio.create_task(monitor_service.start_polling())
    asyncio.create_task(monitor_service.backfill_missing_analyses(limit=75))
    yield


app = FastAPI(title="Unified Agent Backend", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ─────────────────────────────────────────────────────────────────
app.include_router(planner_router,          prefix="/api/planner",                     tags=["planner"])
app.include_router(executor_router,         prefix="/api/executor",                    tags=["executor"])
app.include_router(mon_pipelines.router,    prefix="/api/monitor/pipelines",           tags=["monitor-pipelines"])
app.include_router(mon_logs.router,         prefix="/api/monitor/logs",                tags=["monitor-logs"])
app.include_router(mon_predictions.router,  prefix="/api/monitor/predictions",         tags=["monitor-predictions"])
app.include_router(mon_anomalies.router,    prefix="/api/monitor/anomalies",           tags=["monitor-anomalies"])


@app.get("/api/health")
async def health():
    return {"status": "ok", "agents": ["planner", "executor", "monitor"]}


@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket):
    await websocket.accept()
    monitor_service.ws_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        monitor_service.ws_clients.discard(websocket)
