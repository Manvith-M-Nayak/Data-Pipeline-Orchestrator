"""
FastAPI endpoints for the Learning and Policy Update Agent.

Registered in main.py at /api/learning/ :

  GET  /api/learning/status      full status (metrics, policies, retrain state)
  GET  /api/learning/metrics     just the aggregate error metrics
  GET  /api/learning/policies    current policies
  GET  /api/learning/log         improvement log (audit trail)
  GET  /api/learning/versions    rollback-able snapshots
  GET  /api/learning/resource-drift  Resource Agent's own correction drift check
  POST /api/learning/cycle       run a learning cycle now
  POST /api/learning/retrain     force a retrain (sync=false → background)
  POST /api/learning/rollback    restore a snapshot by version_id
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .learning_agent import get_learning_agent

router = APIRouter()

# Shared singleton — same instance the Central Manager's hook uses
agent = get_learning_agent()


class RollbackRequest(BaseModel):
    version_id: str


class CycleRequest(BaseModel):
    background_retrain: bool = True


class RetrainRequest(BaseModel):
    sync: bool = False


@router.get("/status")
def status():
    return agent.get_status()


@router.get("/metrics")
def metrics():
    records = agent.collector.load_records()
    return agent.analyzer.analyze(records)


@router.get("/policies")
def policies():
    return agent.policies.load()


@router.get("/log")
def log(limit: int = 100):
    return {"entries": agent.get_log(limit)}


@router.get("/versions")
def versions():
    return {"versions": agent.safety.list_versions()}


@router.get("/resource-drift")
def resource_drift():
    return {"flags": agent.policies.check_resource_agent_drift()}


@router.post("/cycle")
def run_cycle(body: CycleRequest = CycleRequest()):
    return agent.run_cycle(background_retrain=body.background_retrain)


@router.post("/retrain")
def force_retrain(body: RetrainRequest = RetrainRequest()):
    records = agent.collector.load_records()
    if body.sync:
        return agent.retrainer.retrain_sync(records)
    return agent.retrainer.retrain_async(records)


@router.post("/rollback")
def rollback(body: RollbackRequest):
    try:
        return agent.rollback(body.version_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))