"""
FastAPI router for the Assurance Agent. Mounted at /api/assurance in main.py.

POST /api/assurance/validate
  body: {
    "request": "<original user request>",
    "plan":    { ...generated plan... },   # dict (or JSON string)
    "schema":  { "columns": [...], ... },   # optional; falls back to config template
    "run_semantic": true,                   # optional, default true
    "block_on_intent": false                # optional, default false
  }
  returns: the full AssuranceResult.to_dict() (overall_status, summary,
           structural_results, semantic_result, and the three render tiers).
"""

from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool

from .config_loader import load_schema
from .orchestrator import AssuranceAgent

router = APIRouter()


@router.post("/validate")
async def validate_plan(body: dict):
    user_request = body.get("request", "")
    plan         = body.get("plan", {})
    schema       = body.get("schema") or load_schema()
    run_semantic = body.get("run_semantic", True)
    block        = body.get("block_on_intent", False)

    agent = AssuranceAgent(semantic_blocks_overall=block)
    # assure() may call Ollama (blocking requests) → run off the event loop.
    result = await run_in_threadpool(
        agent.assure, user_request, plan, schema, run_semantic,
    )
    return result.to_dict()


@router.get("/health")
async def health():
    return {"status": "ok", "agent": "assurance", "layers": ["structural", "semantic"]}
