import asyncio
from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from .groq_planner import decide_pipeline_config

router = APIRouter()


@router.post("/plan")
async def plan_pipeline(body: dict):
    """Accept {schema: {...}, prompt: "..."} and return AI-generated pipeline config."""
    schema = body.get("schema", {})
    prompt = body.get("prompt", "")
    config, used_fallback = await run_in_threadpool(decide_pipeline_config, schema, prompt)
    return {"config": config, "used_fallback": used_fallback}
