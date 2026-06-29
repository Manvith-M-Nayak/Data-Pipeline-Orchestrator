import asyncio
from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from . import decide_pipeline_config

router = APIRouter()


@router.post("/plan")
async def plan_pipeline(body: dict):
    """Accept {schema: {...}, prompt: "..."} and return AI-generated pipeline config."""
    raw    = body.get("schema", {})
    prompt = body.get("prompt", "")

    # Normalize schema from /api/schema/detect format → groq_planner format.
    # detect returns: {columns: {col: type}, preview: [...], row_count_sample: N}
    # groq_planner expects: {columns: [col,...], inferred_types: {col: type},
    #                        row_count: N, size_hint: str, samples: [...]}
    cols_dict = raw.get("columns", {})
    if isinstance(cols_dict, dict):
        col_names = list(cols_dict.keys())
        inferred  = cols_dict
    else:
        col_names = cols_dict
        inferred  = raw.get("inferred_types", {})

    schema = {
        "columns":       col_names,
        "inferred_types": inferred,
        "row_count":     raw.get("row_count") or raw.get("row_count_sample", 0),
        "size_hint":     raw.get("size_hint", "medium"),
        "samples":       raw.get("preview") or raw.get("samples", []),
    }

    config, used_fallback = await run_in_threadpool(decide_pipeline_config, schema, prompt)
    return {"config": config, "used_fallback": used_fallback}
