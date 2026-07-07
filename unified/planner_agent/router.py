import asyncio
from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from . import decide_pipeline_config
from .planner_common import sanitize_execution_groups

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

    # Optional user overrides — forwarded to the backend so the user can pick
    # stage count, container names, and resource settings (diu/workers/etc.).
    num_containers   = body.get("num_containers")
    custom_settings  = body.get("custom_settings")
    container_names  = body.get("container_names")
    execution_groups = body.get("execution_groups")
    if num_containers is not None:
        try:
            num_containers = int(num_containers)
        except (TypeError, ValueError):
            num_containers = None
    if not isinstance(custom_settings, dict):
        custom_settings = None
    if not isinstance(container_names, list):
        container_names = None

    config, used_fallback = await run_in_threadpool(
        decide_pipeline_config, schema, prompt,
        num_containers, custom_settings, container_names,
    )

    # User-requested concurrency plan overrides whatever the model produced;
    # sanitize_execution_groups repairs any data-dependency violations.
    if isinstance(execution_groups, list) and execution_groups:
        config = sanitize_execution_groups(config, execution_groups)

    return {"config": config, "used_fallback": used_fallback}
