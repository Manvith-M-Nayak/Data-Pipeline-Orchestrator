from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from . import decide_pipeline_config
from .planner_common import sanitize_execution_groups, to_streaming_plan
from .self_check import plan_with_verification

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

    mode = (body.get("mode") or "batch").lower()

    def _build(review_feedback=None):
        config, used_fallback = decide_pipeline_config(
            schema, prompt, num_containers, custom_settings, container_names,
            review_feedback=review_feedback,
        )
        # User-requested concurrency plan overrides whatever the model produced;
        # sanitize_execution_groups repairs any data-dependency violations.
        if isinstance(execution_groups, list) and execution_groups:
            config = sanitize_execution_groups(config, execution_groups)
        # Streaming mode: reshape the batch plan into a single incremental
        # stream stage, reusing the transforms/filter the model extracted.
        if mode == "streaming":
            config = to_streaming_plan(config, container_names)
        return config, used_fallback

    # The planner verifies its own plan (structural rules + intent check from
    # the assurance library) and replans once with the problems as feedback,
    # so the user gets a checked plan instead of one a later step contradicts.
    config, used_fallback, verification = await run_in_threadpool(
        plan_with_verification, _build, schema, prompt,
    )

    # Also inside the config: the plan travels alone to the Manager, whose
    # validate_plan warning and feedback log read plan["used_fallback"].
    config["used_fallback"] = bool(used_fallback)
    return {"config": config, "used_fallback": used_fallback, "verification": verification}
