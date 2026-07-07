"""
Ollama planner — serves the fine-tuned Qwen2.5-7B planner adapter locally
(via Ollama) in place of the Groq cloud LLM.

The model was fine-tuned on a fixed contract:
  system  : SYSTEM_PROMPT below (also baked into the Modelfile)
  user    : JSON string {"schema": {...}, "user_prompt": "..."}
  output  : a single compact JSON pipeline config (no prose)

We send that exact contract to Ollama, then reuse the structural validation
and deterministic fallback from planner_common so the rest of the pipeline
(manager, executor, notebook builder) sees an identical config shape.
On any connection / JSON / validation error we fall back to build_default_config.
"""

import json
import os

import requests

from .planner_common import (
    DEFAULT_EDITABLE_SETTINGS,
    MAX_CONTAINERS,
    _print_plan_summary,
    _structural_validate,
    apply_custom_settings,
    apply_prompt_stage_names,
    build_default_config,
    enforce_container_count,
    get_recommended_settings,
    redistribute_operations,
    required_containers_for_prompt,
)

# Exact system prompt the adapter was trained with (matches the Modelfile).
SYSTEM_PROMPT = (
    "You are the Planner Agent for a data pipeline orchestrator. Given CSV schema metadata and a "
    "user transformation request, output ONLY a JSON pipeline configuration with keys: containers, "
    "containers_to_create, datasets, stages, execution_order, num_containers, recommended_settings, "
    "editable_settings, reasoning. The first stage has type 'copy' (ADF ingest); all later stages "
    "have type 'notebook' (Databricks PySpark). Output compact JSON only, no prose."
)


def _cfg(name: str, default: str) -> str:
    """Read a setting from config.py, then env, else default."""
    try:
        import config as _c
        val = getattr(_c, name, None)
        if val:
            return str(val)
    except ImportError:
        pass
    return os.getenv(name, default)


def _ollama_host() -> str:
    return _cfg("OLLAMA_HOST", "http://localhost:11434").rstrip("/")


def _planner_model() -> str:
    return _cfg("PLANNER_MODEL", "planner-agent")


def decide_pipeline_config(
    schema: dict,
    user_prompt: str,
    num_containers: int = None,
    custom_settings: dict = None,
    container_names: list = None,
) -> tuple:
    """
    Ask the local fine-tuned planner (served by Ollama) to design the config.
    Returns (config_dict, used_fallback_bool).

    Mirrors groq_planner.decide_pipeline_config's signature and return shape.
    The fine-tuned model has a fixed contract and picks its own stage count,
    so when the user requests num_containers / container_names they are
    enforced afterwards via enforce_container_count (pass-through stages are
    appended if the model produced fewer, extras trimmed if more).
    """
    rec = get_recommended_settings(schema.get("size_hint", "medium"))
    if custom_settings:
        rec.update(custom_settings)

    # K numbered stages in the prompt are transformation stages — the copy
    # stage must not consume one of them, so K+2 containers are required.
    needed = required_containers_for_prompt(user_prompt)
    if needed and (num_containers or 0) < needed:
        if num_containers:
            print(f"   Prompt numbers {needed - 2} stage(s) — raising containers {num_containers} → {needed}")
        num_containers = needed

    user_message = json.dumps(
        {"schema": schema, "user_prompt": user_prompt},
        ensure_ascii=False,
    )

    payload = {
        "model": _planner_model(),
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.2,
            "top_p": 0.8,
            # 2048 truncated large schemas + the generated config itself;
            # the full contract (system + schema + samples + output JSON)
            # regularly exceeds it, which silently degraded output quality.
            "num_ctx": 4096,
        },
    }

    print(f"Local fine-tuned planner ({_planner_model()}) is designing your pipeline...")

    try:
        response = requests.post(
            f"{_ollama_host()}/api/chat",
            json=payload,
            timeout=120,
        )

        if response.status_code != 200:
            raise requests.exceptions.HTTPError(
                f"HTTP {response.status_code}: {response.text[:300]}"
            )

        raw = response.json()["message"]["content"].strip()
        config = json.loads(raw)

        # Normalise to the contract the rest of the system expects.
        config.setdefault("recommended_settings", rec)
        config.setdefault("editable_settings", DEFAULT_EDITABLE_SETTINGS)

        clist = config.get("containers_to_create") or list(config.get("containers", {}).values())
        if clist:
            config["num_containers"] = min(MAX_CONTAINERS, len(clist))

        # The model ignores stage-count requests (fixed contract) — enforce
        # the user's choice on the produced config.
        if num_containers:
            config = enforce_container_count(config, num_containers, container_names, rec)
        # Spread stacked operations into any do-nothing stages — only when
        # the prompt shows distribution intent (numbered stages, "each stage",
        # "distribute", ...); otherwise the model's grouping is respected.
        config = redistribute_operations(config, user_prompt)
        # Explicit user resource settings override whatever the model emitted.
        config = apply_custom_settings(config, custom_settings)
        # Prompt-referenced stage numbers become the notebook stage names.
        config = apply_prompt_stage_names(config, user_prompt)

        config = _structural_validate(config, schema, custom_settings=custom_settings)
        _print_plan_summary(config)
        return config, False

    except json.JSONDecodeError as e:
        print(f"   Ollama returned invalid JSON: {e}")
    except Exception as e:
        err = str(e)
        is_network = isinstance(e, (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        )) or any(x in err for x in ["Connection", "RemoteDisconnected", "aborted", "refused"])
        print(f"   Ollama {'connection error' if is_network else 'error'}: {e}")

    print("   Falling back to default config...")
    return build_default_config(
        schema, user_prompt,
        num_containers=num_containers or 3,
        custom_settings=custom_settings,
        container_names=container_names,
    ), True
