"""
Planner agent package.

Selects the planning backend at import time via PLANNER_BACKEND
(config.py or env):
  "ollama" (default) → local fine-tuned model served by Ollama
  "groq"             → Groq cloud LLaMA (legacy)

Both expose decide_pipeline_config(schema, user_prompt, ...) -> (config, used_fallback).
"""



def _planner_backend() -> str:
    import settings

    return settings.get("PLANNER_BACKEND", "ollama").lower()


if _planner_backend() == "groq":
    from .groq_planner import decide_pipeline_config
else:
    from .ollama_planner import decide_pipeline_config

__all__ = ["decide_pipeline_config"]
