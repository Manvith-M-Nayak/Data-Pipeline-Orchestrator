"""
Planner agent package.

Selects the planning backend at import time via PLANNER_BACKEND
(config.py or env):
  "ollama" (default) → local fine-tuned model served by Ollama
  "groq"             → Groq cloud LLaMA (legacy)

Both expose decide_pipeline_config(schema, user_prompt, ...) -> (config, used_fallback).
"""

import os


def _planner_backend() -> str:
    try:
        import config as _c
        val = getattr(_c, "PLANNER_BACKEND", None)
        if val:
            return str(val).lower()
    except ImportError:
        pass
    return os.getenv("PLANNER_BACKEND", "ollama").lower()


if _planner_backend() == "groq":
    from .groq_planner import decide_pipeline_config
else:
    from .ollama_planner import decide_pipeline_config

__all__ = ["decide_pipeline_config"]
