"""
SEMANTIC VALIDATION layer — base LLM via Ollama, prompting ONLY.

Checks whether the generated plan actually matches the user's original request.
This is the probabilistic layer; a human may override its judgment.

Deliberate constraints (see README):
  - Uses the CLEAN BASE model `qwen2.5:7b-instruct`.
  - NO LoRA adapter. The Planner's adapter was trained to GENERATE plans, not
    to VALIDATE them; reusing it would bias the validator toward agreeing with
    whatever the Planner produced. The validator must be independent.
  - Shares no generation logic with the Planner.

The model is asked to return strict JSON {flagged: bool, reasoning: str}. If
Ollama is unreachable or returns garbage, we degrade gracefully: the result is
marked available=False and is NOT counted against the plan.
"""

import json
import os

import requests

from .result import SemanticResult


# Frame the model as an auditor, not an author, and force a binary judgment.
SYSTEM_PROMPT = (
    "You are an independent Assurance auditor for a data-pipeline orchestrator. "
    "You are given a USER REQUEST and a PLAN (JSON) that another agent generated "
    "to satisfy it. Your ONLY job is to judge whether the plan matches the intent "
    "of the request. Do NOT rewrite or generate a plan. Look for mismatches: "
    "missing operations the user asked for, extra operations the user did not ask "
    "for, wrong columns, wrong aggregation, wrong filtering, wrong direction of "
    "data flow. Respond with STRICT JSON only, no prose, in the form "
    '{"flagged": true|false, "reasoning": "<one or two sentences>", '
    '"issues": [{"stage": "<exact stage name, or \'plan\' for plan-wide issues>", '
    '"problem": "<the specific operation, filter, or column that is wrong and why>", '
    '"suggestion": "<the concrete change that would fix it>"}]}. '
    "Set flagged=true if the plan does NOT faithfully match the request. "
    "issues MUST be an empty list when flagged is false. Every issue must name "
    "the exact stage and the exact operation/filter/column at fault — never say "
    "'unnecessary transformations' without listing which ones."
)


def _cfg(name: str, default: str) -> str:
    """Read a setting from config.py, then env, else default (mirrors planner)."""
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


def _model() -> str:
    # NOTE: base model, NOT the planner adapter. Override via ASSURANCE_MODEL.
    return _cfg("ASSURANCE_MODEL", "qwen2.5:7b-instruct")


def check_intent(user_request: str, plan: dict, timeout: int = 120) -> SemanticResult:
    """
    Hand the base model the original request + generated plan and ask it to flag
    mismatches. Returns a SemanticResult; never raises (degrades gracefully).
    """
    model = _model()
    user_message = json.dumps(
        {"user_request": user_request, "plan": plan},
        ensure_ascii=False,
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        "stream": False,
        "format": "json",
        # 8192 ctx: request + full plan JSON often exceeds 4096, which silently
        # truncated the plan the auditor was judging.
        "options": {"temperature": 0.0, "num_ctx": 8192},
    }

    try:
        resp = requests.post(f"{_ollama_host()}/api/chat", json=payload, timeout=timeout)
        if resp.status_code != 200:
            return SemanticResult(
                flagged=False,
                reasoning=f"semantic check unavailable: Ollama HTTP {resp.status_code}: {resp.text[:200]}",
                model=model, available=False,
            )
        raw = resp.json()["message"]["content"].strip()
        data = json.loads(raw)
        flagged_val = data.get("flagged", False)
        if isinstance(flagged_val, str):
            # models sometimes emit "true"/"false" as strings; bool("false") is True
            flagged = flagged_val.strip().lower() in ("true", "yes", "1")
        else:
            flagged = bool(flagged_val)
        reasoning = str(data.get("reasoning", "")).strip() or "(model returned no reasoning)"

        issues = []
        for it in (data.get("issues") or []):
            if not isinstance(it, dict):
                continue
            problem = str(it.get("problem", "")).strip()
            if not problem:
                continue
            issues.append({
                "stage":      str(it.get("stage", "plan")).strip() or "plan",
                "problem":    problem,
                "suggestion": str(it.get("suggestion", "")).strip(),
            })
        if not flagged:
            issues = []

        return SemanticResult(flagged=flagged, reasoning=reasoning, model=model,
                              available=True, issues=issues)

    except json.JSONDecodeError as e:
        return SemanticResult(
            flagged=False,
            reasoning=f"semantic check unavailable: model returned non-JSON ({e})",
            model=model, available=False,
        )
    except Exception as e:
        return SemanticResult(
            flagged=False,
            reasoning=f"semantic check unavailable: {type(e).__name__}: {e}",
            model=model, available=False,
        )
