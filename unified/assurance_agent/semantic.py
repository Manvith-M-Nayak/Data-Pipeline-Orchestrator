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

import requests

from .result import SemanticResult


# Frame the model as an auditor, not an author, and force a binary judgment.
SYSTEM_PROMPT = (
    "You are an independent Assurance auditor for a data-pipeline orchestrator. "
    "You are given a USER REQUEST and the PLAN_OPERATIONS another agent generated "
    "to satisfy it: one line per stage listing exactly what that stage does "
    "(copy, filter, transforms, aggregation). Read each line literally — if a "
    "line shows a filter, that filter IS applied. Lines marked [infrastructure] "
    "(the mandatory ingest copy, or a stage that only passes data on) exist in "
    "every plan for technical reasons: they change no data and are NEVER a "
    "mismatch — judge intent only by the data operations (filters, transforms, "
    "aggregations). "
    "Your ONLY job is to judge whether the plan matches the intent "
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
    "'unnecessary transformations' without listing which ones. "
    "DATASET_COLUMNS lists the real columns of the input data: any column name "
    "that appears there is correct as written — never suggest renaming it, and "
    "never suggest a column that is not in DATASET_COLUMNS or created by an "
    "earlier transformation in the plan."
)


def plan_operations(plan: dict) -> list:
    """One plain line per stage describing what it does. Small models misread
    the full plan JSON (containers, datasets, settings — none of which bear on
    intent) and e.g. report a filter as "missing" when the stage has one."""
    lines = []
    for s in (plan or {}).get("stages", []) or []:
        name, stype = s.get("name", "?"), s.get("type", "?")
        if stype == "copy":
            lines.append(f"{name}: [infrastructure] load the input data (mandatory ingest copy)")
            continue
        ops = []
        transforms = [t for t in (s.get("transformations") or []) if isinstance(t, str) and t.strip()]
        if transforms:
            ops.append("transforms: " + "; ".join(transforms))
        if s.get("filter_condition"):
            ops.append(f"filter (keep rows where): {s['filter_condition']}")
        agg = s.get("aggregation") or {}
        if agg.get("aggregations"):
            # Plain words: "count(*) as n" was read by the 7B model as
            # "only counts rows", missing the per-group part.
            parts = []
            for a in agg["aggregations"]:
                if not isinstance(a, dict):
                    continue
                what = "count the rows" if a.get("op") == "count" else f"{a.get('op')} of {a.get('column')}"
                parts.append(f"{what} (output column {a.get('alias')})")
            ops.append(f"aggregate — for each distinct {' + '.join(agg.get('group_by') or [])}: "
                       + "; ".join(parts))
        lines.append(f"{name}: " + (" | ".join(ops) if ops else "[infrastructure] passes data on unchanged"))
    return lines


def _is_noop_suggestion(issue: dict, plan: dict) -> bool:
    """True when the suggested fix is what a stage already does — verbatim, or
    with only the output column renamed ("output column n" → "n_animals").
    Such a "fix" changes no data, so the issue is not an intent mismatch."""
    import re

    def _norm(text):
        text = re.sub(r"output column \w+", "output column <name>", str(text or ""))
        text = re.sub(r"^(change( it)? to|use|should be)\s*:?\s*", "", text.strip(), flags=re.I)
        return re.sub(r"\s+", " ", text).strip(" .'\"`").lower()

    raw = str(issue.get("suggestion") or "")
    suggestion = _norm(raw)
    if not suggestion:
        return False
    ops_text = " ".join(_norm(line) for line in plan_operations(plan))
    # "Add `sum of price`" when the stage already computes sum of price.
    if re.match(r"\s*(add|include|keep|apply)\b", raw, re.I):
        frags = [f for f in re.findall(r"`([^`]+)`|'([^']+)'|\"([^\"]+)\"", raw)]
        frags = [_norm(next(x for x in f if x)) for f in frags]
        if frags and all(f and f in ops_text for f in frags):
            return True
    for line in plan_operations(plan):
        op = _norm(line.split(": ", 1)[1] if ": " in line else line)
        if op and (suggestion == op or suggestion.endswith(op) or op.endswith(suggestion)):
            return True
    return False


def _plan_created_columns(plan: dict) -> set:
    """Columns the plan itself creates (transform LHS, aggregation aliases)."""
    out = set()
    for s in (plan or {}).get("stages", []) or []:
        for t in s.get("transformations") or []:
            if isinstance(t, str) and "=" in t:
                lhs = t.split("=", 1)[0].strip()
                if lhs.isidentifier():
                    out.add(lhs)
        for a in ((s.get("aggregation") or {}).get("aggregations") or []):
            if isinstance(a, dict) and a.get("alias"):
                out.add(a["alias"])
    return out


def _hallucinated_columns(text: str, known: set) -> set:
    """Column-like identifiers inside quoted expressions of `text` that are
    neither dataset columns nor plan-created ones. Quoted fragments are where
    the model writes concrete filters/transforms ('is_predator = 1')."""
    import re

    from .config_loader import load_allowed_operations
    from .structural import StructuralValidator

    validator = StructuralValidator(load_allowed_operations(), {})
    # SQL / aggregation vocabulary the model writes inside quotes — never columns.
    vocab = {"group", "by", "distinct", "as", "per", "each", "order", "select", "where",
             "from", "having", "count", "sum", "avg", "average", "min", "max", "rows", "row",
             "filter", "keep", "drop", "the", "of", "for", "and", "or", "not", "in", "is"}
    unknown = set()
    for frag in re.findall(r"'([^']+)'|\"([^\"]+)\"", text or ""):
        expr = frag[0] or frag[1]
        if not re.search(r"[=<>(]", expr) and not expr.isidentifier():
            continue  # prose in quotes, not an expression or a bare column
        unknown |= {r for r in validator._refs_in_expr(expr)
                    if r not in known and r.lower() not in vocab}
    return unknown


def _cfg(name: str, default: str) -> str:
    """Read a setting from env/.env, then config.py, else default (mirrors planner)."""
    import settings

    return settings.get(name, default)


def _ollama_host() -> str:
    return _cfg("OLLAMA_HOST", "http://localhost:11434").rstrip("/")


def _model() -> str:
    # NOTE: base model, NOT the planner adapter. Override via ASSURANCE_MODEL.
    return _cfg("ASSURANCE_MODEL", "qwen2.5:7b-instruct")


def check_intent(user_request: str, plan: dict, timeout: int = 120,
                 schema: dict = None) -> SemanticResult:
    """
    Hand the base model the original request + generated plan + the dataset's
    real columns and ask it to flag mismatches. Returns a SemanticResult;
    never raises (degrades gracefully).

    schema: {"columns": [...], "inferred_types": {...}} (normalize_schema
    shape). Without it the model cannot know the real column names and
    invents "corrections" (e.g. predator → is_predator).
    """
    model = _model()
    schema = schema or {}
    columns = list(schema.get("columns") or [])
    types = schema.get("inferred_types") or {}
    user_message = json.dumps(
        {
            "user_request": user_request,
            "DATASET_COLUMNS": {c: types.get(c, "unknown") for c in columns},
            "PLAN_OPERATIONS": plan_operations(plan),
        },
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

        # Deterministic guard: an issue whose fix uses a column that exists
        # neither in the data nor in the plan is a hallucination — drop it.
        if issues and columns:
            known = set(columns) | _plan_created_columns(plan)
            kept, dropped = [], []
            for it in issues:
                bad = _hallucinated_columns(f"{it['problem']} {it['suggestion']}", known)
                if not bad and _is_noop_suggestion(it, plan):
                    bad = {"(suggestion repeats an existing operation)"}
                (dropped if bad else kept).append((it, bad))
            if dropped:
                names = sorted({c for _, bad in dropped for c in bad})
                issues = [it for it, _ in kept]
                note = (f"Discarded {len(dropped)} issue(s) that referenced columns not in "
                        f"the data or the plan, or proposed what the plan already does: "
                        f"{', '.join(names)}.")
                if not issues:
                    flagged = False
                    reasoning = f"No valid issues remain. {note}"
                else:
                    reasoning = f"{reasoning} ({note})"

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
