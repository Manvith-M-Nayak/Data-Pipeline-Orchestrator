"""Score the shipped Groq planner on the same 24 prompts as ablation_planner.py.

One sample per prompt. Temperature and top-p are the values groq_planner sends
(0.2 / 0.8). Scoring uses the same checks and intent regexes as B1. No Azure.

    python scripts/paper_eval/groq_planner_eval.py <out.json>
"""
import importlib.util
import json
import os
import sys
import time

_UNIFIED = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _UNIFIED)
os.chdir(_UNIFIED)

_spec = importlib.util.spec_from_file_location(
    "ablation_planner",
    os.path.join(_UNIFIED, "scripts", "paper_eval", "ablation_planner.py"),
)
_ablation = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ablation)

import planner_agent.groq_planner as gp

OUT = sys.argv[1]
CASES = _ablation.CASES
score = _ablation.score

# gpt-oss-120b list price checked 2026-10-04. Other models report tokens only.
_PRICE = {
    "openai/gpt-oss-120b": (0.15, 0.60),
}

_usage = []
_post = gp.requests.post


def _post_retry(*args, **kwargs):
    resp = None
    for _ in range(6):
        resp = _post(*args, **kwargs)
        if resp.status_code != 429:
            break
        try:
            wait = int(resp.headers.get("retry-after") or 20)
        except ValueError:
            wait = 20
        time.sleep(min(max(wait, 1), 60))
    try:
        _usage.append((resp.json() or {}).get("usage") or {})
    except Exception:
        _usage.append({})
    return resp


gp.requests.post = _post_retry


def _pct(rows, key):
    if not rows:
        return None
    return round(100 * sum(1 for r in rows if r[key]) / len(rows), 1)


def main():
    rows = []
    quiet = open(os.devnull, "w")
    for i, (name, schema, prompt, expects) in enumerate(CASES, 1):
        before = len(_usage)
        t0 = time.time()
        try:
            with _ablation.mock.patch("sys.stdout", quiet):
                cfg, fallback = gp.decide_pipeline_config(schema, prompt)
        except Exception as exc:
            cfg, fallback = {}, True
            err = str(exc)[:200]
        else:
            err = ""
        latency = round(time.time() - t0, 2)
        usage = _usage[before:] if len(_usage) > before else []
        row = {
            "case": name,
            "latency_s": latency,
            "used_fallback": bool(fallback),
            "error": err,
            "usage": usage,
            "score": score(cfg, schema, prompt, expects),
        }
        rows.append(row)
        print(f"{i}/{len(CASES)} {name} fallback={fallback} "
              f"correct={row['score']['correct']} {latency}s", flush=True)

    scored = [r["score"] for r in rows]
    prompt_tokens = sum(int(u.get("prompt_tokens") or 0) for r in rows for u in r["usage"])
    completion_tokens = sum(int(u.get("completion_tokens") or 0) for r in rows for u in r["usage"])
    price = _PRICE.get(gp.GROQ_MODEL)
    cost = None
    if price and (prompt_tokens or completion_tokens):
        cost = round(prompt_tokens * price[0] / 1e6 + completion_tokens * price[1] / 1e6, 6)
    summary = {
        "model": gp.GROQ_MODEL,
        "n": len(rows),
        "fallbacks": sum(1 for r in rows if r["used_fallback"]),
        "valid_json_pct": _pct(scored, "valid_json"),
        "structural_pct": _pct(scored, "structural"),
        "safe_pct": _pct(scored, "safe"),
        "compiles_pct": _pct(scored, "compiles"),
        "executable_pct": _pct(scored, "executable"),
        "intent_pct": _pct(scored, "intent"),
        "correct_pct": _pct(scored, "correct"),
        "latency_s_median": sorted(r["latency_s"] for r in rows)[len(rows) // 2],
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "estimated_cost_usd": cost,
    }
    payload = {"summary": summary, "rows": rows}
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
