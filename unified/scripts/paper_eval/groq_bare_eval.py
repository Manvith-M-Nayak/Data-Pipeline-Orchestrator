"""Groq planner with vs without the repair layer, paired, on the B1 prompts.

One Groq call per prompt per repeat. The raw model reply is captured from the
HTTP response before `decide_pipeline_config` repairs it, so "raw" and
"shipped" score the same sample (like ablation_planner's *_raw / *_repair).
Raw = the reply's JSON after only the code-fence stripping the backend also
does. Same checks and intent regexes as §B1. No Azure, no Ollama.

    python scripts/paper_eval/groq_bare_eval.py /tmp/groq_bare.json [repeats]
"""
import importlib.util
import json
import re
import os
import statistics as st
import sys
import time

_UNIFIED = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CALLER_CWD = os.getcwd()
sys.path.insert(0, _UNIFIED)
os.chdir(_UNIFIED)

_spec = importlib.util.spec_from_file_location(
    "ablation_planner", os.path.join(_UNIFIED, "scripts", "paper_eval", "ablation_planner.py"))
_ab = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ab)

import planner_agent.groq_planner as gp  # noqa: E402

PRICE = {"openai/gpt-oss-120b": (0.15, 0.60)}  # $ per 1M input / output tokens
_post = gp.requests.post
_last = {}


def _post_capture(*args, **kwargs):
    resp = None
    for _ in range(6):
        resp = _post(*args, **kwargs)
        if resp.status_code != 429:
            break
        try:
            wait = float(resp.headers.get("retry-after") or 20)
        except ValueError:
            wait = 20
        time.sleep(min(max(wait, 1), 60))
    try:
        body = resp.json()
        _last["content"] = body["choices"][0]["message"]["content"]
        _last["usage"] = body.get("usage") or {}
    except Exception:
        _last["content"], _last["usage"] = None, {}
    return resp


gp.requests.post = _post_capture


def _raw_config(content):
    if not content:
        return ""
    # Same fence handling as groq_planner.decide_pipeline_config (lines ~237-248).
    raw = content.strip()
    if "```" in raw:
        for part in raw.split("```"):
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("{"):
                raw = part
                break
    return raw.strip()


_FUNC_OPS = {"equals": "=", "notEquals": "!=", "greater": ">", "greaterOrEqual": ">=",
             "less": "<", "lessOrEqual": "<="}
_CASTS = re.compile(r"\b(?:toInteger|toLong|toDouble|toFloat|toString|toDecimal)\(\s*([A-Za-z_]\w*)\s*\)")


def _dsl_to_sql(text):
    """equals(a, b) -> a = b, greater(toInteger(q), 9) -> q > 9, etc. (one level)."""
    text = _CASTS.sub(r"\1", text)
    for fn, op in _FUNC_OPS.items():
        text = re.sub(rf"\b{fn}\(\s*([^,()]+?)\s*,\s*([^()]+?)\s*\)", rf"\1 {op} \2", text)
    return text


def _int_shift(text, schema):
    """For integer columns only: x > 9 also reads as x >= 10, x >= 10 as x > 9."""
    ints = {c for c, t in (schema.get("inferred_types") or {}).items() if t == "integer"}
    def gt(m):
        col, op, n = m.group(1), m.group(2), int(m.group(3))
        if col not in ints:
            return m.group(0)
        return f"{col} >= {n + 1}" if op == ">" else f"{col} > {n - 1}"
    return re.sub(r"\b([A-Za-z_]\w*)\s*(>=|>)\s*(-?\d+)\b(?!\.)", gt, text)


def fair_score(cfg_or_raw, schema, prompt, expects):
    """Same checks as score(); intent also accepts the function-style filter
    grammar the Groq prompt teaches and integer-equivalent thresholds."""
    s = _ab.score(cfg_or_raw, schema, prompt, expects)
    if not s["valid_json"]:
        return s
    cfg = json.loads(cfg_or_raw) if isinstance(cfg_or_raw, str) else cfg_or_raw
    if isinstance(cfg, dict) and isinstance(cfg.get("config"), dict):
        cfg = cfg["config"]
    blob = json.dumps(cfg)
    texts = [blob, _dsl_to_sql(blob)]
    texts.append(_int_shift(texts[1], schema))
    s["intent"] = all(any(re.search(rx, t, re.IGNORECASE) for t in texts) for rx in expects)
    s["correct"] = s["executable"] and s["intent"]
    return s


def main():
    out = os.path.join(_CALLER_CWD, sys.argv[1])
    repeats = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    quiet = open(os.devnull, "w")
    reps = []
    for rep in range(repeats):
        rows = []
        for name, schema, prompt, expects in _ab.CASES:
            _last.clear()
            t0 = time.time()
            try:
                with _ab.mock.patch("sys.stdout", quiet):
                    cfg, fallback = gp.decide_pipeline_config(schema, prompt)
            except Exception as exc:
                cfg, fallback = {}, True
                _last.setdefault("error", str(exc)[:200])
            raw_text = _raw_config(_last.get("content"))
            rows.append({
                "case": name, "latency_s": round(time.time() - t0, 2),
                "used_fallback": bool(fallback), "usage": _last.get("usage") or {},
                "raw": _ab.score(raw_text, schema, prompt, expects),
                "shipped": _ab.score(cfg, schema, prompt, expects),
                "raw_fair": fair_score(raw_text, schema, prompt, expects),
                "shipped_fair": fair_score(cfg, schema, prompt, expects),
                "raw_output": raw_text, "shipped_config": cfg,
            })
            print(f"rep {rep + 1} {name}: raw={rows[-1]['raw']['correct']} "
                  f"shipped={rows[-1]['shipped']['correct']} fallback={fallback}", flush=True)
        reps.append(rows)

    def pct(rows, cond, key):
        return round(100 * sum(r[cond][key] for r in rows) / len(rows), 1)

    keys = ["valid_json", "structural", "safe", "compiles", "executable", "intent", "correct"]
    conds = ("raw", "shipped", "raw_fair", "shipped_fair")
    per_rep = [{cond: {k: pct(rows, cond, k) for k in keys} for cond in conds} for rows in reps]
    summary = {"model": gp.GROQ_MODEL, "prompts": len(_ab.CASES), "repeats": repeats}
    for cond in conds:
        summary[cond] = {}
        for k in keys:
            vals = [p[cond][k] for p in per_rep]
            summary[cond][k] = {"mean": round(st.mean(vals), 1),
                                "sd": round(st.pstdev(vals), 1) if len(vals) > 1 else 0.0,
                                "per_repeat": vals}
    allrows = [r for rows in reps for r in rows]
    lat = sorted(r["latency_s"] for r in allrows)
    summary["latency_s_median"] = round(st.median(lat), 2)
    summary["fallbacks"] = sum(r["used_fallback"] for r in allrows)
    pt = sum(int(r["usage"].get("prompt_tokens") or 0) for r in allrows)
    ct = sum(int(r["usage"].get("completion_tokens") or 0) for r in allrows)
    price = PRICE.get(gp.GROQ_MODEL)
    summary["tokens"] = {"prompt": pt, "completion": ct}
    summary["cost_usd_all_repeats"] = round(pt * price[0] / 1e6 + ct * price[1] / 1e6, 4) if price else None
    json.dump({"summary": summary, "per_repeat": per_rep, "rows": reps}, open(out, "w"), indent=1)
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
