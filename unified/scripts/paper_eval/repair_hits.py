"""Per-function hit rate of the deterministic repair layer, on saved raw replies.

Replays every saved raw model reply through the same repair chain as
groq_planner.decide_pipeline_config, one function at a time, and records
which functions changed the plan (ignoring the execution_groups
drop-and-rebuild that happens to every plan). No model calls: the replies come from a
groq_bare_eval.py output file (which saves `raw_output` for every call).

    python scripts/paper_eval/repair_hits.py groq_out.json out.json

`_structural_validate` is split into its two renaming passes and the rest, so
the container de-duplication fixed on 2026-10-07 shows up on its own.
"""
import copy
import importlib.util
import io
import json
import os
import sys
from contextlib import redirect_stdout

_UNIFIED = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_CALLER_CWD = os.getcwd()
sys.path.insert(0, _UNIFIED)
os.chdir(_UNIFIED)

_spec = importlib.util.spec_from_file_location(
    "ablation_planner", os.path.join(_UNIFIED, "scripts", "paper_eval", "ablation_planner.py"))
_ab = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ab)

import planner_agent.planner_common as pc  # noqa: E402
from planner_agent.groq_planner import MAX_CONTAINERS  # noqa: E402

STEPS = ["strip_auto_timestamp", "enforce_container_count", "redistribute_operations",
         "reconcile_prompt_filters", "apply_custom_settings", "apply_prompt_stage_names",
         "normalize_container_names", "normalize_identifiers", "structural_validate_rest"]


def replay(raw_text, schema, prompt):
    """Same order and arguments as decide_pipeline_config (custom_settings=None)."""
    config = json.loads(raw_text)
    rec = pc.get_recommended_settings(schema.get("size_hint", "medium"))
    needed = pc.required_containers_for_prompt(prompt)
    n = max(2, min(MAX_CONTAINERS, needed or 3))
    hits = {}

    def substance(c):
        # execution_groups is dropped by enforce_container_count and rebuilt by
        # _structural_validate for every plan; that round trip is bookkeeping.
        return {k: v for k, v in c.items() if k != "execution_groups"}

    def step(name, fn):
        nonlocal config
        before = copy.deepcopy(substance(config))
        config = fn(config)
        hits[name] = substance(config) != before

    with redirect_stdout(io.StringIO()):
        step("strip_auto_timestamp", lambda c: pc.strip_auto_timestamp(c, prompt))
        config.setdefault("recommended_settings", rec)
        config.setdefault("editable_settings", pc.DEFAULT_EDITABLE_SETTINGS)
        config["num_containers"] = n
        step("enforce_container_count", lambda c: pc.enforce_container_count(c, n, None, rec))
        step("redistribute_operations", lambda c: pc.redistribute_operations(c, prompt))
        step("reconcile_prompt_filters", lambda c: pc.reconcile_prompt_filters(c, prompt, schema))
        step("apply_custom_settings", lambda c: pc.apply_custom_settings(c, None))
        step("apply_prompt_stage_names", lambda c: pc.apply_prompt_stage_names(c, prompt))
        # _structural_validate runs both renaming passes first; running them
        # here makes its own calls no-ops, so "rest" counts only the remainder.
        step("normalize_container_names", pc._normalize_container_names)
        step("normalize_identifiers", pc._normalize_identifiers)
        step("structural_validate_rest", lambda c: pc._structural_validate(c, schema))
    return config, hits


def main():
    src = json.load(open(os.path.join(_CALLER_CWD, sys.argv[1])))
    out_path = os.path.join(_CALLER_CWD, sys.argv[2])
    cases = {name: (schema, prompt, expects) for name, schema, prompt, expects in _ab.CASES}
    rows = []
    for rep in src["rows"]:
        for r in rep:
            if r.get("used_fallback") or not r.get("raw_output"):
                continue
            schema, prompt, expects = cases[r["case"]]
            try:
                cfg, hits = replay(r["raw_output"], schema, prompt)
            except Exception as exc:  # unparseable reply: nothing to repair
                rows.append({"case": r["case"], "error": str(exc)[:120]})
                continue
            raw = _ab.score(r["raw_output"], schema, prompt, expects)
            rep_score = _ab.score(cfg, schema, prompt, expects)
            rows.append({"case": r["case"], "hits": hits,
                         "raw_executable": raw["executable"], "repaired_executable": rep_score["executable"],
                         "raw_correct": raw["correct"], "repaired_correct": rep_score["correct"],
                         # replay fidelity: equals what the backend shipped at eval time
                         "matches_shipped": cfg == r.get("shipped_config")})
    ok = [r for r in rows if "hits" in r]
    n = len(ok)
    table = {s: {"changed": sum(r["hits"][s] for r in ok),
                 "pct": round(100 * sum(r["hits"][s] for r in ok) / n, 1) if n else 0.0,
                 "in_rescued_plans": sum(r["hits"][s] for r in ok
                                         if r["repaired_executable"] and not r["raw_executable"])}
             for s in STEPS}
    summary = {"replies": n, "unparseable": len(rows) - n,
               "any_change": sum(any(r["hits"].values()) for r in ok),
               "rescued_to_executable": sum(r["repaired_executable"] and not r["raw_executable"] for r in ok),
               "replay_matches_shipped": sum(r["matches_shipped"] for r in ok),
               "broken_by_repair": sum(r["raw_executable"] and not r["repaired_executable"] for r in ok),
               "per_function": table}
    json.dump({"summary": summary, "rows": rows}, open(out_path, "w"), indent=1)
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
