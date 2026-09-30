"""
Planner self-verification.

The planner checks its own plan with the Assurance library before returning
it, instead of handing the user a plan that a later check contradicts:

  1. generate a plan (LLM + the planner's deterministic repairs)
  2. verify it: structural rules + the intent check (assurance_agent)
  3. if a structural check failed or the intent check found valid issues,
     regenerate ONCE with those problems as feedback to the model
  4. return the best attempt plus a verification report

The deterministic fallback plan (model unreachable) is verified but never
regenerated — a retry would hit the same unreachable model.
"""

from typing import Callable, Optional, Tuple

MAX_REPLANS = 1


def _problems(result) -> Tuple[list, list]:
    structural = [c for c in result.structural_results if not c.passed]
    sem = result.semantic_result
    issues = list(sem.issues) if (sem and sem.available and sem.flagged) else []
    return structural, issues


def _feedback(structural: list, issues: list) -> str:
    lines = [f"- {c.label}: {c.message}" for c in structural]
    lines += [
        f"- Stage '{it.get('stage')}': {it.get('problem')}"
        + (f" — fix: {it['suggestion']}" if it.get("suggestion") else "")
        for it in issues
    ]
    return (
        "REVIEW FEEDBACK — a previous plan for this request was rejected. "
        "Generate a corrected plan that fixes these problems:\n" + "\n".join(lines)
    )


def plan_with_verification(
    build: Callable[[Optional[str]], Tuple[dict, bool]],
    schema: dict,
    user_prompt: str,
    max_replans: int = MAX_REPLANS,
) -> Tuple[dict, bool, dict]:
    """build(review_feedback) -> (config, used_fallback).

    Returns (config, used_fallback, verification) where verification is
    {"verified", "attempts", "replanned", "history", "final"}; "final" is the
    chosen attempt's full AssuranceResult dict (same shape as
    POST /api/assurance/validate, so the UI renders it unchanged).
    """
    from assurance_agent import AssuranceAgent

    attempts, feedback = [], None
    for n in range(max_replans + 1):
        config, used_fallback = build(feedback)
        result = AssuranceAgent().assure(user_prompt, config, schema, run_semantic=True)
        structural, issues = _problems(result)
        attempts.append({
            "config": config, "used_fallback": used_fallback, "result": result,
            "structural": structural, "issues": issues,
        })
        print(f"   Self-check attempt {n + 1}: {len(structural)} structural failure(s), "
              f"{len(issues)} intent issue(s)")
        if (not structural and not issues) or used_fallback:
            break
        feedback = _feedback(structural, issues)

    # Fewest structural failures wins, then fewest intent issues; on a tie
    # prefer the later attempt (it was made with the feedback).
    best = min(
        enumerate(attempts),
        key=lambda ia: (len(ia[1]["structural"]), len(ia[1]["issues"]), -ia[0]),
    )[1]
    verification = {
        "verified": not best["structural"] and not best["issues"],
        "attempts": len(attempts),
        "replanned": len(attempts) > 1,
        "history": [
            {
                "attempt": i + 1,
                "structural_failures": [c.label for c in a["structural"]],
                "intent_issues": len(a["issues"]),
                "used_fallback": a["used_fallback"],
            }
            for i, a in enumerate(attempts)
        ],
        "final": best["result"].to_dict(),
    }
    return best["config"], best["used_fallback"], verification
