"""
Combined Results & Logs endpoint — aggregates outputs from every agent into
a single view per run and a single analytics summary across all runs.

Data sources merged:
  - manager_runs (SQLite)       → RunState snapshots (all agent results + decision log)
  - manager_feedback.jsonl      → per-run feedback (predicted vs actual, cost, assurance)
  - pipeline_runs (SQLite)      → ADF monitor run records
  - pipeline_analyses (SQLite)  → Monitor Agent's AI analysis per run
  - anomaly_log (SQLite)        → stuck-pipeline anomaly records
"""

import os

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def _feedback_records() -> list:
    from jsonl_log import read_jsonl

    try:
        return read_jsonl(os.path.join(_DATA_DIR, "manager_feedback.jsonl"))
    except Exception as exc:
        print(f"[combined] feedback read failed: {exc}")
        return []


async def _get_db():
    from monitor_agent import deps

    return deps.get_db()


# ── Per-run combined detail ──────────────────────────────────────────────────
@router.get("/run/{run_id}")
async def combined_run_detail(run_id: str):
    """Return every agent's result + logs for a single run, merging the
    manager RunState, the feedback record, and the monitor analysis."""

    # 1. Manager state (full RunState with all agent outputs + decisions)
    db = await _get_db()
    state = None
    if db is not None:
        state = await db.get_manager_run(run_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Run not found")

    # 2. Feedback record (predicted vs actual, cost accuracy)
    feedback = None
    for rec in _feedback_records():
        if rec.get("run_id") == run_id:
            feedback = rec
            break

    # 3. Monitor analysis (AI insights, anomalies, root cause)
    # pipeline_runs is keyed by the executor's ADF / dbx- run id, not the
    # manager's uuid — try the Databricks record first, then the ADF one.
    monitor_analysis = None
    if db is not None:
        ex = state.get("executor_result") or {}
        rows = []
        for rid in (ex.get("dbx_run_id"), ex.get("run_id"), ex.get("adf_run_id")):
            if rid:
                rows = await db.get_pipeline_runs(run_id=rid, limit=1)
                if rows:
                    break
        if rows:
            monitor_analysis = {
                "pipeline_name": rows[0].get("pipeline_name"),
                "status": rows[0].get("status"),
                "duration_ms": rows[0].get("duration_ms"),
                "message": rows[0].get("message"),
                "status_summary": rows[0].get("status_summary"),
                "anomalies": rows[0].get("anomalies"),
                "root_cause": rows[0].get("root_cause"),
                "performance_insights": rows[0].get("performance_insights"),
                "suggestions": rows[0].get("suggestions"),
                "severity": rows[0].get("severity"),
                "explanation": rows[0].get("explanation"),
            }

    return {
        "run_id": run_id,
        "status": state.get("status"),
        "phase": state.get("phase"),
        "step": state.get("step"),
        "started_at": state.get("started_at"),
        "completed_at": state.get("completed_at"),
        "retries": state.get("retries"),
        "error": state.get("error"),
        # Agent results
        "plan": state.get("plan"),
        "plan_summary": state.get("plan_summary"),
        "validation": state.get("validation"),
        "plan_assurance": state.get("plan_assurance"),
        "parallelism": state.get("parallelism"),
        "predictions": state.get("predictions"),
        "resource_plan": state.get("resource_plan"),
        "cost_estimate": state.get("cost_estimate"),
        "performance_prediction": state.get("performance_prediction"),
        "cost_optimization": state.get("cost_optimization"),
        "executor_result": state.get("executor_result"),
        "assurance": state.get("assurance"),
        "user_request": state.get("user_request"),
        # Decision / audit log (all agent log entries)
        "decisions": state.get("decisions", []),
        # Cross-referenced data
        "feedback": feedback,
        "monitor_analysis": monitor_analysis,
    }


# ── Aggregated analytics across all runs ─────────────────────────────────────
@router.get("/analytics")
async def combined_analytics(limit: int = Query(default=200, ge=1, le=1000)):
    """Aggregate stats across all managed runs combining every agent's data."""

    db = await _get_db()

    # ── Manager runs ──────────────────────────────────────────────────────
    manager_runs = []
    if db is not None:
        manager_runs = await db.list_manager_runs(limit=limit)

    # ── Feedback records ──────────────────────────────────────────────────
    feedback = _feedback_records()
    if db is not None:
        # Same window as manager_runs (limited by `limit`) — otherwise totals
        # mix "last N runs" with "all feedback ever".
        window = {r.get("run_id") for r in manager_runs}
        feedback = [r for r in feedback if r.get("run_id") in window]
    feedback_by_run = {r.get("run_id"): r for r in feedback}

    # ── Monitor pipeline runs ─────────────────────────────────────────────
    monitor_runs = []
    if db is not None:
        monitor_runs = await db.get_pipeline_runs(limit=limit)

    # ── Anomalies ─────────────────────────────────────────────────────────
    anomalies, ai_verdicts = [], []
    if db is not None:
        # Classified anomaly events (anomaly_detector); the legacy
        # anomaly_log only holds Groq verdicts on live slow runs.
        anomalies = await db.get_anomaly_events(limit=limit)
        ai_verdicts = await db.get_anomaly_log(limit=limit)

    # ── Compute aggregates ────────────────────────────────────────────────
    total_runs = len(manager_runs)
    completed = sum(1 for r in manager_runs if r.get("status") == "completed")
    failed = sum(1 for r in manager_runs if r.get("status") == "failed")
    in_progress = total_runs - completed - failed

    # Duration accuracy (predicted vs actual)
    # Only runs whose pipeline completed: aborted or failed runs log abort
    # time / retry backoff, not a duration comparable with the prediction.
    duration_ratios = []
    for fb in feedback:
        # "feedback" = success in records logged before the final_status fix.
        if fb.get("final_status") not in ("completed", "feedback") or fb.get("executed") is False:
            continue
        pred = fb.get("predicted_duration_s") or fb.get("perf_predicted_total_s")
        actual = fb.get("actual_duration_s")
        if pred and actual and pred > 0:
            duration_ratios.append(round(actual / pred, 2))

    avg_duration_ratio = (
        round(sum(duration_ratios) / len(duration_ratios), 2)
        if duration_ratios
        else None
    )

    # Cost accuracy
    cost_errors = []
    for fb in feedback:
        est = fb.get("estimated_cost_usd")
        actual = fb.get("actual_cost_usd")
        if est is not None and actual is not None and est > 0:
            cost_errors.append(round(abs(actual - est) / est * 100, 1))

    avg_cost_error_pct = (
        round(sum(cost_errors) / len(cost_errors), 1) if cost_errors else None
    )

    # Assurance pass rates
    assurance_runs = [fb for fb in feedback if fb.get("assurance_passed") is not None]
    assurance_pass_rate = (
        round(
            sum(1 for fb in assurance_runs if fb["assurance_passed"])
            / len(assurance_runs)
            * 100,
            1,
        )
        if assurance_runs
        else None
    )

    plan_assurance_runs = [
        fb for fb in feedback if fb.get("plan_assurance_passed") is not None
    ]
    plan_assurance_pass_rate = (
        round(
            sum(1 for fb in plan_assurance_runs if fb["plan_assurance_passed"])
            / len(plan_assurance_runs)
            * 100,
            1,
        )
        if plan_assurance_runs
        else None
    )

    # Prediction source distribution
    ml_predictions = sum(
        1 for fb in feedback if fb.get("prediction_source") == "ml_model"
    )
    formula_predictions = sum(
        1 for fb in feedback if fb.get("prediction_source") == "formula"
    )

    # Learning corrections applied
    corrections_applied = sum(
        1
        for fb in feedback
        if fb.get("learning_correction_applied") or fb.get("cost_correction_applied")
    )

    # Retry stats
    total_retries = sum(r.get("retries", 0) for r in manager_runs)

    # Cost totals
    total_estimated_cost = sum(fb.get("estimated_cost_usd") or 0 for fb in feedback)
    total_actual_cost = sum(fb.get("actual_cost_usd") or 0 for fb in feedback)

    # Per-run combined summary (most recent first)
    runs_summary = []
    for run in manager_runs:
        rid = run.get("run_id")
        fb = feedback_by_run.get(rid)
        runs_summary.append(
            {
                "run_id": rid,
                "status": run.get("status"),
                "phase": run.get("phase"),
                "step": run.get("step"),
                "started_at": run.get("started_at"),
                "completed_at": run.get("completed_at"),
                "retries": run.get("retries"),
                "stage_count": run.get("stage_count"),
                # From feedback
                "actual_duration_s": fb.get("actual_duration_s") if fb else None,
                "predicted_duration_s": fb.get("predicted_duration_s") if fb else None,
                "cost_estimate_usd": fb.get("estimated_cost_usd") if fb else None,
                "actual_cost_usd": fb.get("actual_cost_usd") if fb else None,
                "assurance_passed": fb.get("assurance_passed") if fb else None,
                "plan_assurance_passed": fb.get("plan_assurance_passed")
                if fb
                else None,
                "prediction_source": fb.get("prediction_source") if fb else None,
                "complexity": fb.get("complexity") if fb else None,
            }
        )

    # Agent health summary
    agent_health = {
        "planner": {
            "total_plans": total_runs,
            "fallback_used": sum(1 for fb in feedback if fb.get("used_fallback")),
        },
        "assurance": {
            "pre_execution_pass_rate": plan_assurance_pass_rate,
            "post_execution_pass_rate": assurance_pass_rate,
            "total_checks": len(assurance_runs),
        },
        "resource": {
            "total_predictions": total_runs,
            # resource_feasible is logged since the logic fixes; older
            # records lack it and are not counted either way.
            "feasible_plans": sum(1 for fb in feedback if fb.get("resource_feasible") is True),
            "infeasible_plans": sum(1 for fb in feedback if fb.get("resource_feasible") is False),
        },
        "performance_prediction": {
            "ml_predictions": ml_predictions,
            "formula_predictions": formula_predictions,
            "corrections_applied": corrections_applied,
        },
        "cost_optimization": {
            "total_estimated_usd": round(total_estimated_cost, 5),
            "total_actual_usd": round(total_actual_cost, 5),
            "avg_error_pct": avg_cost_error_pct,
        },
        "executor": {
            "total_runs": total_runs,
            "total_retries": total_retries,
            "avg_retries": round(total_retries / total_runs, 2) if total_runs else 0,
        },
        "monitor": {
            "total_pipeline_runs": len(monitor_runs),
            "anomalies_detected": len(anomalies),
            "ai_slow_run_verdicts": len(ai_verdicts),
        },
    }

    return {
        "summary": {
            "total_runs": total_runs,
            "completed": completed,
            "failed": failed,
            "in_progress": in_progress,
            "success_rate_pct": round(completed / total_runs * 100, 1)
            if total_runs
            else 0,
        },
        "duration_accuracy": {
            "avg_predicted_vs_actual_ratio": avg_duration_ratio,
            "samples": len(duration_ratios),
        },
        "cost_accuracy": {
            "total_estimated_usd": round(total_estimated_cost, 5),
            "total_actual_usd": round(total_actual_cost, 5),
            "avg_error_pct": avg_cost_error_pct,
            "samples": len(cost_errors),
        },
        "agent_health": agent_health,
        "runs": runs_summary,
    }
