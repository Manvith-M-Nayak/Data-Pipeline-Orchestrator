"""
Real-time anomaly detector.

Runs once per completed managed run (hooked in central_manager_agent/router.py
after the executor finishes) and classifies the run against a fixed catalogue
of anomaly KINDS using only real signals already on the RunState / executor
result / monitor history. Every detected kind is persisted as one row in the
`anomaly_events` table (db_service), so the record survives restarts and is
queryable at GET /api/monitor/anomalies/events.

Kinds detected here and the signal each uses:
    failure       executor status != ok
    timeout       poll result TIMEOUT / POLL_ERROR in the failure message
    retry_storm   manager retries > 0 (>=2 escalates to high)
    slow_runtime  duration > 1.2x the pipeline's historical p95 (needs >=3 runs)
    cold_start    slow_runtime AND >6h gap since the previous run
    zero_rows     run succeeded but the final stage wrote 0 rows
    sla_breach    duration > SLA_SECONDS (config, 0/unset disables)
    cost_spike    manager cost estimate > 2x the trailing average (needs >=3)
    schema_drift  input columns differ from the pipeline's last-seen columns

NOT detectable without Spark task-level metrics (documented, not faked):
    data_skew     needs per-partition task timings the Jobs API doesn't expose
                  on serverless; would require emitting metrics from inside the
                  generated notebook. Tracked as a future enhancement.

Detection failures are always non-fatal: this module observes runs, it must
never break one.
"""

from typing import Dict, List, Optional

from .db_service import DBService

# >6h since previous run of the same pipeline counts as a cold start when the
# run is also slow — serverless spin-up dominates the first run after idle.
COLD_GAP_S = 6 * 3600
SLOW_FACTOR = 1.2         # duration > p95 * this → slow_runtime
COST_FACTOR = 2.0         # cost > trailing avg * this → cost_spike
MIN_HISTORY = 3           # baseline runs needed before slow/cost verdicts


def _sla_seconds() -> float:
    """SLA target from config.py / env; 0 disables sla_breach detection."""
    try:
        import config as _c
        val = getattr(_c, "SLA_SECONDS", None)
        if val:
            return float(val)
    except ImportError:
        pass
    import os
    try:
        return float(os.getenv("SLA_SECONDS", "0"))
    except ValueError:
        return 0.0


def _pipeline_name(result: Optional[Dict]) -> str:
    """Mirror executor_agent.router._notify_monitor's naming so events join
    cleanly with the monitor's pipeline_runs history."""
    mode = ((result or {}).get("mode") or "batch").lower()
    return (
        "Databricks_Streaming_Pipeline"
        if mode == "streaming"
        else "Databricks_Notebook_Pipeline"
    )


async def detect_and_store(
    state: Dict,
    result: Optional[Dict],
    elapsed_ms: int,
    schema: Optional[Dict] = None,
    db: Optional[DBService] = None,
) -> List[Dict]:
    """Classify one finished run and persist every detected anomaly kind.

    state    : manager RunState as dict (get_state_dict) — retries, cost, error
    result   : executor result dict (may be None when the run died pre-execute)
    elapsed_ms: wall time of the whole managed run
    schema   : the input schema dict ({columns: {...}} or {columns: [...]})
    Returns the list of events written (also useful for logging/tests).
    """
    db = db or DBService()
    events: List[Dict] = []
    run_id = (result or {}).get("run_id") or state.get("run_id", "unknown")
    pipeline = _pipeline_name(result)
    duration_s = elapsed_ms / 1000.0
    status_ok = bool(result) and result.get("status") == "ok"
    retries = int(state.get("retries") or 0)
    fail_msg = (
        (result or {}).get("message")
        or state.get("error")
        or ""
    )

    def _add(kind: str, severity: str, detail: str, metrics: Dict):
        events.append({
            "run_id": run_id, "pipeline_name": pipeline, "kind": kind,
            "severity": severity, "detail": detail, "metrics": metrics,
        })

    # ── failure / timeout ────────────────────────────────────────────────────
    if not status_ok:
        msg_u = fail_msg.upper()
        if "TIMEOUT" in msg_u or "POLL_ERROR" in msg_u or "TIMED OUT" in msg_u:
            _add("timeout", "high",
                 f"Run never reached a terminal state: {fail_msg[:200]}",
                 {"duration_s": duration_s})
        else:
            _add("failure", "high",
                 f"Run failed: {fail_msg[:200]}",
                 {"duration_s": duration_s, "retries": retries})

    # ── retry_storm ──────────────────────────────────────────────────────────
    if retries > 0:
        _add("retry_storm", "high" if retries >= 2 else "medium",
             f"Run needed {retries} retry attempt(s) before finishing "
             f"(status={'ok' if status_ok else 'failed'}).",
             {"retries": retries})

    # ── history-based checks (need the monitor DB; all optional) ─────────────
    try:
        stats = await db.get_historical_stats(pipeline)
    except Exception:
        stats = {"count": 0}

    is_slow = (
        status_ok
        and stats.get("count", 0) >= MIN_HISTORY
        and stats.get("p95", 0) > 0
        and duration_s > stats["p95"] * SLOW_FACTOR
    )
    if is_slow:
        ratio = duration_s / stats["p95"]
        # Cold start: same slowness signal but explained by a long idle gap.
        gap_s = None
        try:
            prior = await db.get_pipeline_runs(pipeline_name=pipeline, limit=2)
            # rows are newest-first; index 1 is the previous run (0 may be us)
            if len(prior) >= 2 and prior[1].get("run_end"):
                import datetime as _dt
                prev_end = _dt.datetime.strptime(
                    prior[1]["run_end"], "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=_dt.timezone.utc)
                gap_s = (_dt.datetime.now(_dt.timezone.utc) - prev_end
                         ).total_seconds() - duration_s
        except Exception:
            pass

        if gap_s is not None and gap_s > COLD_GAP_S:
            _add("cold_start", "medium",
                 f"First run after {gap_s/3600:.1f}h idle ran {duration_s:.0f}s "
                 f"({ratio:.1f}x p95) — serverless cold start likely.",
                 {"duration_s": duration_s, "p95_s": stats["p95"],
                  "idle_gap_h": round(gap_s / 3600, 1)})
        else:
            _add("slow_runtime", "high" if ratio >= 2 else "medium",
                 f"Ran {duration_s:.0f}s — {ratio:.1f}x the historical p95 "
                 f"({stats['p95']:.0f}s over {stats['count']} runs).",
                 {"duration_s": duration_s, "avg_s": stats.get("avg"),
                  "p95_s": stats["p95"], "count": stats["count"]})

    # ── sla_breach ───────────────────────────────────────────────────────────
    sla = _sla_seconds()
    if sla > 0 and duration_s > sla:
        _add("sla_breach", "high",
             f"Ran {duration_s:.0f}s against an SLA of {sla:.0f}s "
             f"({duration_s / sla:.2f}x the target).",
             {"duration_s": duration_s, "sla_s": sla})

    # ── zero_rows ────────────────────────────────────────────────────────────
    rows_written = (result or {}).get("rows_written")
    if status_ok and rows_written == 0:
        _add("zero_rows", "medium",
             "Run succeeded but the final stage wrote 0 rows — filter likely "
             "removed everything, or the source was empty.",
             {"rows_written": 0})

    # ── cost_spike ───────────────────────────────────────────────────────────
    cost = ((state.get("cost_estimate") or {}).get("total_usd"))
    try:
        if cost is not None:
            hist = await db.get_metric_history(pipeline, exclude_run_id=run_id)
            costs = [h["cost_usd"] for h in hist if h.get("cost_usd")]
            if len(costs) >= MIN_HISTORY:
                avg_cost = sum(costs) / len(costs)
                if avg_cost > 0 and cost > avg_cost * COST_FACTOR:
                    _add("cost_spike", "high",
                         f"Estimated cost ${cost:.4f} is "
                         f"{cost / avg_cost:.1f}x the trailing average "
                         f"(${avg_cost:.4f} over {len(costs)} runs).",
                         {"cost_usd": cost, "avg_cost_usd": avg_cost,
                          "history": len(costs)})
    except Exception:
        pass

    # ── schema_drift ─────────────────────────────────────────────────────────
    try:
        cols_raw = (schema or {}).get("columns") or {}
        columns = sorted(cols_raw if isinstance(cols_raw, list) else cols_raw.keys())
        if columns:
            last = await db.get_last_schema(pipeline)
            if last is not None and set(last) != set(columns):
                added = sorted(set(columns) - set(last))
                removed = sorted(set(last) - set(columns))
                _add("schema_drift", "medium",
                     f"Input schema changed vs the previous run — "
                     f"added: {added or 'none'}, removed: {removed or 'none'}.",
                     {"added": added, "removed": removed,
                      "columns": len(columns), "previous": len(last)})
            await db.save_schema(pipeline, columns)
    except Exception:
        pass

    # ── persist events + this run's metrics baseline ─────────────────────────
    for ev in events:
        try:
            await db.log_anomaly_event(
                ev["run_id"], ev["pipeline_name"], ev["kind"],
                ev["severity"], ev["detail"], ev["metrics"],
            )
        except Exception as exc:
            print(f"[anomaly] event write non-fatal: {exc}")
    try:
        await db.save_run_metrics(
            run_id, pipeline, duration_s, cost,
            rows_written if isinstance(rows_written, int) else None,
            retries, "ok" if status_ok else "failed",
        )
    except Exception as exc:
        print(f"[anomaly] metrics write non-fatal: {exc}")

    if events:
        print(f"[anomaly] {run_id}: {[e['kind'] for e in events]}")
    return events
