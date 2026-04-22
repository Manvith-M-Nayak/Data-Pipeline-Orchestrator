"""
monitoring_integration.py
==========================
Integration bridge between the MonitoringAgent and the existing
Databricks executor (databricks_api.execute_pipeline) and dashboard.

Drop-in usage in databricks_api.py → execute_pipeline():
    from monitoring_integration import MonitoredPipelineExecutor
    executor = MonitoredPipelineExecutor()
    result = executor.run(csv_path, pipeline_config, schema, log_fn, progress_fn)

Or wrap around any existing execute_pipeline call:
    from monitoring_integration import wrap_execute_pipeline
    result = wrap_execute_pipeline(csv_path, pipeline_config, schema, log_fn, progress_fn)

The bridge:
  1. Emits lifecycle events (pipeline_started, job_started, job_succeeded/failed)
  2. Records queue times (time from job creation to first status = RUNNING)
  3. Tracks retry counts from Databricks run metadata
  4. Feeds real-time logs back through the monitoring agent event bus
  5. Provides post-run context to planner/optimizer agents
"""

import time
import uuid
import datetime
import threading
from typing import Callable, Optional

from monitoring_agent import monitoring_agent


# ══════════════════════════════════════════════════════════════════════════════
# MONITORED PIPELINE EXECUTOR
# ══════════════════════════════════════════════════════════════════════════════

class MonitoredPipelineExecutor:
    """
    Wraps the core execute_pipeline function with monitoring hooks.

    Automatically:
      - Assigns a pipeline_id (or uses the config name)
      - Emits start/complete/fail events
      - Tracks per-job queue time + execution time
      - Relays log messages to the monitoring agent event bus
      - Ensures the feedback loop back to planner/optimizer
    """

    def __init__(self):
        # Ensure monitoring agent is running
        if not monitoring_agent._running:
            monitoring_agent.start()

    def run(
        self,
        csv_path: str,
        pipeline_config: dict,
        schema: dict,
        log_fn: Callable = print,
        progress_fn: Optional[Callable] = None,
    ) -> dict:
        """
        Execute pipeline with full monitoring instrumentation.
        Returns the same dict as execute_pipeline().
        """
        from databricks_api import execute_pipeline

        pipeline_id = self._make_pipeline_id(pipeline_config)
        pipeline_name = pipeline_config.get("execution_order", ["pipeline"])[0] or pipeline_id
        total_stages = len(pipeline_config.get("execution_order", []))

        # ── Monitoring-aware log wrapper ──
        def monitored_log(msg: str):
            log_fn(msg)
            _relay_log_to_agent(msg, pipeline_id)

        # ── Emit pipeline_started ──
        monitoring_agent.on_pipeline_started(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
        )

        start_wall = time.time()

        # ── Patch execute_pipeline to intercept per-job events ──
        # We replace the log_fn so we can parse job lifecycle messages
        intercepting_log = _JobLifecycleInterceptor(
            pipeline_id=pipeline_id,
            base_log_fn=monitored_log,
        )

        try:
            result = execute_pipeline(
                csv_path=csv_path,
                pipeline_config=pipeline_config,
                schema=schema,
                log_fn=intercepting_log.log,
                progress_fn=progress_fn,
            )

            if result.get("status") == "ok":
                # Estimate records processed from output CSV
                total_records = _estimate_records(result.get("output_csv_bytes", b""))
                monitoring_agent.on_pipeline_completed(
                    pipeline_id=pipeline_id,
                    total_records=total_records,
                )
                # Provide feedback context to planner
                result["monitoring"] = {
                    "pipeline_id": pipeline_id,
                    "planner_context":   monitoring_agent.get_planner_context(),
                    "optimizer_context": monitoring_agent.get_optimizer_context(),
                    "alerts":            monitoring_agent.get_executor_alerts(),
                }
            else:
                reason = result.get("message", "unknown error")
                monitoring_agent.on_pipeline_failed(
                    pipeline_id=pipeline_id,
                    reason=reason,
                )
                result["monitoring"] = {
                    "pipeline_id": pipeline_id,
                    "alerts":      monitoring_agent.get_executor_alerts(),
                }

            return result

        except Exception as e:
            monitoring_agent.on_pipeline_failed(
                pipeline_id=pipeline_id,
                reason=str(e),
            )
            raise


    @staticmethod
    def _make_pipeline_id(pipeline_config: dict) -> str:
        """Generate a unique pipeline ID from config or UUID."""
        orders = pipeline_config.get("execution_order", [])
        base = orders[0].split("_")[1] if orders else "pipeline"
        ts = datetime.datetime.utcnow().strftime("%H%M%S")
        return f"{base}_{ts}_{uuid.uuid4().hex[:6]}"


# ══════════════════════════════════════════════════════════════════════════════
# JOB LIFECYCLE INTERCEPTOR
# ══════════════════════════════════════════════════════════════════════════════

class _JobLifecycleInterceptor:
    """
    Parses log messages from execute_pipeline to detect job lifecycle moments.
    Emits job_started / job_succeeded / job_failed events based on log patterns.

    Patterns matched (from databricks_api.py):
        "Creating job + triggering run: {pl_name}"  → job_started
        "Pipeline '{pl_name}' succeeded"            → job_succeeded
        "Pipeline '{pl_name}' {status}: {msg}"      → job_failed
        "Run {run_id} -> {lifecycle}..."             → track run_id
    """

    def __init__(self, pipeline_id: str, base_log_fn: Callable):
        self.pipeline_id = pipeline_id
        self._base_log = base_log_fn
        self._current_job_name: Optional[str] = None
        self._current_run_id: Optional[str] = None
        self._current_job_id: Optional[str] = None
        self._job_start_wall: Optional[float] = None
        self._job_created_wall: Optional[float] = None
        self._lock = threading.Lock()

    def log(self, msg: str):
        """Intercept log messages and emit monitoring events."""
        self._base_log(msg)
        msg_lower = msg.lower()

        # "Creating job + triggering run: Pipeline_Xxx_to_Yyy"
        if "creating job + triggering run:" in msg_lower:
            job_name = msg.split(":", 1)[-1].strip()
            with self._lock:
                self._current_job_name = job_name
                self._job_created_wall = time.time()

        # "Job created: DB_Pipeline_... (id=12345)"
        elif "job created:" in msg_lower and "id=" in msg_lower:
            try:
                job_id = msg.split("id=")[1].rstrip(")")
                with self._lock:
                    self._current_job_id = job_id
            except Exception:
                pass

        # "Run triggered: run_id=67890"
        elif "run triggered:" in msg_lower and "run_id=" in msg_lower:
            try:
                run_id = msg.split("run_id=")[1].strip()
                queue_time = 0.0
                with self._lock:
                    self._current_run_id = run_id
                    if self._job_created_wall:
                        queue_time = time.time() - self._job_created_wall
                    self._job_start_wall = time.time()
                monitoring_agent.on_job_started(
                    run_id=run_id,
                    job_id=self._current_job_id or run_id,
                    pipeline_id=self.pipeline_id,
                    job_name=self._current_job_name or "unknown",
                    queue_time_s=round(queue_time, 2),
                )
            except Exception:
                pass

        # "Run 67890 succeeded"
        elif "succeeded" in msg_lower and "run" in msg_lower:
            with self._lock:
                run_id = self._current_run_id
            if run_id:
                monitoring_agent.on_job_succeeded(run_id=run_id)

        # "Run 67890 FAILED"
        elif ("failed" in msg_lower or "fail" in msg_lower) and "run" in msg_lower:
            with self._lock:
                run_id = self._current_run_id
            if run_id:
                monitoring_agent.on_job_failed(
                    run_id=run_id,
                    failure_reason=msg,
                )

        # "Pipeline 'xxx' succeeded" → job-level success confirmation
        elif "pipeline '" in msg_lower and "succeeded" in msg_lower:
            with self._lock:
                run_id = self._current_run_id
            if run_id:
                monitoring_agent.on_job_succeeded(run_id=run_id)
                with self._lock:
                    self._current_run_id = None
                    self._current_job_name = None


# ══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE WRAPPER
# ══════════════════════════════════════════════════════════════════════════════

def wrap_execute_pipeline(
    csv_path: str,
    pipeline_config: dict,
    schema: dict,
    log_fn: Callable = print,
    progress_fn: Optional[Callable] = None,
) -> dict:
    """
    Drop-in replacement for databricks_api.execute_pipeline().
    Adds full monitoring without changing the call signature.

    Replace in your code:
        from databricks_api import execute_pipeline
        result = execute_pipeline(csv_path, pipeline_config, schema, log_fn, progress_fn)

    With:
        from monitoring_integration import wrap_execute_pipeline
        result = wrap_execute_pipeline(csv_path, pipeline_config, schema, log_fn, progress_fn)
    """
    executor = MonitoredPipelineExecutor()
    return executor.run(csv_path, pipeline_config, schema, log_fn, progress_fn)


# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD MONITOR FEED  (for databricks_dashboard.py)
# ══════════════════════════════════════════════════════════════════════════════

def get_dashboard_monitor_data() -> dict:
    """
    Returns a pre-formatted dict for the Streamlit dashboard monitor section.
    Call from stage_monitor() in databricks_dashboard.py.
    """
    metrics = monitoring_agent.get_metrics()
    alerts  = monitoring_agent.get_executor_alerts()
    anomalies = monitoring_agent.get_anomalies(limit=10)

    current = metrics.get("current_resource", {})
    system  = metrics.get("system", {})

    # Pre-formatted monitor cards
    cards = [
        {
            "label": "Active Pipelines",
            "value": str(system.get("number_of_active_pipelines", 0)),
            "sub":   f"{system.get('number_of_total_pipelines', 0)} total",
            "class": "mon-card-blue",
        },
        {
            "label": "CPU Usage",
            "value": f"{current.get('cpu_percent', 0):.1f}%",
            "sub":   f"avg {system.get('cluster_cpu_avg_pct', 0):.1f}% (last 5m)",
            "class": "mon-card-warn" if current.get("cpu_percent", 0) > 70 else "mon-card-ok",
        },
        {
            "label": "Memory",
            "value": f"{current.get('memory_percent', 0):.1f}%",
            "sub":   f"{current.get('memory_mb', 0):.0f} MB used",
            "class": "mon-card-warn" if current.get("memory_percent", 0) > 75 else "mon-card-ok",
        },
        {
            "label": "Anomalies",
            "value": str(metrics.get("anomaly_count", 0)),
            "sub":   f"{len(alerts)} active alerts",
            "class": "mon-card-err" if metrics.get("anomaly_count", 0) > 0 else "mon-card-ok",
        },
    ]

    return {
        "cards":     cards,
        "alerts":    alerts,
        "anomalies": anomalies,
        "metrics":   metrics,
    }


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _relay_log_to_agent(msg: str, pipeline_id: str):
    """Push a log line as a monitoring event (for audit trail)."""
    monitoring_agent._emit_event(
        "log_line",
        pipeline_id=pipeline_id,
        metadata={"message": msg[:300]},
    )


def _estimate_records(csv_bytes: bytes) -> int:
    """Estimate row count from output CSV bytes."""
    if not csv_bytes:
        return 0
    try:
        text = csv_bytes.decode("utf-8", errors="ignore")
        return max(0, text.count("\n") - 1)  # subtract header row
    except Exception:
        return 0