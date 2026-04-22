"""
monitoring_agent.py
===================
Core Monitoring Agent for the Databricks Pipeline Orchestrator.

Responsibilities:
  - Collect pipeline-level, job-level, resource, cost, and system metrics
  - Track events (pipeline_started, job_failed, anomaly_detected, etc.)
  - Detect performance, resource, and failure anomalies (rule-based)
  - Store structured time-series records in-memory + JSON log files
  - Expose structured data to planner / optimizer agents
  - Enable the closed feedback loop: Execution → Monitoring → Analysis → Optimization

Usage (standalone):
    from monitoring_agent import MonitoringAgent
    agent = MonitoringAgent()
    agent.start()   # begins background collection thread
    ...
    agent.stop()

Usage (from dashboard / executor):
    from monitoring_agent import monitoring_agent   # shared singleton
"""

import os
import json
import time
import threading
import logging
import psutil
import datetime
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from typing import Optional

# ── Logging setup ──────────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitoring_logs")
os.makedirs(LOG_DIR, exist_ok=True)

_logger = logging.getLogger("monitoring_agent")
_logger.setLevel(logging.DEBUG)

_fh = logging.FileHandler(os.path.join(LOG_DIR, "agent.log"), encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))
_logger.addHandler(_fh)

_sh = logging.StreamHandler()
_sh.setLevel(logging.WARNING)
_logger.addHandler(_sh)


# ══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ResourceSnapshot:
    """Point-in-time resource reading from psutil."""
    timestamp: str
    cpu_percent: float
    memory_mb: float
    memory_percent: float
    disk_read_mb: float
    disk_write_mb: float
    net_sent_mb: float
    net_recv_mb: float
    active_threads: int


@dataclass
class JobMetrics:
    """Metrics tracked per Databricks job run."""
    job_id: str
    run_id: str
    pipeline_id: str
    job_name: str
    status: str                         # running | succeeded | failed | timeout
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    execution_time_s: Optional[float] = None
    queue_time_s: Optional[float] = None
    retry_count: int = 0
    failure_reason: Optional[str] = None
    dependency_delay_s: float = 0.0
    throughput_rps: Optional[float] = None  # records per second
    expected_duration_s: Optional[float] = None


@dataclass
class PipelineMetrics:
    """Aggregate metrics for an entire pipeline run."""
    pipeline_id: str
    pipeline_name: str
    status: str                          # running | completed | failed
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    total_duration_s: Optional[float] = None
    throughput_rps: Optional[float] = None
    success_rate: float = 0.0
    total_jobs: int = 0
    successful_jobs: int = 0
    failed_jobs: int = 0


@dataclass
class CostMetrics:
    """Cost estimates (DBU-based, configurable rate)."""
    pipeline_id: str
    timestamp: str
    cost_per_job: float = 0.0
    cost_per_pipeline: float = 0.0
    cost_per_dbu: float = 0.07          # $/DBU default
    idle_resource_cost: float = 0.0
    estimated_dbus: float = 0.0


@dataclass
class AnomalyRecord:
    """Structured anomaly event."""
    timestamp: str
    anomaly_type: str                   # slow_job | cpu_spike | memory_leak | idle_resource | repeated_failure | cascade_failure
    severity: str                       # warning | critical
    pipeline_id: Optional[str]
    job_id: Optional[str]
    description: str
    metric_value: Optional[float] = None
    threshold: Optional[float] = None


@dataclass
class MonitoringEvent:
    """Event-driven tracking record."""
    timestamp: str
    event_type: str                     # pipeline_started | job_started | job_failed | resource_scaled_up | anomaly_detected
    pipeline_id: Optional[str]
    job_id: Optional[str]
    metadata: dict = field(default_factory=dict)


# ══════════════════════════════════════════════════════════════════════════════
# THRESHOLDS (tunable)
# ══════════════════════════════════════════════════════════════════════════════

THRESHOLDS = {
    "cpu_spike_pct":         80.0,   # % — flag if above this
    "memory_high_pct":       85.0,   # % — flag if above this
    "job_slow_multiplier":    1.5,   # flag if actual > expected * 1.5
    "idle_cpu_pct":           5.0,   # % — resource allocated but barely used
    "repeated_failure_count": 3,     # N consecutive failures
    "throughput_drop_pct":   40.0,   # % drop from baseline triggers alert
}


# ══════════════════════════════════════════════════════════════════════════════
# MONITORING AGENT
# ══════════════════════════════════════════════════════════════════════════════

class MonitoringAgent:
    """
    The central monitoring agent.

    Architecture:
        - Background collection thread polls system metrics every POLL_INTERVAL seconds
        - Event bus (deque) holds recent events for real-time consumers
        - In-memory stores indexed by pipeline_id / job_id
        - JSON log files flushed periodically for persistence
        - Anomaly detector runs after every metric collection cycle

    Interfaces to other agents:
        - get_planner_context()   → historical execution times, resource patterns
        - get_optimizer_context() → inefficiencies, cost vs performance
        - get_executor_alerts()   → real-time alerts (kill / restart triggers)
        - get_metrics()           → full metrics snapshot (API /metrics)
        - get_anomalies()         → list of detected anomalies
    """

    POLL_INTERVAL = 5          # seconds between resource polls
    MAX_HISTORY   = 1000       # max resource snapshots kept in memory
    MAX_EVENTS    = 500        # max events in the event bus

    def __init__(self, dbu_rate: float = 0.07):
        self.dbu_rate = dbu_rate
        self._running = False
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None

        # ── Time-series ring buffers ──
        self._resource_history: deque = deque(maxlen=self.MAX_HISTORY)
        self._event_bus: deque = deque(maxlen=self.MAX_EVENTS)

        # ── Indexed stores ──
        self._pipelines:  dict[str, PipelineMetrics]  = {}
        self._jobs:       dict[str, JobMetrics]        = {}  # keyed by run_id
        self._anomalies:  list[AnomalyRecord]          = []
        self._cost_store: dict[str, CostMetrics]       = {}

        # ── Baseline learning ──
        # Maps job_name → list of historical durations (for anomaly detection)
        self._duration_baselines: dict[str, list] = defaultdict(list)
        # Maps pipeline_id → list of historical throughputs
        self._throughput_baselines: dict[str, list] = defaultdict(list)

        # ── Failure tracking ──
        self._consecutive_failures: dict[str, int] = defaultdict(int)  # keyed by job_name

        # ── psutil baseline for delta calculations ──
        self._prev_disk_io  = psutil.disk_io_counters()
        self._prev_net_io   = psutil.net_io_counters()
        self._prev_sample_ts = time.time()

        # ── System-level counters ──
        self._active_pipeline_count: int = 0
        self._scheduling_latencies: deque = deque(maxlen=100)

        _logger.info("MonitoringAgent initialised (dbu_rate=%.4f)", dbu_rate)

    # ══════════════════════════════════════════════════════════════════════════
    # LIFECYCLE
    # ══════════════════════════════════════════════════════════════════════════

    def start(self):
        """Start the background metrics collection thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._collection_loop,
            daemon=True,
            name="MonitoringAgent-Collector",
        )
        self._thread.start()
        _logger.info("MonitoringAgent started")

    def stop(self):
        """Gracefully stop the background thread and flush logs."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        self._flush_logs()
        _logger.info("MonitoringAgent stopped")

    # ══════════════════════════════════════════════════════════════════════════
    # PIPELINE LIFECYCLE HOOKS  (called by executor / dashboard)
    # ══════════════════════════════════════════════════════════════════════════

    def on_pipeline_started(self, pipeline_id: str, pipeline_name: str):
        """Call this when a pipeline execution begins."""
        ts = _now()
        with self._lock:
            self._pipelines[pipeline_id] = PipelineMetrics(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline_name,
                status="running",
                start_time=ts,
            )
            self._active_pipeline_count += 1
        self._emit_event("pipeline_started", pipeline_id=pipeline_id,
                         metadata={"name": pipeline_name})
        _logger.info("Pipeline started: %s (%s)", pipeline_name, pipeline_id)

    def on_pipeline_completed(self, pipeline_id: str, total_records: int = 0):
        """Call this when a pipeline finishes successfully."""
        ts = _now()
        with self._lock:
            p = self._pipelines.get(pipeline_id)
            if p:
                p.status = "completed"
                p.end_time = ts
                if p.start_time:
                    dur = _delta_seconds(p.start_time, ts)
                    p.total_duration_s = dur
                    if total_records > 0 and dur > 0:
                        p.throughput_rps = total_records / dur
                        self._throughput_baselines[pipeline_id].append(p.throughput_rps)

                # Calculate success rate across jobs in this pipeline
                jobs_in_pipeline = [j for j in self._jobs.values()
                                    if j.pipeline_id == pipeline_id]
                p.total_jobs = len(jobs_in_pipeline)
                p.successful_jobs = sum(1 for j in jobs_in_pipeline if j.status == "succeeded")
                p.failed_jobs = sum(1 for j in jobs_in_pipeline if j.status == "failed")
                p.success_rate = (p.successful_jobs / p.total_jobs * 100) if p.total_jobs else 100.0

                self._active_pipeline_count = max(0, self._active_pipeline_count - 1)

        self._emit_event("pipeline_completed", pipeline_id=pipeline_id)
        self._update_cost(pipeline_id)
        _logger.info("Pipeline completed: %s", pipeline_id)

    def on_pipeline_failed(self, pipeline_id: str, reason: str = ""):
        """Call this when a pipeline fails."""
        ts = _now()
        with self._lock:
            p = self._pipelines.get(pipeline_id)
            if p:
                p.status = "failed"
                p.end_time = ts
                if p.start_time:
                    p.total_duration_s = _delta_seconds(p.start_time, ts)
                self._active_pipeline_count = max(0, self._active_pipeline_count - 1)

        self._emit_event("pipeline_failed", pipeline_id=pipeline_id,
                         metadata={"reason": reason})
        _logger.warning("Pipeline FAILED: %s — %s", pipeline_id, reason)

    # ══════════════════════════════════════════════════════════════════════════
    # JOB LIFECYCLE HOOKS
    # ══════════════════════════════════════════════════════════════════════════

    def on_job_started(self, run_id: str, job_id: str, pipeline_id: str,
                       job_name: str, queue_time_s: float = 0.0):
        """Call when a Databricks job run starts executing."""
        ts = _now()
        sched_latency = queue_time_s
        with self._lock:
            self._jobs[run_id] = JobMetrics(
                job_id=job_id,
                run_id=run_id,
                pipeline_id=pipeline_id,
                job_name=job_name,
                status="running",
                start_time=ts,
                queue_time_s=queue_time_s,
                expected_duration_s=self._get_expected_duration(job_name),
            )
            if sched_latency > 0:
                self._scheduling_latencies.append(sched_latency)

        self._emit_event("job_started", pipeline_id=pipeline_id, job_id=run_id,
                         metadata={"job_name": job_name, "queue_time_s": queue_time_s})
        _logger.info("Job started: %s (run=%s)", job_name, run_id)

    def on_job_succeeded(self, run_id: str, records_processed: int = 0):
        """Call when a Databricks job run succeeds."""
        ts = _now()
        with self._lock:
            j = self._jobs.get(run_id)
            if j:
                j.status = "succeeded"
                j.end_time = ts
                if j.start_time:
                    dur = _delta_seconds(j.start_time, ts)
                    j.execution_time_s = dur
                    self._duration_baselines[j.job_name].append(dur)
                    # Keep only last 50 durations
                    self._duration_baselines[j.job_name] = \
                        self._duration_baselines[j.job_name][-50:]
                if records_processed > 0 and j.execution_time_s and j.execution_time_s > 0:
                    j.throughput_rps = records_processed / j.execution_time_s
                # Reset consecutive failure counter
                self._consecutive_failures[j.job_name] = 0
                # Detect slow job anomaly
                self._check_slow_job(j)

        _logger.info("Job succeeded: run=%s", run_id)

    def on_job_failed(self, run_id: str, failure_reason: str = "",
                      retry_count: int = 0):
        """Call when a Databricks job run fails."""
        ts = _now()
        with self._lock:
            j = self._jobs.get(run_id)
            if j:
                j.status = "failed"
                j.end_time = ts
                j.failure_reason = failure_reason
                j.retry_count = retry_count
                if j.start_time:
                    j.execution_time_s = _delta_seconds(j.start_time, ts)
                self._consecutive_failures[j.job_name] += 1
                # Detect repeated failure / cascade patterns
                self._check_failure_patterns(j)

        self._emit_event("job_failed", pipeline_id=None, job_id=run_id,
                         metadata={"reason": failure_reason, "retry_count": retry_count})
        _logger.warning("Job FAILED: run=%s — %s", run_id, failure_reason)

    def on_resource_scaled(self, direction: str, resource_type: str,
                           pipeline_id: str = None):
        """Call when a resource is scaled up or down."""
        event_type = "resource_scaled_up" if direction == "up" else "resource_scaled_down"
        self._emit_event(event_type, pipeline_id=pipeline_id,
                         metadata={"resource_type": resource_type, "direction": direction})
        _logger.info("Resource scaled %s: %s (pipeline=%s)", direction, resource_type, pipeline_id)

    # ══════════════════════════════════════════════════════════════════════════
    # BACKGROUND COLLECTION LOOP
    # ══════════════════════════════════════════════════════════════════════════

    def _collection_loop(self):
        """Runs in background thread — collects system metrics every POLL_INTERVAL seconds."""
        log_flush_counter = 0
        while self._running:
            try:
                snapshot = self._collect_resource_snapshot()
                with self._lock:
                    self._resource_history.append(snapshot)

                # Anomaly detection on every sample
                self._detect_resource_anomalies(snapshot)

                # Flush JSON logs every 60 seconds (12 cycles at 5s)
                log_flush_counter += 1
                if log_flush_counter >= 12:
                    self._flush_logs()
                    log_flush_counter = 0

            except Exception as e:
                _logger.error("Collection loop error: %s", e)

            time.sleep(self.POLL_INTERVAL)

    def _collect_resource_snapshot(self) -> ResourceSnapshot:
        """Collect one psutil resource snapshot with delta I/O calculations."""
        now_ts = time.time()
        elapsed = max(now_ts - self._prev_sample_ts, 0.001)

        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()

        # Disk I/O deltas (MB/s → total MB in interval)
        curr_disk = psutil.disk_io_counters()
        disk_read_mb  = 0.0
        disk_write_mb = 0.0
        if curr_disk and self._prev_disk_io:
            disk_read_mb  = (curr_disk.read_bytes  - self._prev_disk_io.read_bytes)  / 1_048_576
            disk_write_mb = (curr_disk.write_bytes - self._prev_disk_io.write_bytes) / 1_048_576
        self._prev_disk_io = curr_disk

        # Network I/O deltas
        curr_net = psutil.net_io_counters()
        net_sent_mb = 0.0
        net_recv_mb = 0.0
        if curr_net and self._prev_net_io:
            net_sent_mb = (curr_net.bytes_sent - self._prev_net_io.bytes_sent) / 1_048_576
            net_recv_mb = (curr_net.bytes_recv - self._prev_net_io.bytes_recv) / 1_048_576
        self._prev_net_io = curr_net
        self._prev_sample_ts = now_ts

        return ResourceSnapshot(
            timestamp=_now(),
            cpu_percent=round(cpu, 2),
            memory_mb=round(mem.used / 1_048_576, 2),
            memory_percent=round(mem.percent, 2),
            disk_read_mb=round(max(disk_read_mb, 0), 4),
            disk_write_mb=round(max(disk_write_mb, 0), 4),
            net_sent_mb=round(max(net_sent_mb, 0), 4),
            net_recv_mb=round(max(net_recv_mb, 0), 4),
            active_threads=threading.active_count(),
        )

    # ══════════════════════════════════════════════════════════════════════════
    # ANOMALY DETECTION
    # ══════════════════════════════════════════════════════════════════════════

    def _detect_resource_anomalies(self, snapshot: ResourceSnapshot):
        """Rule-based resource anomaly detection on each new snapshot."""
        ts = snapshot.timestamp

        # A. CPU spike
        if snapshot.cpu_percent > THRESHOLDS["cpu_spike_pct"]:
            self._record_anomaly(AnomalyRecord(
                timestamp=ts,
                anomaly_type="cpu_spike",
                severity="critical" if snapshot.cpu_percent > 95 else "warning",
                pipeline_id=self._get_active_pipeline_id(),
                job_id=None,
                description=f"CPU usage at {snapshot.cpu_percent:.1f}%",
                metric_value=snapshot.cpu_percent,
                threshold=THRESHOLDS["cpu_spike_pct"],
            ))

        # B. Memory pressure
        if snapshot.memory_percent > THRESHOLDS["memory_high_pct"]:
            self._record_anomaly(AnomalyRecord(
                timestamp=ts,
                anomaly_type="memory_pressure",
                severity="critical" if snapshot.memory_percent > 95 else "warning",
                pipeline_id=self._get_active_pipeline_id(),
                job_id=None,
                description=f"Memory usage at {snapshot.memory_percent:.1f}% ({snapshot.memory_mb:.0f} MB)",
                metric_value=snapshot.memory_percent,
                threshold=THRESHOLDS["memory_high_pct"],
            ))

        # C. Memory leak detection (rising trend over last 20 samples)
        self._check_memory_leak_trend()

        # D. Idle resource: CPU very low but pipeline is running
        if (snapshot.cpu_percent < THRESHOLDS["idle_cpu_pct"]
                and self._active_pipeline_count > 0):
            self._record_anomaly(AnomalyRecord(
                timestamp=ts,
                anomaly_type="idle_resource",
                severity="warning",
                pipeline_id=self._get_active_pipeline_id(),
                job_id=None,
                description=f"Pipeline running but CPU only {snapshot.cpu_percent:.1f}% — possible bottleneck or stall",
                metric_value=snapshot.cpu_percent,
                threshold=THRESHOLDS["idle_cpu_pct"],
            ))

    def _check_slow_job(self, job: JobMetrics):
        """Detect slow job anomaly post-completion."""
        if job.expected_duration_s and job.execution_time_s:
            threshold = job.expected_duration_s * THRESHOLDS["job_slow_multiplier"]
            if job.execution_time_s > threshold:
                self._record_anomaly(AnomalyRecord(
                    timestamp=_now(),
                    anomaly_type="slow_job",
                    severity="warning",
                    pipeline_id=job.pipeline_id,
                    job_id=job.run_id,
                    description=(
                        f"Job '{job.job_name}' took {job.execution_time_s:.1f}s "
                        f"(expected ≤{threshold:.1f}s, {THRESHOLDS['job_slow_multiplier']}× threshold)"
                    ),
                    metric_value=job.execution_time_s,
                    threshold=threshold,
                ))

    def _check_failure_patterns(self, job: JobMetrics):
        """Detect repeated and cascade failures."""
        count = self._consecutive_failures.get(job.job_name, 0)

        if count >= THRESHOLDS["repeated_failure_count"]:
            self._record_anomaly(AnomalyRecord(
                timestamp=_now(),
                anomaly_type="repeated_failure",
                severity="critical",
                pipeline_id=job.pipeline_id,
                job_id=job.run_id,
                description=(
                    f"Job '{job.job_name}' has failed {count} consecutive times. "
                    f"Last reason: {job.failure_reason or 'unknown'}"
                ),
                metric_value=float(count),
                threshold=float(THRESHOLDS["repeated_failure_count"]),
            ))

        # Cascade detection: ≥2 different jobs in same pipeline failing within short window
        if job.pipeline_id:
            recent_failures_in_pipeline = [
                j for j in self._jobs.values()
                if j.pipeline_id == job.pipeline_id
                and j.status == "failed"
                and j.run_id != job.run_id
                and j.end_time
                and _delta_seconds(j.end_time, _now()) < 120  # within last 2 min
            ]
            if len(recent_failures_in_pipeline) >= 1:
                self._record_anomaly(AnomalyRecord(
                    timestamp=_now(),
                    anomaly_type="cascade_failure",
                    severity="critical",
                    pipeline_id=job.pipeline_id,
                    job_id=job.run_id,
                    description=(
                        f"Cascade failure detected in pipeline '{job.pipeline_id}': "
                        f"{len(recent_failures_in_pipeline) + 1} jobs failed in quick succession"
                    ),
                ))

    def _check_memory_leak_trend(self):
        """Detect monotonically rising memory over recent samples (possible leak)."""
        with self._lock:
            recent = list(self._resource_history)[-20:]
        if len(recent) < 15:
            return
        values = [s.memory_percent for s in recent]
        # Check if each value is >= previous (monotone rise)
        rising_count = sum(1 for i in range(1, len(values)) if values[i] >= values[i - 1])
        if rising_count >= 14:  # 14/19 transitions rising
            self._record_anomaly(AnomalyRecord(
                timestamp=_now(),
                anomaly_type="memory_leak",
                severity="warning",
                pipeline_id=self._get_active_pipeline_id(),
                job_id=None,
                description=(
                    f"Memory has been rising steadily for {len(recent)} samples "
                    f"({values[0]:.1f}% → {values[-1]:.1f}%). Possible memory leak."
                ),
                metric_value=values[-1],
                threshold=None,
            ))

    def _record_anomaly(self, anomaly: AnomalyRecord):
        """Record anomaly if not a duplicate of a very recent identical event."""
        with self._lock:
            # Deduplicate: suppress same anomaly_type within 30 seconds
            for a in reversed(self._anomalies[-10:]):
                if (a.anomaly_type == anomaly.anomaly_type
                        and a.pipeline_id == anomaly.pipeline_id
                        and _delta_seconds(a.timestamp, anomaly.timestamp) < 30):
                    return  # suppress duplicate
            self._anomalies.append(anomaly)

        self._emit_event("anomaly_detected",
                         pipeline_id=anomaly.pipeline_id,
                         metadata={
                             "type": anomaly.anomaly_type,
                             "severity": anomaly.severity,
                             "description": anomaly.description,
                         })
        _logger.warning("ANOMALY [%s/%s]: %s", anomaly.severity, anomaly.anomaly_type,
                        anomaly.description)

    # ══════════════════════════════════════════════════════════════════════════
    # COST ESTIMATION
    # ══════════════════════════════════════════════════════════════════════════

    def _update_cost(self, pipeline_id: str):
        """Estimate cost for a completed pipeline (simple DBU model)."""
        with self._lock:
            p = self._pipelines.get(pipeline_id)
            if not p or not p.total_duration_s:
                return

            # 1 DBU ≈ 1 Spark-hour on a small cluster (very rough estimate)
            hours = p.total_duration_s / 3600
            dbus = hours * 1.0
            cost_pipeline = dbus * self.dbu_rate
            cost_per_job = cost_pipeline / max(p.total_jobs, 1)

            # Idle cost: if CPU was low during run, some resource was wasted
            recent = list(self._resource_history)[-int(p.total_duration_s / self.POLL_INTERVAL):]
            if recent:
                avg_cpu = statistics.mean(s.cpu_percent for s in recent)
                idle_fraction = max(0, (THRESHOLDS["idle_cpu_pct"] * 2 - avg_cpu) / 100)
                idle_cost = cost_pipeline * idle_fraction
            else:
                idle_cost = 0.0

            self._cost_store[pipeline_id] = CostMetrics(
                pipeline_id=pipeline_id,
                timestamp=_now(),
                cost_per_job=round(cost_per_job, 6),
                cost_per_pipeline=round(cost_pipeline, 6),
                cost_per_dbu=self.dbu_rate,
                idle_resource_cost=round(idle_cost, 6),
                estimated_dbus=round(dbus, 4),
            )

    # ══════════════════════════════════════════════════════════════════════════
    # BASELINE / HISTORY HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    def _get_expected_duration(self, job_name: str) -> Optional[float]:
        """Return median historical duration for a job, or None if no history."""
        history = self._duration_baselines.get(job_name, [])
        if len(history) >= 3:
            return statistics.median(history)
        return None

    def _get_active_pipeline_id(self) -> Optional[str]:
        """Return first running pipeline_id found, or None."""
        with self._lock:
            for p in self._pipelines.values():
                if p.status == "running":
                    return p.pipeline_id
        return None

    # ══════════════════════════════════════════════════════════════════════════
    # EVENT BUS
    # ══════════════════════════════════════════════════════════════════════════

    def _emit_event(self, event_type: str, pipeline_id: str = None,
                    job_id: str = None, metadata: dict = None):
        """Push an event onto the event bus."""
        event = MonitoringEvent(
            timestamp=_now(),
            event_type=event_type,
            pipeline_id=pipeline_id,
            job_id=job_id,
            metadata=metadata or {},
        )
        with self._lock:
            self._event_bus.append(event)
        _logger.debug("EVENT %s | pipeline=%s job=%s", event_type, pipeline_id, job_id)

    # ══════════════════════════════════════════════════════════════════════════
    # PUBLIC API — METRICS & QUERY INTERFACES
    # ══════════════════════════════════════════════════════════════════════════

    def get_metrics(self) -> dict:
        """
        Full metrics snapshot — for GET /metrics endpoint.
        Returns system-level, pipeline, job, resource, cost data.
        """
        with self._lock:
            recent_resource = list(self._resource_history)[-1:] or [None]
            latest = recent_resource[0]
            history_window = list(self._resource_history)[-60:]  # last 5 min

        avg_cpu = round(statistics.mean(s.cpu_percent for s in history_window), 2) \
            if history_window else 0.0
        avg_mem = round(statistics.mean(s.memory_percent for s in history_window), 2) \
            if history_window else 0.0

        sched_latencies = list(self._scheduling_latencies)
        avg_sched_latency = round(statistics.mean(sched_latencies), 2) \
            if sched_latencies else 0.0

        with self._lock:
            pipelines_snapshot = {k: asdict(v) for k, v in self._pipelines.items()}
            jobs_snapshot      = {k: asdict(v) for k, v in self._jobs.items()}
            costs_snapshot     = {k: asdict(v) for k, v in self._cost_store.items()}

        return {
            "timestamp": _now(),
            "system": {
                "number_of_active_pipelines": self._active_pipeline_count,
                "number_of_total_pipelines":  len(self._pipelines),
                "number_of_jobs":             len(self._jobs),
                "cluster_cpu_avg_pct":        avg_cpu,
                "cluster_memory_avg_pct":     avg_mem,
                "scheduling_latency_avg_s":   avg_sched_latency,
                "active_threads":             latest.active_threads if latest else 0,
            },
            "current_resource": asdict(latest) if latest else {},
            "pipelines":        pipelines_snapshot,
            "jobs":             jobs_snapshot,
            "costs":            costs_snapshot,
            "anomaly_count":    len(self._anomalies),
        }

    def get_pipeline_status(self, pipeline_id: str) -> dict:
        """
        Status for a specific pipeline — for GET /pipeline/{id}/status.
        Includes pipeline metrics, its jobs, anomalies, and cost.
        """
        with self._lock:
            p = self._pipelines.get(pipeline_id)
            if not p:
                return {"error": f"Pipeline '{pipeline_id}' not found"}

            jobs = [asdict(j) for j in self._jobs.values()
                    if j.pipeline_id == pipeline_id]
            anomalies = [asdict(a) for a in self._anomalies
                         if a.pipeline_id == pipeline_id]
            cost = asdict(self._cost_store[pipeline_id]) \
                if pipeline_id in self._cost_store else {}

        return {
            "pipeline":  asdict(p),
            "jobs":      jobs,
            "anomalies": anomalies,
            "cost":      cost,
        }

    def get_anomalies(self, severity: str = None, limit: int = 50) -> list:
        """
        All detected anomalies — for GET /anomalies.
        Optionally filter by severity ('warning' | 'critical').
        """
        with self._lock:
            results = [asdict(a) for a in self._anomalies]
        if severity:
            results = [a for a in results if a["severity"] == severity]
        return results[-limit:]

    def get_recent_events(self, limit: int = 50) -> list:
        """Return the most recent events from the event bus."""
        with self._lock:
            return [asdict(e) for e in list(self._event_bus)[-limit:]]

    def get_resource_history(self, last_n: int = 60) -> list:
        """Return the last N resource snapshots (default = last 5 min at 5s interval)."""
        with self._lock:
            return [asdict(s) for s in list(self._resource_history)[-last_n:]]

    # ══════════════════════════════════════════════════════════════════════════
    # INTER-AGENT INTERFACES
    # ══════════════════════════════════════════════════════════════════════════

    def get_planner_context(self) -> dict:
        """
        Structured data for the Planner Agent.
        Provides historical execution times and resource usage patterns.
        """
        with self._lock:
            # Historical duration summary per job name
            duration_summary = {}
            for job_name, durations in self._duration_baselines.items():
                if durations:
                    duration_summary[job_name] = {
                        "median_s":  round(statistics.median(durations), 2),
                        "mean_s":    round(statistics.mean(durations), 2),
                        "min_s":     round(min(durations), 2),
                        "max_s":     round(max(durations), 2),
                        "stddev_s":  round(statistics.stdev(durations), 2) if len(durations) > 1 else 0.0,
                        "samples":   len(durations),
                    }

            # Average resource usage across all history
            history = list(self._resource_history)

        resource_profile = {}
        if history:
            resource_profile = {
                "avg_cpu_pct":    round(statistics.mean(s.cpu_percent for s in history), 2),
                "peak_cpu_pct":   round(max(s.cpu_percent for s in history), 2),
                "avg_memory_pct": round(statistics.mean(s.memory_percent for s in history), 2),
                "peak_memory_mb": round(max(s.memory_mb for s in history), 2),
            }

        # Completed pipeline stats
        completed = [p for p in self._pipelines.values() if p.status == "completed"]
        pipeline_stats = {}
        if completed:
            durations = [p.total_duration_s for p in completed if p.total_duration_s]
            pipeline_stats = {
                "total_completed":     len(completed),
                "avg_duration_s":      round(statistics.mean(durations), 2) if durations else 0,
                "avg_success_rate":    round(statistics.mean(p.success_rate for p in completed), 2),
            }

        return {
            "job_duration_baselines": duration_summary,
            "resource_profile":       resource_profile,
            "pipeline_stats":         pipeline_stats,
            "recommended_workers":    self._recommend_workers(),
        }

    def get_optimizer_context(self) -> dict:
        """
        Structured data for the Optimizer Agent.
        Provides inefficiencies, anomalies, and cost vs performance trade-offs.
        """
        with self._lock:
            all_anomalies = [asdict(a) for a in self._anomalies]
            costs = {k: asdict(v) for k, v in self._cost_store.items()}
            jobs = list(self._jobs.values())

        # Classify inefficiencies
        slow_jobs = [asdict(j) for j in jobs
                     if j.execution_time_s and j.expected_duration_s
                     and j.execution_time_s > j.expected_duration_s * THRESHOLDS["job_slow_multiplier"]]

        failed_jobs = [asdict(j) for j in jobs if j.status == "failed"]

        high_retry_jobs = [asdict(j) for j in jobs if j.retry_count > 0]

        total_cost = sum(c["cost_per_pipeline"] for c in costs.values())
        total_idle_cost = sum(c["idle_resource_cost"] for c in costs.values())

        return {
            "inefficiencies": {
                "slow_jobs":        slow_jobs,
                "failed_jobs":      failed_jobs,
                "high_retry_jobs":  high_retry_jobs,
                "anomalies":        all_anomalies,
            },
            "cost_analysis": {
                "total_cost_usd":         round(total_cost, 4),
                "total_idle_cost_usd":    round(total_idle_cost, 4),
                "idle_waste_pct":         round(total_idle_cost / total_cost * 100, 2) if total_cost else 0,
                "per_pipeline":           costs,
            },
            "optimization_hints":    self._generate_optimization_hints(),
        }

    def get_executor_alerts(self) -> list:
        """
        Real-time alerts for the Execution Agent.
        Returns actionable items: kill, restart, scale signals.
        """
        alerts = []
        with self._lock:
            recent_anomalies = self._anomalies[-20:]

        for a in recent_anomalies:
            if _delta_seconds(a.timestamp, _now()) > 60:
                continue  # only truly recent alerts
            alert = {
                "timestamp":   a.timestamp,
                "severity":    a.severity,
                "type":        a.anomaly_type,
                "pipeline_id": a.pipeline_id,
                "job_id":      a.job_id,
                "description": a.description,
                "action":      self._suggest_action(a),
            }
            alerts.append(alert)
        return alerts

    # ══════════════════════════════════════════════════════════════════════════
    # INTELLIGENCE HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    def _recommend_workers(self) -> dict:
        """Suggest worker count based on recent CPU/memory patterns."""
        history = list(self._resource_history)[-60:]
        if not history:
            return {"suggestion": "no data"}
        avg_cpu = statistics.mean(s.cpu_percent for s in history)
        avg_mem = statistics.mean(s.memory_percent for s in history)
        if avg_cpu > 80 or avg_mem > 80:
            return {"suggestion": "scale_up", "reason": f"avg CPU={avg_cpu:.1f}%, mem={avg_mem:.1f}%"}
        if avg_cpu < 20 and avg_mem < 40:
            return {"suggestion": "scale_down", "reason": f"avg CPU={avg_cpu:.1f}%, mem={avg_mem:.1f}%"}
        return {"suggestion": "maintain", "reason": f"avg CPU={avg_cpu:.1f}%, mem={avg_mem:.1f}%"}

    def _generate_optimization_hints(self) -> list:
        """Generate plain-language optimization suggestions."""
        hints = []
        with self._lock:
            anomaly_types = [a.anomaly_type for a in self._anomalies[-50:]]
        counts = defaultdict(int)
        for t in anomaly_types:
            counts[t] += 1

        if counts["slow_job"] > 0:
            hints.append({
                "hint": "increase_shuffle_partitions",
                "reason": f"{counts['slow_job']} slow job(s) detected — consider raising shuffle_partitions",
            })
        if counts["cpu_spike"] > 0:
            hints.append({
                "hint": "add_workers",
                "reason": f"{counts['cpu_spike']} CPU spike(s) — consider adding more worker nodes",
            })
        if counts["memory_pressure"] > 0 or counts["memory_leak"] > 0:
            hints.append({
                "hint": "optimize_memory",
                "reason": "Memory pressure detected — check for wide transformations or data skew",
            })
        if counts["idle_resource"] > 2:
            hints.append({
                "hint": "reduce_cluster_size",
                "reason": f"{counts['idle_resource']} idle resource alerts — cluster may be over-provisioned",
            })
        if counts["repeated_failure"] > 0:
            hints.append({
                "hint": "review_job_logic",
                "reason": "Repeated job failures detected — review transformation logic and input data quality",
            })
        if counts["cascade_failure"] > 0:
            hints.append({
                "hint": "add_retry_and_isolation",
                "reason": "Cascade failures detected — add retry logic and isolate dependent tasks",
            })
        return hints

    def _suggest_action(self, anomaly: AnomalyRecord) -> str:
        """Map an anomaly to an executor action."""
        mapping = {
            "cpu_spike":        "scale_up_cluster",
            "memory_pressure":  "restart_job_with_more_memory",
            "memory_leak":      "restart_job",
            "idle_resource":    "scale_down_or_investigate",
            "slow_job":         "increase_shuffle_partitions",
            "repeated_failure": "pause_and_alert",
            "cascade_failure":  "pause_pipeline",
        }
        return mapping.get(anomaly.anomaly_type, "investigate")

    # ══════════════════════════════════════════════════════════════════════════
    # PERSISTENCE
    # ══════════════════════════════════════════════════════════════════════════

    def _flush_logs(self):
        """Write current state to structured JSON log files."""
        try:
            date_str = datetime.datetime.utcnow().strftime("%Y%m%d")

            def _write(filename, data):
                path = os.path.join(LOG_DIR, filename)
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, default=str)

            with self._lock:
                pipeline_data = {k: asdict(v) for k, v in self._pipelines.items()}
                jobs_data = {k: asdict(v) for k, v in self._jobs.items()}
                anomalies_data = [asdict(a) for a in self._anomalies]
                events_data = [asdict(e) for e in self._event_bus]
                resource_sample = list(self._resource_history)[-100:]

            _write(f"pipelines_{date_str}.json", pipeline_data)
            _write(f"jobs_{date_str}.json", jobs_data)
            _write(f"anomalies_{date_str}.json", anomalies_data)
            _write(f"events_{date_str}.json", events_data)
            _write(f"resources_{date_str}.json", [asdict(s) for s in resource_sample])

            _logger.debug("Logs flushed to %s", LOG_DIR)
        except Exception as e:
            _logger.error("Log flush failed: %s", e)

    def load_historical_baselines(self):
        """
        Load duration baselines from the most recent jobs log on startup.
        Enables anomaly detection from the very first run.
        """
        import glob
        pattern = os.path.join(LOG_DIR, "jobs_*.json")
        files = sorted(glob.glob(pattern))
        if not files:
            return
        latest = files[-1]
        try:
            with open(latest, "r", encoding="utf-8") as f:
                jobs_data = json.load(f)
            for job in jobs_data.values():
                if job.get("status") == "succeeded" and job.get("execution_time_s") and job.get("job_name"):
                    self._duration_baselines[job["job_name"]].append(job["execution_time_s"])
            _logger.info("Loaded baselines from %s (%d jobs)", latest, len(jobs_data))
        except Exception as e:
            _logger.warning("Could not load baselines: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _now() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _delta_seconds(start_iso: str, end_iso: str) -> float:
    """Return seconds between two ISO timestamp strings."""
    try:
        fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
        t1 = datetime.datetime.strptime(start_iso, fmt)
        t2 = datetime.datetime.strptime(end_iso, fmt)
        return (t2 - t1).total_seconds()
    except Exception:
        return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# SHARED SINGLETON  (import this in other modules)
# ══════════════════════════════════════════════════════════════════════════════

monitoring_agent = MonitoringAgent()
monitoring_agent.load_historical_baselines()