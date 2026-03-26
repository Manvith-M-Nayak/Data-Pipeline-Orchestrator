"""
monitoring_agent.py
--------------------
Monitors Azure Data Factory pipeline runs using the ADF Management SDK.

Detects:
  - Long-running pipelines (duration > threshold)
  - Failed / cancelled pipelines WITH root-cause activity errors
  - Anomalous run durations (> N std-deviations from recent history)
  - Per-activity drill-down (status, duration, error code + message)
  - Writes a structured JSON report for the dashboard to consume

Usage (instantiated in app.py / main.py after ADF client is created):
    agent = MonitoringAgent(adf_client, resource_group, factory_name)
    agent.monitor()
"""

from __future__ import annotations

import json
import statistics
from datetime import datetime, timezone, timedelta
from typing import Optional

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters


# ------------------------------------------------------------------ #
#  Tuneable thresholds                                                  #
# ------------------------------------------------------------------ #
DEFAULT_LONG_RUN_MINUTES: int   = 30
ANOMALY_STDDEV_FACTOR:    float = 2.0
LOOKBACK_HOURS:           int   = 24
MIN_HISTORY_FOR_ANOMALY:  int   = 5


class MonitoringAgent:
    """
    Fetches recent ADF pipeline runs and reports:
      1. Failures / cancellations + root-cause activity errors
      2. Long-running pipelines
      3. Statistical anomalies in run duration
      4. Resource usage, data flow, cloud compute metrics
      5. Always scans running pipelines (live status)
      6. Optional past pipeline scanning (toggle)
      7. Structured JSON report written to disk for the dashboard
    """

    def __init__(
        self,
        client: DataFactoryManagementClient,
        resource_group: str,
        factory_name: str,
        long_run_threshold_minutes: int = DEFAULT_LONG_RUN_MINUTES,
        report_path: str = "adf_monitoring_report.json",
        scan_past_pipelines: bool = True,
        silent: bool = False,
    ) -> None:
        self.client = client
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.long_run_threshold_minutes = long_run_threshold_minutes
        self.report_path = report_path
        self.scan_past_pipelines = scan_past_pipelines
        self.silent = silent

    # ---------------------------------------------------------------- #
    #  Public entry point                                                #
    # ---------------------------------------------------------------- #
    def monitor(self, silent: bool | None = None, limit: int = None) -> dict:
        """
        Fetch pipeline runs, print a structured health report, write JSON,
        and return the full report dict.
        
        Always scans running/in-progress pipelines for live status.
        Past pipeline scanning is controlled by scan_past_pipelines flag.
        """
        # Use instance silent if not overridden
        if silent is None:
            silent = self.silent
        
        def log(*args, **kwargs):
            if not silent:
                print(*args, **kwargs)
        
        log("\n" + "=" * 60)
        log("  MONITORING AGENT — ADF Pipeline Health Report")
        log("=" * 60)

        # Always fetch running pipelines for live monitoring
        running_runs = self._fetch_running_runs()
        
        # Fetch past runs only if enabled
        past_runs = []
        if self.scan_past_pipelines:
            past_runs = self._fetch_past_runs()
        
        # Combine runs - running first, then past
        all_runs = running_runs + past_runs

        # Sort newest first
        all_runs = sorted(
            all_runs,
            key=lambda r: r.run_start or datetime.min,
            reverse=True
        )

        # Apply limit if provided
        if limit:
            all_runs = all_runs[:limit]

        report = {
            "generated_at":      datetime.now(timezone.utc).isoformat(),
            "factory":            self.factory_name,
            "resource_group":    self.resource_group,
            "lookback_hours":    LOOKBACK_HOURS,
            "scan_past_pipelines": self.scan_past_pipelines,
            "total_runs":         len(all_runs),
            "running_runs":       len(running_runs),
            "past_runs":          len(past_runs),
            "pipelines":          {},
            "issues":             [],
            "summary":            {},
        }

        if not all_runs:
            msg = f"No pipeline runs found in the last {LOOKBACK_HOURS} hours."
            log(f"  {msg}\n")
            report["summary"] = {"status": "no_data", "message": msg}
            return report

        log(f"  Running pipelines: {len(running_runs)}")
        if self.scan_past_pipelines:
            log(f"  Past pipelines scanned: {len(past_runs)}")
        else:
            log(f"  Past pipeline scanning disabled")
        log(f"  Total runs: {len(all_runs)}\n")

        by_pipeline: dict[str, list] = {}
        for run in all_runs:
            by_pipeline.setdefault(run.pipeline_name, []).append(run)

        all_issues: list[str] = []

        for pipeline_name, pipeline_runs in by_pipeline.items():
            pipeline_data = self._analyse_pipeline(
                pipeline_name, pipeline_runs, all_issues, log
            )
            report["pipelines"][pipeline_name] = pipeline_data

        report["issues"] = all_issues

        # Use the already fetched runs (running + past)
        all_run_objects = running_runs + past_runs
        total     = len(all_run_objects)
        failed    = sum(1 for r in all_run_objects if r.status in ("Failed", "Cancelled"))
        succeeded = sum(1 for r in all_run_objects if r.status == "Succeeded")
        running   = sum(1 for r in all_run_objects if r.status in ("InProgress", "Queued", "Running"))
        report["summary"] = {
            "status":        "issues_found" if all_issues else "healthy",
            "total_runs":    total,
            "running_runs":  running,
            "completed_runs": total - running,
            "succeeded":     succeeded,
            "failed":        failed,
            "in_progress":   running,
            "issue_count":   len(all_issues),
            "success_rate":  round(succeeded / (total - running) * 100, 1) if (total - running) else 0,
        }

        log("-" * 60)
        if all_issues:
            log(f"  ⚠  {len(all_issues)} issue(s) detected:\n")
            for i, issue in enumerate(all_issues, 1):
                log(f"    {i}. {issue}")
        else:
            log("  ✓  All pipelines look healthy.")
        log("=" * 60 + "\n")

        return report

    # ---------------------------------------------------------------- #
    #  Per-pipeline analysis                                             #
    # ---------------------------------------------------------------- #
    def _analyse_pipeline(
        self,
        pipeline_name: str,
        runs: list,
        issues: list[str],
        log: callable = print,
    ) -> dict:
        log(f"  Pipeline: {pipeline_name}")

        pipeline_data = {
            "name":      pipeline_name,
            "runs":      [],
            "anomalies": [],
            "resources": {},
        }

        durations_seconds: list[float] = []
        total_data_moved = 0
        compute_units = []
        activity_types = {}

        for run in runs:
            status     = run.status or "Unknown"
            duration_s = self._duration_seconds(run)
            run_record = {
                "run_id":           run.run_id,
                "status":           status,
                "started_at":       run.run_start.isoformat() if run.run_start else None,
                "ended_at":         run.run_end.isoformat()   if run.run_end   else None,
                "duration_s":       round(duration_s, 1) if duration_s is not None else None,
                "duration_display": self._format_duration(duration_s),
                "activities":       [],
                "errors":           [],
                "flags":            [],
                "resources":        {},
            }

            log(f"    run_id  : {run.run_id}")
            log(f"    status  : {status}")
            log(f"    started : {run.run_start}")
            log(f"    duration: {self._format_duration(duration_s)}")

            # ---- Fetch activity details for resources & data flow ----
            activity_details = self._fetch_activity_runs(run.run_id)
            run_record["activities"] = activity_details

            # Extract resource usage and data flow info
            resources = self._extract_resource_metrics(activity_details)
            run_record["resources"] = resources

            # Aggregate for pipeline stats
            total_data_moved += resources.get("data_moved_bytes", 0)
            if resources.get("compute_units"):
                compute_units.extend(resources["compute_units"])
            for act_type in resources.get("activity_types", []):
                activity_types[act_type] = activity_types.get(act_type, 0) + 1

            # ---- Failure / cancellation ----
            if status in ("Failed", "Cancelled"):
                msg = (
                    f"[{pipeline_name}] run {run.run_id} "
                    f"ended with status '{status}'"
                )
                log(f"    ❌ {msg}")
                issues.append(msg)
                run_record["flags"].append("failed")

                for act in activity_details:
                    if act.get("status") in ("Failed", "Cancelled"):
                        err  = act.get("error") or {}
                        if "error" in err and isinstance(err["error"], dict):
                            err = err["error"]

                        code = (err.get("errorCode")
                                or err.get("code")
                                or err.get("ErrorCode")
                                or "N/A")
                        msg_text = (err.get("message")
                                    or err.get("Message")
                                    or err.get("errorMessage")
                                    or "No error message — check ADF portal for details")
                        cat  = (err.get("failureType")
                                or err.get("category")
                                or err.get("FailureType")
                                or "Unknown")

                        error_entry = {
                            "activity":   act["activity_name"],
                            "type":       act.get("activity_type", "Unknown"),
                            "error_code": code,
                            "message":    msg_text,
                            "category":   cat,
                        }
                        run_record["errors"].append(error_entry)
                        log(f"    → Activity '{act['activity_name']}' [{code}]: {msg_text}")
                        issues.append(
                            f"[{pipeline_name}/{act['activity_name']}] {code}: {msg_text}"
                        )

            # ---- Long-run check ----
            if duration_s is not None:
                if duration_s / 60.0 > self.long_run_threshold_minutes:
                    msg = (
                        f"[{pipeline_name}] run {run.run_id} took "
                        f"{self._format_duration(duration_s)} "
                        f"(threshold: {self.long_run_threshold_minutes} min)"
                    )
                    log(f"    ⏱  Long run: {msg}")
                    issues.append(msg)
                    run_record["flags"].append("long_run")

                durations_seconds.append(duration_s)

            # ---- Check for in-progress (running) ----
            if status in ("InProgress", "Queued", "Running"):
                run_record["flags"].append("running")
                log(f"    ▶ Currently running")

            log()
            pipeline_data["runs"].append(run_record)

        # ---- Aggregate pipeline resources ----
        pipeline_data["resources"] = {
            "total_data_moved_bytes": total_data_moved,
            "total_data_moved_display": self._format_bytes(total_data_moved),
            "compute_units_used": list(set(compute_units)) if compute_units else [],
            "activity_type_counts": activity_types,
            "cloud_compute": self._detect_cloud_compute(activity_types),
        }

        # ---- Anomaly detection ----
        anomalies = self._detect_anomalies(
            pipeline_name, runs, durations_seconds, issues, log
        )
        pipeline_data["anomalies"] = anomalies

        anomalous_run_ids = {a["run_id"] for a in anomalies}
        for rec in pipeline_data["runs"]:
            if rec["run_id"] in anomalous_run_ids:
                rec["flags"].append("anomaly")

        if durations_seconds:
            pipeline_data["stats"] = {
                "mean_s":   round(statistics.mean(durations_seconds), 1),
                "median_s": round(statistics.median(durations_seconds), 1),
                "stdev_s":  round(statistics.stdev(durations_seconds), 1)
                            if len(durations_seconds) > 1 else 0,
                "min_s":    round(min(durations_seconds), 1),
                "max_s":    round(max(durations_seconds), 1),
            }

        return pipeline_data

    # ---------------------------------------------------------------- #
    #  Activity-level drill-down                                         #
    # ---------------------------------------------------------------- #
    def _fetch_activity_runs(self, run_id: str) -> list[dict]:
        activities = self._fetch_activities_via_sdk(run_id)
        has_errors = any(a.get("error") for a in activities)
        if not has_errors:
            rest_activities = self._fetch_activities_via_rest(run_id)
            if rest_activities:
                activities = rest_activities
        return activities

    def _fetch_activities_via_sdk(self, run_id: str) -> list[dict]:
        try:
            now   = datetime.now(timezone.utc)
            after = now - timedelta(hours=LOOKBACK_HOURS)

            filter_params = RunFilterParameters(
                last_updated_after=after,
                last_updated_before=now,
            )
            result = self.client.activity_runs.query_by_pipeline_run(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                run_id=run_id,
                filter_parameters=filter_params,
            )
            activities = []
            for act in result.value or []:
                duration_s = self._duration_seconds(act)
                entry = {
                    "activity_name":    act.activity_name or "Unknown",
                    "activity_type":    act.activity_type or "Unknown",
                    "status":           act.status        or "Unknown",
                    "started_at":       act.activity_run_start.isoformat()
                                        if act.activity_run_start else None,
                    "ended_at":         act.activity_run_end.isoformat()
                                        if act.activity_run_end else None,
                    "duration_s":       round(duration_s, 1) if duration_s else None,
                    "duration_display": self._format_duration(duration_s),
                    "input":            self._safe_serialize(getattr(act, "input",  None)),
                    "output":           self._safe_serialize(getattr(act, "output", None)),
                    "error":            {},
                }

                raw_error = getattr(act, "error", None)
                if raw_error:
                    if isinstance(raw_error, dict):
                        entry["error"] = raw_error
                    elif hasattr(raw_error, "additional_properties"):
                        props = raw_error.additional_properties or {}
                        entry["error"] = props.get("error", props) or props
                    else:
                        entry["error"] = {"message": str(raw_error)}

                activities.append(entry)
            return activities

        except Exception as exc:
            if not self.silent:
                print(f"    [MonitoringAgent] SDK activity fetch failed for {run_id}: {exc}")
            return []

    def _fetch_activities_via_rest(self, run_id: str) -> list[dict]:
        """Direct REST fallback — always returns the full error payload."""
        try:
            import urllib.request
            import json as _json
            from azure.identity import ClientSecretCredential
            from config import (AZURE_TENANT_ID, AZURE_CLIENT_ID,
                                AZURE_CLIENT_SECRET, AZURE_SUBSCRIPTION_ID,
                                AZURE_RESOURCE_GROUP, AZURE_DATA_FACTORY)

            credential = ClientSecretCredential(
                AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
            )
            token = credential.get_token("https://management.azure.com/.default").token

            url = (
                f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
                f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
                f"/providers/Microsoft.DataFactory/factories/{AZURE_DATA_FACTORY}"
                f"/pipelineruns/{run_id}/queryActivityruns?api-version=2018-06-01"
            )
            now   = datetime.now(timezone.utc)
            after = now - timedelta(hours=LOOKBACK_HOURS)
            body  = _json.dumps({
                "lastUpdatedAfter":  after.isoformat(),
                "lastUpdatedBefore": now.isoformat(),
            }).encode()

            req = urllib.request.Request(
                url, data=body, method="POST",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type":  "application/json",
                }
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = _json.loads(resp.read())

            activities = []
            for act in data.get("value", []):
                started = act.get("activityRunStart")
                ended   = act.get("activityRunEnd")

                duration_s = None
                if started and ended:
                    from datetime import datetime as _dt
                    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
                    try:
                        s = _dt.strptime(started, fmt).replace(tzinfo=timezone.utc)
                        e = _dt.strptime(ended,   fmt).replace(tzinfo=timezone.utc)
                        duration_s = max((e - s).total_seconds(), 0.0)
                    except ValueError:
                        pass

                raw_err = act.get("error") or {}
                if "error" in raw_err:
                    raw_err = raw_err["error"]

                entry = {
                    "activity_name":    act.get("activityName",  "Unknown"),
                    "activity_type":    act.get("activityType",  "Unknown"),
                    "status":           act.get("status",         "Unknown"),
                    "started_at":       started,
                    "ended_at":         ended,
                    "duration_s":       round(duration_s, 1) if duration_s else None,
                    "duration_display": self._format_duration(duration_s),
                    "input":            act.get("input"),
                    "output":           act.get("output"),
                    "error": {
                        "errorCode":   raw_err.get("errorCode",   raw_err.get("code",    "")),
                        "message":     raw_err.get("message",     ""),
                        "failureType": raw_err.get("failureType", raw_err.get("category", "")),
                        "details":     raw_err.get("details",     []),
                    } if raw_err else {},
                }
                activities.append(entry)

            return activities

        except Exception as exc:
            if not self.silent:
                print(f"    [MonitoringAgent] REST activity fetch failed for {run_id}: {exc}")
            return []

    # ---------------------------------------------------------------- #
    #  Statistical anomaly detection                                     #
    # ---------------------------------------------------------------- #
    def _detect_anomalies(
        self,
        pipeline_name: str,
        runs: list,
        durations: list[float],
        issues: list[str],
        log: callable = print,
    ) -> list[dict]:
        anomalies = []
        if len(durations) < MIN_HISTORY_FOR_ANOMALY:
            return anomalies

        mean  = statistics.mean(durations)
        stdev = statistics.stdev(durations)

        if stdev == 0:
            return anomalies

        threshold = mean + ANOMALY_STDDEV_FACTOR * stdev

        for run, duration_s in zip(runs, durations):
            if duration_s > threshold:
                z_score = (duration_s - mean) / stdev
                msg = (
                    f"[{pipeline_name}] run {run.run_id} is anomalously slow "
                    f"({self._format_duration(duration_s)} vs "
                    f"mean {self._format_duration(mean)}, "
                    f"stddev {self._format_duration(stdev)}, "
                    f"z={z_score:.1f})"
                )
                log(f"    📊 Anomaly: {msg}")
                issues.append(msg)
                anomalies.append({
                    "run_id":      run.run_id,
                    "duration_s":  round(duration_s, 1),
                    "mean_s":      round(mean, 1),
                    "stdev_s":     round(stdev, 1),
                    "z_score":     round(z_score, 2),
                    "threshold_s": round(threshold, 1),
                })

        return anomalies

    # ---------------------------------------------------------------- #
    #  ADF API helpers                                                   #
    # ---------------------------------------------------------------- #
    def _fetch_running_runs(self) -> list:
        """Fetch only running/in-progress pipelines for live monitoring."""
        now   = datetime.now(timezone.utc)
        after = now - timedelta(hours=LOOKBACK_HOURS)

        # Filter for running pipelines
        filter_params = RunFilterParameters(
            last_updated_after=after,
            last_updated_before=now,
        )
        try:
            result = self.client.pipeline_runs.query_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                filter_parameters=filter_params,
            )
            # Only return running/in-progress pipelines
            return [r for r in (result.value or []) 
                    if r.status in ("InProgress", "Queued", "Running")]
        except Exception as exc:
            if not self.silent:
                print(f"  [MonitoringAgent] Failed to fetch running pipelines: {exc}")
            return []

    def _fetch_past_runs(self) -> list:
        """Fetch completed past pipelines for historical analysis."""
        now   = datetime.now(timezone.utc)
        after = now - timedelta(hours=LOOKBACK_HOURS)

        filter_params = RunFilterParameters(
            last_updated_after=after,
            last_updated_before=now,
        )
        try:
            result = self.client.pipeline_runs.query_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                filter_parameters=filter_params,
            )
            # Return only completed pipelines (not running)
            return [r for r in (result.value or []) 
                    if r.status not in ("InProgress", "Queued", "Running")]
        except Exception as exc:
            if not self.silent:
                print(f"  [MonitoringAgent] Failed to fetch past pipeline runs: {exc}")
            return []

    def _fetch_recent_runs(self) -> list:
        """Legacy method - fetches all runs (running + past)."""
        now   = datetime.now(timezone.utc)
        after = now - timedelta(hours=LOOKBACK_HOURS)

        filter_params = RunFilterParameters(
            last_updated_after=after,
            last_updated_before=now,
        )
        try:
            result = self.client.pipeline_runs.query_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                filter_parameters=filter_params,
            )
            return result.value or []
        except Exception as exc:
            if not self.silent:
                print(f"  [MonitoringAgent] Failed to fetch pipeline runs: {exc}")
            return []

    # ---------------------------------------------------------------- #
    #  Resource & metrics extraction                                     #
    # ---------------------------------------------------------------- #
    def _extract_resource_metrics(self, activities: list[dict]) -> dict:
        """Extract resource usage, data flow, and compute metrics from activities."""
        data_moved = 0
        compute_units = []
        activity_types = []
        data_flows = []

        for act in activities:
            act_type = act.get("activity_type", "Unknown")
            activity_types.append(act_type)

            # Extract data moved from copy activities
            if act_type == "Copy":
                output = act.get("output", {}) or {}
                if isinstance(output, dict):
                    # Try to find bytes read/written
                    files_copied = output.get("filesCopied", 0)
                    bytes_read = output.get("bytesRead", 0)
                    bytes_written = output.get("bytesWritten", 0)
                    data_moved += max(bytes_read, bytes_written)

            # Extract Data Flow info
            elif act_type == "DataFlow":
                output = act.get("output", {}) or {}
                if isinstance(output, dict):
                    flow_name = output.get("dataFlowName", "")
                    if flow_name:
                        data_flows.append({
                            "name": flow_name,
                            "rowsWritten": output.get("rowsWritten", 0),
                            "rowsRead": output.get("rowsRead", 0),
                        })
                    # Data Flow compute (Azure Synapse / Databricks)
                    compute = output.get("computeType", "")
                    if compute:
                        compute_units.append(compute)

            # Extract execution info for other activities
            elif act_type in ("Databricks", "Spark", "HDInsight", "AzureML"):
                output = act.get("output", {}) or {}
                if isinstance(output, dict):
                    compute = output.get("computeType", output.get("clusterId", ""))
                    if compute:
                        compute_units.append(str(compute))

        return {
            "data_moved_bytes": data_moved,
            "data_moved_display": self._format_bytes(data_moved),
            "compute_units": compute_units,
            "activity_types": activity_types,
            "data_flows": data_flows,
        }

    def _detect_cloud_compute(self, activity_types: list) -> dict:
        """Detect what cloud compute resources are being used."""
        compute_info = {
            "databricks": False,
            "azure_ml": False,
            "hdinsight": False,
            "synapse": False,
            "sql": False,
            "azure_functions": False,
        }

        for act_type in activity_types:
            act_lower = act_type.lower()
            if "databricks" in act_lower or "spark" in act_lower:
                compute_info["databricks"] = True
            if "azureml" in act_lower or "ml" in act_lower:
                compute_info["azure_ml"] = True
            if "hdinsight" in act_lower or "hdi" in act_lower:
                compute_info["hdinsight"] = True
            if "synapse" in act_lower:
                compute_info["synapse"] = True
            if "sql" in act_lower or "storedprocedure" in act_lower:
                compute_info["sql"] = True
            if "function" in act_lower:
                compute_info["azure_functions"] = True

        # Return only active compute types
        return {k: v for k, v in compute_info.items() if v}

    @staticmethod
    def _format_bytes(bytes_val: int) -> str:
        """Format bytes to human readable format."""
        if bytes_val == 0:
            return "0 B"
        units = ["B", "KB", "MB", "GB", "TB"]
        unit_idx = 0
        val = float(bytes_val)
        while val >= 1024 and unit_idx < len(units) - 1:
            val /= 1024
            unit_idx += 1
        return f"{val:.2f} {units[unit_idx]}"

    # ---------------------------------------------------------------- #
    #  Utility helpers                                                   #
    # ---------------------------------------------------------------- #
    @staticmethod
    def _duration_seconds(run) -> Optional[float]:
        start = (getattr(run, "run_start", None)
                 or getattr(run, "activity_run_start", None))
        if start is None:
            return None
        end = (getattr(run, "run_end", None)
               or getattr(run, "activity_run_end", None)
               or datetime.now(timezone.utc))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        return max((end - start).total_seconds(), 0.0)

    @staticmethod
    def _format_duration(seconds: Optional[float]) -> str:
        if seconds is None:
            return "N/A"
        seconds = int(seconds)
        hours, remainder = divmod(seconds, 3600)
        minutes, secs    = divmod(remainder, 60)
        if hours:   return f"{hours}h {minutes}m {secs}s"
        if minutes: return f"{minutes}m {secs}s"
        return f"{secs}s"

    @staticmethod
    def _safe_serialize(obj) -> Optional[dict]:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "additional_properties"):
            return obj.additional_properties
        try:
            return json.loads(json.dumps(obj, default=str))
        except Exception:
            return {"raw": str(obj)}