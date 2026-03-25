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

Usage (instantiated in main.py after ADF client is created):
    agent = MonitoringAgent(adf_client, resource_group, factory_name)
    agent.monitor()   # call after pipelines have been triggered
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
DEFAULT_LONG_RUN_MINUTES: int   = 30      # flag runs longer than this
ANOMALY_STDDEV_FACTOR:    float = 2.0     # flag runs > mean + N * stddev
LOOKBACK_HOURS:           int   = 24      # how far back to pull history
MIN_HISTORY_FOR_ANOMALY:  int   = 5       # need at least N past runs


class MonitoringAgent:
    """
    Fetches recent ADF pipeline runs and reports:
      1. Failures / cancellations + root-cause activity errors
      2. Long-running pipelines
      3. Statistical anomalies in run duration
      4. Structured JSON report written to disk for the dashboard
    """

    def __init__(
        self,
        client: DataFactoryManagementClient,
        resource_group: str,
        factory_name: str,
        long_run_threshold_minutes: int = DEFAULT_LONG_RUN_MINUTES,
        report_path: str = "adf_monitoring_report.json",
    ) -> None:
        self.client = client
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.long_run_threshold_minutes = long_run_threshold_minutes
        self.report_path = report_path

    # ---------------------------------------------------------------- #
    #  Public entry point                                                #
    # ---------------------------------------------------------------- #
    def monitor(self) -> dict:
        """
        Fetch pipeline runs, print a structured health report, write JSON,
        and return the full report dict (so callers can act on it).
        """
        print("\n" + "=" * 60)
        print("  MONITORING AGENT — ADF Pipeline Health Report")
        print("=" * 60)

        runs = self._fetch_recent_runs()

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "factory": self.factory_name,
            "resource_group": self.resource_group,
            "lookback_hours": LOOKBACK_HOURS,
            "total_runs": len(runs),
            "pipelines": {},
            "issues": [],
            "summary": {},
        }

        if not runs:
            msg = f"No pipeline runs found in the last {LOOKBACK_HOURS} hours."
            print(f"  {msg}\n")
            report["summary"] = {"status": "no_data", "message": msg}
            self._write_report(report)
            return report

        print(f"  Analysing {len(runs)} pipeline run(s) "
              f"from the last {LOOKBACK_HOURS} hours.\n")

        # Group by pipeline name
        by_pipeline: dict[str, list] = {}
        for run in runs:
            by_pipeline.setdefault(run.pipeline_name, []).append(run)

        all_issues: list[str] = []

        for pipeline_name, pipeline_runs in by_pipeline.items():
            pipeline_data = self._analyse_pipeline(
                pipeline_name, pipeline_runs, all_issues
            )
            report["pipelines"][pipeline_name] = pipeline_data

        report["issues"] = all_issues

        # Compute summary stats
        total    = len(runs)
        failed   = sum(1 for r in runs if r.status in ("Failed", "Cancelled"))
        succeeded = sum(1 for r in runs if r.status == "Succeeded")
        report["summary"] = {
            "status":         "issues_found" if all_issues else "healthy",
            "total_runs":     total,
            "succeeded":      succeeded,
            "failed":         failed,
            "in_progress":    total - failed - succeeded,
            "issue_count":    len(all_issues),
            "success_rate":   round(succeeded / total * 100, 1) if total else 0,
        }

        # Console summary
        print("-" * 60)
        if all_issues:
            print(f"  ⚠  {len(all_issues)} issue(s) detected:\n")
            for i, issue in enumerate(all_issues, 1):
                print(f"    {i}. {issue}")
        else:
            print("  ✓  All pipelines look healthy.")
        print("=" * 60 + "\n")

        self._write_report(report)
        return report

    # ---------------------------------------------------------------- #
    #  Per-pipeline analysis                                             #
    # ---------------------------------------------------------------- #
    def _analyse_pipeline(
        self,
        pipeline_name: str,
        runs: list,
        issues: list[str],
    ) -> dict:
        print(f"  Pipeline: {pipeline_name}")

        pipeline_data = {
            "name":    pipeline_name,
            "runs":    [],
            "anomalies": [],
        }

        durations_seconds: list[float] = []

        for run in runs:
            status      = run.status or "Unknown"
            duration_s  = self._duration_seconds(run)
            run_record  = {
                "run_id":     run.run_id,
                "status":     status,
                "started_at": run.run_start.isoformat() if run.run_start else None,
                "ended_at":   run.run_end.isoformat()   if run.run_end   else None,
                "duration_s": round(duration_s, 1) if duration_s is not None else None,
                "duration_display": self._format_duration(duration_s),
                "activities": [],
                "errors":     [],
                "flags":      [],
            }

            print(f"    run_id  : {run.run_id}")
            print(f"    status  : {status}")
            print(f"    started : {run.run_start}")
            print(f"    duration: {self._format_duration(duration_s)}")

            # ---- Failure / cancellation → drill into activities ----
            if status in ("Failed", "Cancelled"):
                msg = (
                    f"[{pipeline_name}] run {run.run_id} "
                    f"ended with status '{status}'"
                )
                print(f"    ❌ {msg}")
                issues.append(msg)
                run_record["flags"].append("failed")

                activity_details = self._fetch_activity_runs(run.run_id)
                run_record["activities"] = activity_details

                for act in activity_details:
                    if act.get("status") in ("Failed", "Cancelled"):
                        err = act.get("error") or {}

                        # Unwrap nested {"error": {...}} if present
                        if "error" in err and isinstance(err["error"], dict):
                            err = err["error"]

                        # Try every key variant ADF uses across SDK/REST versions
                        code = (err.get("errorCode")
                                or err.get("code")
                                or err.get("ErrorCode")
                                or "N/A")
                        msg  = (err.get("message")
                                or err.get("Message")
                                or err.get("errorMessage")
                                or "No error message returned — check ADF portal for details")
                        cat  = (err.get("failureType")
                                or err.get("category")
                                or err.get("FailureType")
                                or "Unknown")

                        error_entry = {
                            "activity":   act["activity_name"],
                            "type":       act.get("activity_type", "Unknown"),
                            "error_code": code,
                            "message":    msg,
                            "category":   cat,
                        }
                        run_record["errors"].append(error_entry)

                        print(f"    → Activity '{act['activity_name']}' [{code}]: {msg}")
                        issues.append(
                            f"[{pipeline_name}/{act['activity_name']}] {code}: {msg}"
                        )

            # ---- Long-run check ----
            if duration_s is not None:
                duration_minutes = duration_s / 60.0
                if duration_minutes > self.long_run_threshold_minutes:
                    msg = (
                        f"[{pipeline_name}] run {run.run_id} took "
                        f"{self._format_duration(duration_s)} "
                        f"(threshold: {self.long_run_threshold_minutes} min)"
                    )
                    print(f"    ⏱  Long run: {msg}")
                    issues.append(msg)
                    run_record["flags"].append("long_run")

                durations_seconds.append(duration_s)

            print()
            pipeline_data["runs"].append(run_record)

        # ---- Anomaly detection ----
        anomalies = self._detect_anomalies(
            pipeline_name, runs, durations_seconds, issues
        )
        pipeline_data["anomalies"] = anomalies

        # Attach anomaly flags back to individual run records
        anomalous_run_ids = {a["run_id"] for a in anomalies}
        for rec in pipeline_data["runs"]:
            if rec["run_id"] in anomalous_run_ids:
                rec["flags"].append("anomaly")

        # Pipeline-level stats
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
        """
        Query activity runs for a pipeline run.
        Primary: SDK query_by_pipeline_run.
        Fallback: direct REST POST to queryActivityruns (same endpoint
                  used by check_pipeline_status in adf_api.py) which
                  reliably returns the full error object ADF sometimes
                  omits from the SDK response.
        """
        activities = self._fetch_activities_via_sdk(run_id)

        # If SDK returned no useful errors, try REST directly
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

                # ADF SDK surfaces errors in several ways depending on version
                raw_error = getattr(act, "error", None)
                if raw_error:
                    if isinstance(raw_error, dict):
                        entry["error"] = raw_error
                    elif hasattr(raw_error, "additional_properties"):
                        props = raw_error.additional_properties or {}
                        # Flatten nested 'error' key ADF sometimes wraps in
                        entry["error"] = props.get("error", props) or props
                    else:
                        entry["error"] = {"message": str(raw_error)}

                activities.append(entry)
            return activities

        except Exception as exc:
            print(f"    [MonitoringAgent] SDK activity fetch failed for {run_id}: {exc}")
            return []

    def _fetch_activities_via_rest(self, run_id: str) -> list[dict]:
        """
        Direct REST fallback — POST .../pipelineruns/{runId}/queryActivityruns
        This endpoint always returns the full error payload.
        """
        try:
            from azure.identity import DefaultAzureCredential
            import urllib.request, json as _json

            credential = DefaultAzureCredential()
            token = credential.get_token("https://management.azure.com/.default").token

            from config import AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP, AZURE_DATA_FACTORY

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

                # Compute duration
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

                # Error — REST always has it under "error" key with nested object
                raw_err = act.get("error") or {}
                # ADF wraps it: {"errorCode":"..","message":"..","failureType":".."}
                # or nested:    {"error": {"errorCode":...}}
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
                    "error":            {
                        "errorCode":   raw_err.get("errorCode",   raw_err.get("code",    "")),
                        "message":     raw_err.get("message",     ""),
                        "failureType": raw_err.get("failureType", raw_err.get("category", "")),
                        "details":     raw_err.get("details",     []),
                    } if raw_err else {},
                }
                activities.append(entry)

            return activities

        except Exception as exc:
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
                print(f"    📊 Anomaly: {msg}")
                issues.append(msg)
                anomalies.append({
                    "run_id":     run.run_id,
                    "duration_s": round(duration_s, 1),
                    "mean_s":     round(mean, 1),
                    "stdev_s":    round(stdev, 1),
                    "z_score":    round(z_score, 2),
                    "threshold_s": round(threshold, 1),
                })

        return anomalies

    # ---------------------------------------------------------------- #
    #  ADF API helpers                                                   #
    # ---------------------------------------------------------------- #
    def _fetch_recent_runs(self) -> list:
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
            print(f"  [MonitoringAgent] Failed to fetch pipeline runs: {exc}")
            return []

    def _write_report(self, report: dict) -> None:
        try:
            with open(self.report_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, default=str)
            print(f"  📄 Monitoring report saved → {self.report_path}")
        except Exception as exc:
            print(f"  [MonitoringAgent] Could not write report: {exc}")

    # ---------------------------------------------------------------- #
    #  Utility helpers                                                   #
    # ---------------------------------------------------------------- #
    @staticmethod
    def _duration_seconds(run) -> Optional[float]:
        if run.run_start is None:
            return None
        end   = getattr(run, "run_end",          None) or \
                getattr(run, "activity_run_end",  None) or \
                datetime.now(timezone.utc)
        start = getattr(run, "run_start",         None) or \
                getattr(run, "activity_run_start", None)
        if start is None:
            return None
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
        if hours:
            return f"{hours}h {minutes}m {secs}s"
        if minutes:
            return f"{minutes}m {secs}s"
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