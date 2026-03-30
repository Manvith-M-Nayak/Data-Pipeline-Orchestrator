"""
monitor_agent.py
--------------------
Monitors Azure Data Factory pipeline runs using the ADF Management SDK.

Detects:
  - Long-running pipelines (duration > threshold)
  - Failed / cancelled pipelines WITH root-cause activity errors
  - Anomalous run durations (> N std-deviations from recent history)
  - Per-activity drill-down (status, duration, error code + message)
  - Writes a structured JSON report for the dashboard to consume

DYNAMIC LAYER:
  - GroqAdvisor sends the finished report to Groq LLaMA 3.3 70B
  - Groq returns: adjusted thresholds, root-cause summaries, remediation steps
  - If Groq flags suspicious running pipelines → agent re-scans automatically
  - All decisions are stored in report["groq_advice"] for dashboard consumption

Usage:
    agent = MonitoringAgent(adf_client, resource_group, factory_name)
    report = agent.monitor()           # report["groq_advice"] has Groq's analysis
"""

from __future__ import annotations

import json
import statistics
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters


# ------------------------------------------------------------------ #
#  Initial defaults — Groq may override these per-run                  #
# ------------------------------------------------------------------ #
DEFAULT_LONG_RUN_MINUTES: int   = 30
ANOMALY_STDDEV_FACTOR:    float = 2.0
LOOKBACK_HOURS:           int   = 24
MIN_HISTORY_FOR_ANOMALY:  int   = 5

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


# ================================================================== #
#  GROQ ADVISOR — Dynamic intelligence layer                           #
# ================================================================== #
class GroqAdvisor:
    """
    Sends the finished monitoring report to Groq LLaMA 3.3 70B.
    Groq returns:
      - adjusted thresholds (long_run_minutes, anomaly_stddev_factor, lookback_hours)
      - root_cause summaries for each failed pipeline
      - remediation steps (actionable, ADF-specific)
      - rescan_needed flag + reason
      - overall health verdict
    """

    SYSTEM_PROMPT = """
You are an Azure Data Factory (ADF) SRE (Site Reliability Engineer) AI.

You receive a JSON monitoring report from an ADF MonitoringAgent.
Your job is to analyse it and return a JSON object with your dynamic decisions.

Rules:
1. Examine failed pipelines and their activity-level errors.
   For each, write a concise root_cause (1-2 sentences) and 2-3 actionable remediation steps.
2. Look at duration stats (mean_s, stdev_s) and decide whether the current thresholds
   (long_run_minutes=30, anomaly_stddev=2.0) are appropriate for THIS factory's workloads.
   Suggest tighter or looser values if warranted. Explain why.
3. If any pipelines are still InProgress/Running and their duration already exceeds
   the mean + 1.5 * stdev of historical runs, set rescan_needed=true.
4. Give an overall health verdict: "healthy" | "warning" | "critical".
5. Keep root causes and remediation steps precise and ADF-specific.
   Reference real ADF concepts: DIU, partition_count, linked service, integration runtime, etc.

Return ONLY a valid JSON object — no markdown, no backticks, no explanation outside JSON.

Schema:
{
  "verdict": "healthy | warning | critical",
  "verdict_reason": "one sentence",
  "adjusted_thresholds": {
    "long_run_minutes": <int>,
    "anomaly_stddev_factor": <float>,
    "lookback_hours": <int>,
    "reasoning": "why these values fit this factory"
  },
  "pipeline_insights": [
    {
      "pipeline_name": "...",
      "root_cause": "...",
      "remediation": ["step 1", "step 2", "step 3"],
      "severity": "low | medium | high | critical"
    }
  ],
  "rescan_needed": true | false,
  "rescan_reason": "...",
  "rescan_window_minutes": <int>,
  "summary": "2-3 sentence overall summary for a human operator"
}
"""

    def __init__(self, api_key: str, silent: bool = False) -> None:
        self.api_key = api_key
        self.silent  = silent

    def analyse(self, report: dict) -> dict:
        """Send report to Groq and return its dynamic advice dict."""
        # Trim the report to avoid token bloat — keep structure but cap activity lists
        trimmed = self._trim_report(report)

        user_msg = (
            "Here is the ADF monitoring report. "
            "Analyse it and return the JSON advice object:\n\n"
            + json.dumps(trimmed, indent=2, default=str)
        )

        payload = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {"role": "user",   "content": user_msg},
            ],
            "temperature": 0.15,
            "max_completion_tokens": 1500,
            "top_p": 0.8,
        }

        if not self.silent:
            print("\n🤖 GroqAdvisor: analysing monitoring report with LLaMA 3.3 70B...")

        try:
            resp = requests.post(
                GROQ_URL,
                headers={
                    "Content-Type":  "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                },
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            if not self.silent:
                print(f"  ⚠ GroqAdvisor: API call failed — {exc}. Skipping dynamic advice.")
            return {"error": str(exc)}

        raw = resp.json()["choices"][0]["message"]["content"].strip()

        # Strip accidental markdown fences
        if "```" in raw:
            for part in raw.split("```"):
                part = part.strip().lstrip("json").strip()
                if part.startswith("{"):
                    raw = part
                    break

        try:
            advice = json.loads(raw)
            if not self.silent:
                verdict = advice.get("verdict", "?")
                emoji   = {"healthy": "✅", "warning": "⚠️", "critical": "🔴"}.get(verdict, "❓")
                print(f"  {emoji} Verdict : {verdict.upper()}")
                print(f"  📋 Summary: {advice.get('summary', 'N/A')}")
                if advice.get("rescan_needed"):
                    print(f"  🔄 Rescan : {advice.get('rescan_reason', '')}")
            return advice
        except json.JSONDecodeError as exc:
            if not self.silent:
                print(f"  ⚠ GroqAdvisor: invalid JSON from Groq — {exc}")
            return {"error": "invalid_json", "raw": raw}

    # ---- helpers ----
    @staticmethod
    def _trim_report(report: dict) -> dict:
        """Return a leaner version of the report to stay within token limits."""
        trimmed = {
            "factory":        report.get("factory"),
            "generated_at":   report.get("generated_at"),
            "lookback_hours": report.get("lookback_hours"),
            "summary":        report.get("summary"),
            "issues":         report.get("issues", [])[:20],   # cap at 20 issues
            "pipelines":      {},
        }
        for name, pdata in report.get("pipelines", {}).items():
            trimmed["pipelines"][name] = {
                "stats":     pdata.get("stats"),
                "anomalies": pdata.get("anomalies"),
                "resources": pdata.get("resources"),
                # Only include runs with flags or errors to save tokens
                "runs": [
                    {
                        "run_id":           r["run_id"],
                        "status":           r["status"],
                        "duration_display": r["duration_display"],
                        "flags":            r.get("flags", []),
                        "errors":           r.get("errors", []),
                    }
                    for r in pdata.get("runs", [])
                    if r.get("flags") or r.get("errors")
                ][:10],  # cap at 10 flagged runs per pipeline
            }
        return trimmed


# ================================================================== #
#  MONITORING AGENT                                                    #
# ================================================================== #
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
      8. [NEW] GroqAdvisor dynamic analysis — adaptive thresholds + remediation
    """

    def __init__(
        self,
        client: DataFactoryManagementClient,
        resource_group: str,
        factory_name: str,
        long_run_threshold_minutes: int   = DEFAULT_LONG_RUN_MINUTES,
        anomaly_stddev_factor: float      = ANOMALY_STDDEV_FACTOR,
        lookback_hours: int               = LOOKBACK_HOURS,
        report_path: str                  = "adf_monitoring_report.json",
        scan_past_pipelines: bool         = True,
        silent: bool                      = False,
        groq_api_key: Optional[str]       = None,   # NEW: wire in Groq for dynamic advice
    ) -> None:
        self.client                     = client
        self.resource_group             = resource_group
        self.factory_name               = factory_name
        self.long_run_threshold_minutes = long_run_threshold_minutes
        self.anomaly_stddev_factor      = anomaly_stddev_factor
        self.lookback_hours             = lookback_hours
        self.report_path                = report_path
        self.scan_past_pipelines        = scan_past_pipelines
        self.silent                     = silent
        self.groq_api_key               = groq_api_key

        # GroqAdvisor is optional — agent still works without it
        self._advisor: Optional[GroqAdvisor] = (
            GroqAdvisor(groq_api_key, silent=silent)
            if groq_api_key else None
        )

    # ---------------------------------------------------------------- #
    #  Public entry point                                                #
    # ---------------------------------------------------------------- #
    def monitor(self, silent: bool | None = None, limit: int = None) -> dict:
        """
        Fetch pipeline runs → analyse → optionally query GroqAdvisor
        → optionally re-scan if Groq says so → return full report dict.

        report["groq_advice"] is populated when groq_api_key is set.
        """
        if silent is None:
            silent = self.silent

        def log(*args, **kwargs):
            if not silent:
                print(*args, **kwargs)

        log("\n" + "=" * 60)
        log("  MONITORING AGENT — ADF Pipeline Health Report")
        log("=" * 60)

        report = self._run_scan(limit=limit, log=log)

        # ---- Dynamic Groq analysis ----
        if self._advisor:
            advice = self._advisor.analyse(report)
            report["groq_advice"] = advice

            # Apply Groq's suggested thresholds for a potential re-scan
            adj = advice.get("adjusted_thresholds", {})
            if adj.get("long_run_minutes"):
                self.long_run_threshold_minutes = int(adj["long_run_minutes"])
            if adj.get("anomaly_stddev_factor"):
                self.anomaly_stddev_factor = float(adj["anomaly_stddev_factor"])
            if adj.get("lookback_hours"):
                self.lookback_hours = int(adj["lookback_hours"])

            # Re-scan if Groq recommends it (e.g. suspicious running pipelines)
            if advice.get("rescan_needed"):
                rescan_window = advice.get("rescan_window_minutes", 5)
                log(f"\n🔄 GroqAdvisor requested re-scan in {rescan_window}m: "
                    f"{advice.get('rescan_reason', '')}")
                time.sleep(rescan_window * 60)
                log("\n🔄 Re-scanning with Groq-adjusted thresholds...")
                report = self._run_scan(limit=limit, log=log)
                report["groq_advice"] = advice   # preserve original advice
                report["rescanned"]   = True

            # Print per-pipeline insights
            if not silent:
                insights = advice.get("pipeline_insights", [])
                if insights:
                    log("\n📊 Pipeline Insights from GroqAdvisor:")
                    for ins in insights:
                        sev_emoji = {
                            "low": "🟢", "medium": "🟡",
                            "high": "🟠", "critical": "🔴"
                        }.get(ins.get("severity", ""), "⚪")
                        log(f"  {sev_emoji} {ins['pipeline_name']}: {ins.get('root_cause', '')}")
                        for step in ins.get("remediation", []):
                            log(f"      → {step}")
        else:
            report["groq_advice"] = None

        # Write JSON report to disk
        try:
            with open(self.report_path, "w", encoding="utf-8") as fh:
                json.dump(report, fh, indent=2, default=str)
        except OSError as exc:
            log(f"  ⚠ Could not write report to {self.report_path}: {exc}")

        return report

    # ---------------------------------------------------------------- #
    #  Internal scan — separated so it can be called twice (re-scan)    #
    # ---------------------------------------------------------------- #
    def _run_scan(self, limit: int = None, log=print) -> dict:
        running_runs = self._fetch_running_runs()
        past_runs    = self._fetch_past_runs() if self.scan_past_pipelines else []
        all_runs     = sorted(
            running_runs + past_runs,
            key=lambda r: r.run_start or datetime.min,
            reverse=True,
        )
        if limit:
            all_runs = all_runs[:limit]

        report = {
            "generated_at":        datetime.now(timezone.utc).isoformat(),
            "factory":             self.factory_name,
            "resource_group":      self.resource_group,
            "lookback_hours":      self.lookback_hours,
            "long_run_threshold_minutes": self.long_run_threshold_minutes,
            "anomaly_stddev_factor":      self.anomaly_stddev_factor,
            "scan_past_pipelines": self.scan_past_pipelines,
            "total_runs":          len(all_runs),
            "running_runs":        len(running_runs),
            "past_runs":           len(past_runs),
            "pipelines":           {},
            "issues":              [],
            "summary":             {},
        }

        if not all_runs:
            msg = f"No pipeline runs found in the last {self.lookback_hours} hours."
            log(f"  {msg}\n")
            report["summary"] = {"status": "no_data", "message": msg}
            return report

        log(f"  Running pipelines     : {len(running_runs)}")
        if self.scan_past_pipelines:
            log(f"  Past pipelines scanned: {len(past_runs)}")
        else:
            log(f"  Past pipeline scanning : disabled")
        log(f"  Total runs            : {len(all_runs)}\n")

        by_pipeline: dict[str, list] = {}
        for run in all_runs:
            by_pipeline.setdefault(run.pipeline_name, []).append(run)

        all_issues: list[str] = []
        for pipeline_name, pipeline_runs in by_pipeline.items():
            report["pipelines"][pipeline_name] = self._analyse_pipeline(
                pipeline_name, pipeline_runs, all_issues, log
            )

        report["issues"] = all_issues

        total     = len(all_runs)
        failed    = sum(1 for r in all_runs if r.status in ("Failed", "Cancelled"))
        succeeded = sum(1 for r in all_runs if r.status == "Succeeded")
        running   = sum(1 for r in all_runs if r.status in ("InProgress", "Queued", "Running"))

        report["summary"] = {
            "status":         "issues_found" if all_issues else "healthy",
            "total_runs":     total,
            "running_runs":   running,
            "completed_runs": total - running,
            "succeeded":      succeeded,
            "failed":         failed,
            "in_progress":    running,
            "issue_count":    len(all_issues),
            "success_rate":   round(succeeded / (total - running) * 100, 1)
                              if (total - running) else 0,
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
        log=print,
    ) -> dict:
        log(f"  Pipeline: {pipeline_name}")

        pipeline_data: dict = {
            "name":      pipeline_name,
            "runs":      [],
            "anomalies": [],
            "resources": {},
        }

        durations_seconds: list[float] = []
        total_data_moved  = 0
        compute_units:    list[str]   = []
        activity_types:   dict        = {}

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

            activity_details = self._fetch_activity_runs(run.run_id)
            run_record["activities"] = activity_details

            resources = self._extract_resource_metrics(activity_details)
            run_record["resources"] = resources

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
                        err = act.get("error") or {}
                        if "error" in err and isinstance(err["error"], dict):
                            err = err["error"]
                        code     = (err.get("errorCode") or err.get("code")
                                    or err.get("ErrorCode") or "N/A")
                        msg_text = (err.get("message") or err.get("Message")
                                    or err.get("errorMessage")
                                    or "No error message — check ADF portal for details")
                        cat      = (err.get("failureType") or err.get("category")
                                    or err.get("FailureType") or "Unknown")
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

            # ---- Long-run check (uses current threshold — may be Groq-adjusted) ----
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

            # ---- In-progress ----
            if status in ("InProgress", "Queued", "Running"):
                run_record["flags"].append("running")
                log(f"    ▶ Currently running")

            log()
            pipeline_data["runs"].append(run_record)

        # ---- Aggregate pipeline resources ----
        pipeline_data["resources"] = {
            "total_data_moved_bytes":   total_data_moved,
            "total_data_moved_display": self._format_bytes(total_data_moved),
            "compute_units_used":       list(set(compute_units)) if compute_units else [],
            "activity_type_counts":     activity_types,
            "cloud_compute":            self._detect_cloud_compute(activity_types),
        }

        # ---- Anomaly detection (uses current stddev factor) ----
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
            after = now - timedelta(hours=self.lookback_hours)
            result = self.client.activity_runs.query_by_pipeline_run(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                run_id=run_id,
                filter_parameters=RunFilterParameters(
                    last_updated_after=after,
                    last_updated_before=now,
                ),
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
            after = now - timedelta(hours=self.lookback_hours)
            body  = _json.dumps({
                "lastUpdatedAfter":  after.isoformat(),
                "lastUpdatedBefore": now.isoformat(),
            }).encode()

            req = urllib.request.Request(
                url, data=body, method="POST",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type":  "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = _json.loads(resp.read())

            activities = []
            for act in data.get("value", []):
                started    = act.get("activityRunStart")
                ended      = act.get("activityRunEnd")
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

                activities.append({
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
                })
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
        log=print,
    ) -> list[dict]:
        anomalies = []
        if len(durations) < MIN_HISTORY_FOR_ANOMALY:
            return anomalies

        mean  = statistics.mean(durations)
        stdev = statistics.stdev(durations)
        if stdev == 0:
            return anomalies

        # Uses self.anomaly_stddev_factor — may be updated by Groq
        threshold = mean + self.anomaly_stddev_factor * stdev

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
        now   = datetime.now(timezone.utc)
        after = now - timedelta(hours=self.lookback_hours)
        try:
            result = self.client.pipeline_runs.query_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                filter_parameters=RunFilterParameters(
                    last_updated_after=after, last_updated_before=now
                ),
            )
            return [r for r in (result.value or [])
                    if r.status in ("InProgress", "Queued", "Running")]
        except Exception as exc:
            if not self.silent:
                print(f"  [MonitoringAgent] Failed to fetch running pipelines: {exc}")
            return []

    def _fetch_past_runs(self) -> list:
        now   = datetime.now(timezone.utc)
        after = now - timedelta(hours=self.lookback_hours)
        try:
            result = self.client.pipeline_runs.query_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                filter_parameters=RunFilterParameters(
                    last_updated_after=after, last_updated_before=now
                ),
            )
            return [r for r in (result.value or [])
                    if r.status not in ("InProgress", "Queued", "Running")]
        except Exception as exc:
            if not self.silent:
                print(f"  [MonitoringAgent] Failed to fetch past pipeline runs: {exc}")
            return []

    # ---------------------------------------------------------------- #
    #  Resource & metrics extraction                                     #
    # ---------------------------------------------------------------- #
    def _extract_resource_metrics(self, activities: list[dict]) -> dict:
        data_moved    = 0
        compute_units: list[str]  = []
        activity_types: list[str] = []
        data_flows:    list[dict] = []

        for act in activities:
            act_type = act.get("activity_type", "Unknown")
            activity_types.append(act_type)
            if act_type == "Copy":
                output = act.get("output", {}) or {}
                if isinstance(output, dict):
                    data_moved += max(
                        output.get("bytesRead", 0),
                        output.get("bytesWritten", 0),
                    )
            elif act_type == "DataFlow":
                output = act.get("output", {}) or {}
                if isinstance(output, dict):
                    flow_name = output.get("dataFlowName", "")
                    if flow_name:
                        data_flows.append({
                            "name":        flow_name,
                            "rowsWritten": output.get("rowsWritten", 0),
                            "rowsRead":    output.get("rowsRead",    0),
                        })
                    compute = output.get("computeType", "")
                    if compute:
                        compute_units.append(compute)
            elif act_type in ("Databricks", "Spark", "HDInsight", "AzureML"):
                output = act.get("output", {}) or {}
                if isinstance(output, dict):
                    compute = output.get("computeType", output.get("clusterId", ""))
                    if compute:
                        compute_units.append(str(compute))

        return {
            "data_moved_bytes":   data_moved,
            "data_moved_display": self._format_bytes(data_moved),
            "compute_units":      compute_units,
            "activity_types":     activity_types,
            "data_flows":         data_flows,
        }

    def _detect_cloud_compute(self, activity_types: dict) -> dict:
        compute_info = {
            "databricks": False, "azure_ml": False, "hdinsight": False,
            "synapse": False, "sql": False, "azure_functions": False,
        }
        for act_type in activity_types:
            t = act_type.lower()
            if "databricks" in t or "spark" in t: compute_info["databricks"]      = True
            if "azureml"    in t or "ml"    in t: compute_info["azure_ml"]        = True
            if "hdinsight"  in t or "hdi"   in t: compute_info["hdinsight"]       = True
            if "synapse"    in t:                  compute_info["synapse"]         = True
            if "sql"        in t or "storedprocedure" in t: compute_info["sql"]   = True
            if "function"   in t:                  compute_info["azure_functions"] = True
        return {k: v for k, v in compute_info.items() if v}

    # ---------------------------------------------------------------- #
    #  Utility helpers                                                   #
    # ---------------------------------------------------------------- #
    @staticmethod
    def _format_bytes(bytes_val: int) -> str:
        if bytes_val == 0:
            return "0 B"
        units = ["B", "KB", "MB", "GB", "TB"]
        idx, val = 0, float(bytes_val)
        while val >= 1024 and idx < len(units) - 1:
            val /= 1024
            idx += 1
        return f"{val:.2f} {units[idx]}"

    @staticmethod
    def _duration_seconds(run) -> Optional[float]:
        start = getattr(run, "run_start", None) or getattr(run, "activity_run_start", None)
        if start is None:
            return None
        end = (getattr(run, "run_end", None)
               or getattr(run, "activity_run_end", None)
               or datetime.now(timezone.utc))
        if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo   is None: end   = end.replace(tzinfo=timezone.utc)
        return max((end - start).total_seconds(), 0.0)

    @staticmethod
    def _format_duration(seconds: Optional[float]) -> str:
        if seconds is None:
            return "N/A"
        seconds = int(seconds)
        h, rem = divmod(seconds, 3600)
        m, s   = divmod(rem, 60)
        if h: return f"{h}h {m}m {s}s"
        if m: return f"{m}m {s}s"
        return f"{s}s"

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