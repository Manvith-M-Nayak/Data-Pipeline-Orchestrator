import streamlit as st
import sys
import os
import tempfile
import time
import threading
import queue
import io
import json
from datetime import datetime, timezone

# ── Ensure databricks/ dir is on sys.path so local imports work
# regardless of which directory `streamlit run` is invoked from.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from config import DATABRICKS_HOST

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Databricks Pipeline Orchestrator",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');

html, body,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewBlockContainer"],
.main {
    background: #f8fafc !important;
    color: #1e293b;
    font-family: 'IBM Plex Sans', sans-serif;
}
[data-testid="stHeader"], [data-testid="stToolbar"],
#MainMenu, footer { display: none !important; }

div, span, p, label, li, td, th {
    color: #1e293b !important;
}
div, span, p, label, li, td, th, input, textarea, select {
    opacity: 1 !important;
    animation: none !important;
}

[data-testid="stSelectbox"] > div > div {
    background-color: #1e293b !important; color: #ffffff !important;
    border-radius: 8px !important; border: 1px solid #475569 !important;
}
[data-testid="stSelectbox"] input,
[data-testid="stSelectbox"] div,
[data-testid="stSelectbox"] span,
[data-testid="stSelectbox"] p { color: #ffffff !important; }
div[data-baseweb="popover"] *, ul[data-baseweb="menu"] * { color: #ffffff !important; }
[data-testid="stSelectbox"] div[data-baseweb="select"],
[data-testid="stSelectbox"] ul, [data-testid="stSelectbox"] li,
ul[data-baseweb="menu"] li, div[data-baseweb="popover"] li,
div[data-baseweb="menu"] div {
    background-color: #1e293b !important; color: #ffffff !important;
}
[data-testid="stSelectbox"] li:hover { background-color: #334155 !important; }
[data-testid="stSelectbox"] li[aria-selected="true"] { background-color: #3b82f6 !important; }
[data-testid="stSelectbox"] label { display: none !important; }

[data-testid="stNumberInput"] input {
    color: #1e293b !important; background-color: #ffffff !important;
    border-radius: 8px !important; border: 1px solid #e2e8f0 !important;
}
[data-testid="stFileUploader"] *, .stFileUploader * { color: #1e293b !important; }
[data-testid="stTextInput"] input {
    color: #1e293b !important; background-color: #ffffff !important;
    border-radius: 8px !important; border: 1px solid #e2e8f0 !important;
}
[data-testid="stTextInput"] input:focus { border-color: #3b82f6 !important; outline: none !important; }

.stApp > * { animation: none !important; transition: none !important; }
[data-testid="stMetric"], [data-testid="stMetric"] * { opacity: 1 !important; }

.block-container { padding: 2rem 3rem 4rem !important; max-width: 1280px; }

/* Header */
.app-header {
    display: flex; align-items: center; gap: 1rem;
    padding: 1.5rem 0 1rem; border-bottom: 1px solid #e2e8f0; margin-bottom: 1.8rem;
}
.app-logo {
    width: 36px; height: 36px;
    background: linear-gradient(135deg, #e25a1c 0%, #ff6b35 100%);
    border-radius: 8px; display: flex; align-items: center; justify-content: center;
    font-size: 1.1rem; box-shadow: 0 0 18px rgba(226,90,28,0.35);
}
.app-eyebrow {
    font-family: 'IBM Plex Mono', monospace; font-size: 0.62rem;
    letter-spacing: 0.2em; text-transform: uppercase; color: #64748b; margin-bottom: 0.1rem;
}
.app-title { font-size: 1.45rem; font-weight: 700; letter-spacing: -0.02em; color: #0f172a; margin: 0; }

/* Status badge */
.badge {
    display: inline-flex; align-items: center; gap: 0.4rem;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.65rem;
    font-weight: 600; letter-spacing: 0.1em; text-transform: uppercase;
    padding: 0.22rem 0.7rem; border-radius: 999px;
}
.badge-idle    { background: #f1f5f9; color: #64748b; border: 1px solid #cbd5e1; }
.badge-running { background: #fff7ed; color: #ea580c; border: 1px solid #fdba74; animation: pulse 1.8s infinite; }
.badge-healing { background: #fef3c7; color: #d97706; border: 1px solid #fcd34d; animation: pulse 1.8s infinite; }
.badge-ok      { background: #dcfce7; color: #16a34a; border: 1px solid #86efac; }
.badge-fail    { background: #fee2e2; color: #dc2626; border: 1px solid #fca5a5; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }

/* Healing banner */
.healing-banner {
    background: #fffbeb; border: 1px solid #f59e0b; border-left: 4px solid #f59e0b;
    border-radius: 8px; padding: 0.9rem 1.2rem; margin-bottom: 1rem;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.74rem; color: #92400e;
}
.healing-banner b { color: #78350f; }

/* Cards */
.card { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 14px; padding: 1.5rem 1.7rem; margin-bottom: 1.1rem; box-shadow: 0 2px 12px rgba(0,0,0,0.05); }
.card-label { font-family: 'IBM Plex Mono', monospace; font-size: 0.62rem; font-weight: 600; letter-spacing: 0.18em; text-transform: uppercase; color: #e25a1c; margin-bottom: 1rem; }

/* Tables */
.plan-table { width: 100%; border-collapse: collapse; font-family: 'IBM Plex Mono', monospace; font-size: 0.76rem; }
.plan-table th { text-align: left; padding: 0.5rem 0.9rem; border-bottom: 1px solid #e2e8f0; font-size: 0.62rem; letter-spacing: 0.12em; text-transform: uppercase; color: #64748b; font-weight: 600; }
.plan-table td { padding: 0.5rem 0.9rem; border-bottom: 1px solid #f1f5f9; color: #475569; }
.plan-table tr:last-child td { border-bottom: none; }
.plan-table tr:hover td { background: #f8fafc; }

/* Pills */
.pill { display: inline-block; padding: 0.15rem 0.55rem; border-radius: 5px; font-size: 0.66rem; font-weight: 500; }
.pill-copy      { background: #dcfce7; color: #16a34a; }
.pill-transform { background: #fff7ed; color: #ea580c; }
.pill-integer   { background: #e0e7ff; color: #6366f1; }
.pill-double    { background: #dcfce7; color: #16a34a; }
.pill-string    { background: #fef3c7; color: #d97706; }
.pill-succeeded { background: #dcfce7; color: #16a34a; }
.pill-failed    { background: #fee2e2; color: #dc2626; }
.pill-inprogress, .pill-running { background: #fff7ed; color: #ea580c; animation: pulse 1.8s infinite; }
.pill-queued    { background: #fef3c7; color: #d97706; }
.pill-unknown   { background: #f1f5f9; color: #64748b; }

/* Log terminal */
.log-box {
    background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px;
    padding: 1.1rem 1.3rem; font-family: 'IBM Plex Mono', monospace;
    font-size: 0.74rem; line-height: 1.9; min-height: 320px; max-height: 480px;
    overflow-y: auto; white-space: pre-wrap; word-break: break-word; color: #334155;
}
.log-box .log-error   { color: #dc2626; }
.log-box .log-success { color: #16a34a; }
.log-box .log-warn    { color: #d97706; }
.log-box .log-info    { color: #ea580c; }
.log-box .log-monitor { color: #0891b2; }
.log-box .log-heal    { color: #d97706; font-weight: 600; }
.log-box .log-default { color: #64748b; }

/* Monitor grid */
.mon-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.9rem; margin-bottom: 1.2rem; }
.mon-card { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 1rem 1.2rem; }
.mon-card-label { font-family: 'IBM Plex Mono', monospace; font-size: 0.58rem; letter-spacing: 0.14em; text-transform: uppercase; color: #64748b; margin-bottom: 0.4rem; }
.mon-card-value { font-family: 'IBM Plex Mono', monospace; font-size: 1.35rem; font-weight: 700; color: #0f172a; }
.mon-card-sub { font-size: 0.65rem; color: #64748b; margin-top: 0.2rem; }
.mon-card-ok   { border-color: #86efac; }
.mon-card-warn { border-color: #fcd34d; }
.mon-card-err  { border-color: #fca5a5; }
.mon-card-blue { border-color: #93c5fd; }

/* Run row */
.run-row { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 0.8rem 1rem; margin-bottom: 0.5rem; font-family: 'IBM Plex Mono', monospace; font-size: 0.73rem; }
.run-row-header { display: flex; align-items: center; gap: 0.7rem; flex-wrap: wrap; }
.run-row-name { font-weight: 600; color: #1e293b; }
.run-row-meta { color: #64748b; font-size: 0.67rem; }
.run-row-err { margin-top: 0.6rem; background: #fee2e2; border-left: 3px solid #dc2626; padding: 0.4rem 0.7rem; border-radius: 0 5px 5px 0; color: #dc2626; font-size: 0.69rem; line-height: 1.6; }

/* Streamlit overrides */
[data-testid="stFileUploaderDropzone"] {
    background: #f8fafc !important; border: 2px dashed #cbd5e1 !important; border-radius: 10px !important;
}
[data-testid="stFileUploaderDropzone"]:hover { border-color: #e25a1c !important; background: #fff7ed !important; }
[data-testid="stFileUploaderDropzone"] button {
    background: #fff7ed !important; color: #ea580c !important; border: 1px solid #fdba74 !important;
    border-radius: 6px !important; font-weight: 600 !important; font-size: 0.82rem !important;
    padding: 0.35rem 1rem !important;
}
textarea { background: #ffffff !important; border: 1px solid #e2e8f0 !important; border-radius: 8px !important; color: #1e293b !important; font-size: 0.88rem !important; }
textarea:focus { border-color: #e25a1c !important; outline: none !important; }

[data-testid="stButton"] > button {
    background: linear-gradient(135deg, #e25a1c, #ff6b35) !important;
    color: #fff !important; border: none !important; border-radius: 8px !important;
    font-weight: 600 !important; font-size: 0.88rem !important; padding: 0.6rem 1.4rem !important;
    width: 100% !important; box-shadow: 0 2px 8px rgba(226,90,28,0.3) !important;
}
[data-testid="stButton"] > button:hover { opacity: 0.88 !important; transform: translateY(-1px) !important; }
[data-testid="stButton"] > button:disabled { background: #e2e8f0 !important; color: #94a3b8 !important; box-shadow: none !important; }

[data-testid="stDownloadButton"] > button {
    background: #dcfce7 !important; color: #16a34a !important; border: 1px solid #86efac !important;
    border-radius: 8px !important; font-weight: 600 !important; padding: 0.6rem 1.4rem !important; width: 100% !important;
}
[data-testid="stProgress"] > div > div { background: linear-gradient(90deg, #e25a1c, #ff6b35) !important; border-radius: 999px !important; }
[data-testid="stProgress"] > div { background: #e2e8f0 !important; border-radius: 999px !important; }
[data-testid="stMetric"] { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 1rem 1.2rem; }
label[data-testid="stMetricLabel"] p { color: #64748b !important; font-size: 0.7rem !important; text-transform: uppercase; }
[data-testid="stMetricValue"] { color: #0f172a !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 1.1rem !important; }
[data-testid="stTabs"] [role="tab"] { font-family: 'IBM Plex Mono', monospace !important; font-size: 0.72rem !important; text-transform: uppercase !important; color: #64748b !important; }
[data-testid="stTabs"] [role="tab"][aria-selected="true"] { color: #ea580c !important; border-bottom-color: #ea580c !important; }
[data-testid="stExpander"] { background: #ffffff !important; border: 1px solid #e2e8f0 !important; border-radius: 10px !important; }
.sec-divider { border: none; border-top: 1px solid #e2e8f0; margin: 1.2rem 0; }
</style>
""", unsafe_allow_html=True)

# ── Session state defaults ─────────────────────────────────────────────────────
DEFAULTS = {
    "stage":              "input",
    "pipeline_config":    None,
    "user_prompt":        "",
    "schema":             None,
    "csv_tmp_path":       None,
    "logs":               [],
    "monitor_logs":       [],
    "live_runs":          [],
    "clusters":           [],
    "cluster_cache":      {},
    "run_metrics_cache":  {},
    "inspected_run":      None,
    "ar_counter":         30,
    "output_csv":         None,
    "output_filename":    "output.csv",
    "run_error":          None,
    "progress":           0,
    "pipeline_start_ts":  None,
    "pipeline_end_ts":    None,
    "stage_paths":        {},
    "used_fallback":      False,
    "monitor_only":       False,
    # Self-healing state
    "heal_attempted":     False,   # True once we have tried healing this run
    "heal_cause":         None,    # The CAUSE_* string detected by the agent
    "heal_summary":       [],      # Human-readable lines shown in the banner
    # Thread control — True once a thread has been started for this pipeline run.
    "pipeline_thread_started": False,
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ── Helpers ────────────────────────────────────────────────────────────────────
def read_csv_schema(filepath: str, sample_rows: int = 5) -> dict:
    import csv
    size = os.path.getsize(filepath)
    size_hint = (
        "small (< 5MB)"   if size < 5_242_880  else
        "medium (5–50MB)" if size < 52_428_800 else
        "large (> 50MB)"
    )
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        samples, row_count = [], 0
        for row in reader:
            row_count += 1
            if len(samples) < sample_rows:
                samples.append(dict(row))

    def _is_float(v):
        try: float(v); return True
        except: return False

    inferred = {}
    for col in columns:
        vals = [str(s.get(col, "")) for s in samples if s.get(col)]
        if all(v.isdigit() for v in vals if v):
            inferred[col] = "integer"
        elif all(_is_float(v) for v in vals if v):
            inferred[col] = "double"
        else:
            inferred[col] = "string"

    return {"columns": columns, "samples": samples, "row_count": row_count,
            "size_hint": size_hint, "inferred_types": inferred}


def _fmt_duration(seconds):
    if seconds is None: return "N/A"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:   return f"{h}h {m}m {sec}s"
    if m:   return f"{m}m {sec}s"
    return f"{sec}s"


# ── Self-healing helper ────────────────────────────────────────────────────────
def _attempt_heal(error_message: str, pipeline_config: dict, schema: dict) -> tuple:
    """
    Call DatabricksSelfHealingAgent.heal() and return (fixed_config, summary_lines).
    This is intentionally thin — all real logic lives in the agent.
    """
    from databricks_self_healing_agent import DatabricksSelfHealingAgent
    agent = DatabricksSelfHealingAgent()

    # Capture printed output from the agent so we can surface it in the UI
    captured = io.StringIO()
    import contextlib
    with contextlib.redirect_stdout(captured):
        _, fixed_config = agent.heal(
            error_message,
            pipeline_config,
            csv_columns=schema.get("columns"),
        )

    # Also grab the detected cause for the banner
    cause_info = agent.detect_root_cause(error_message, pipeline_config)

    summary_lines = [
        l for l in captured.getvalue().splitlines()
        if l.strip() and not l.startswith("🛠")
    ]
    return fixed_config, cause_info.get("cause", "unknown"), summary_lines


# ── Pipeline execution thread ──────────────────────────────────────────────────
def run_pipeline_thread(csv_path: str, pipeline_config: dict, schema: dict, result_q: queue.Queue):
    """
    Runs execute_pipeline() and, on failure, automatically invokes the
    self-healing agent once then retries.  All outcomes are posted to
    result_q so the main Streamlit thread can update state cleanly.
    """
    class Tee(io.TextIOBase):
        def write(self, s):
            s = s.rstrip("\n")
            if s.strip():
                result_q.put(("log", s))
            return len(s)
        def flush(self): pass

    import contextlib
    tee = Tee()

    try:
        with contextlib.redirect_stdout(tee):
            from databricks_api import execute_pipeline

            def prog(pct):
                result_q.put(("progress", pct))

            # ── First attempt ──────────────────────────────────────────
            result = execute_pipeline(
                csv_path=csv_path,
                pipeline_config=pipeline_config,
                schema=schema,
                log_fn=lambda msg: result_q.put(("log", msg)),
                progress_fn=prog,
            )

        if result["status"] == "ok":
            result_q.put(("ok", result))
            return

        # ── First attempt failed — invoke self-healing agent ──────────
        error_msg = result.get("message", "Unknown error")

        result_q.put(("log", ""))
        result_q.put(("log", "━" * 55))
        result_q.put(("log", "❌  INITIAL RUN FAILED"))
        result_q.put(("log", f"    Error: {error_msg.splitlines()[0] if error_msg else 'Unknown'}"))
        result_q.put(("log", "━" * 55))
        result_q.put(("log", ""))
        result_q.put(("log", "🛠  SELF-HEALING AGENT ACTIVATED"))

        try:
            fixed_config, cause, summary = _attempt_heal(
                error_msg, pipeline_config, schema
            )

            # Human-readable cause label
            cause_labels = {
                "expression_error":  "PySpark Expression Error — bare column used as Python variable",
                "null_values":       "Null Value detected in column",
                "type_cast_error":   "Type Cast Failure",
                "schema_mismatch":   "Schema Mismatch",
                "cluster_oom":       "Cluster Out of Memory",
                "cluster_error":     "Cluster Error",
                "auth_error":        "Authentication Error",
                "dbfs_missing":      "DBFS Path Not Found",
                "workspace_error":   "Workspace Upload Error",
                "job_timeout":       "Job Timeout",
                "copy_failure":      "CSV Copy Stage Failure",
                "unknown":           "Unknown Error",
            }
            cause_label = cause_labels.get(cause, cause)

            result_q.put(("log", f"🔍  Root cause detected : {cause_label}"))
            result_q.put(("log", f"🔧  Fix applied        : Adjusted generated PySpark code to wrap all"))
            result_q.put(("log", f"                         column references in col(\"...\") as required by Spark"))
            result_q.put(("log", ""))
            for line in summary:
                result_q.put(("log", f"   🔧 {line}"))

            result_q.put(("heal_start", {"cause": cause, "summary": summary}))

        except Exception as heal_err:
            result_q.put(("log", f"   Self-healing agent error: {heal_err}"))
            result_q.put(("error", error_msg))
            return

        # ── Auth / workspace errors cannot be fixed by retry ──────────
        from databricks_self_healing_agent import CAUSE_AUTH_ERROR, CAUSE_WORKSPACE_ERROR
        if cause in (CAUSE_AUTH_ERROR, CAUSE_WORKSPACE_ERROR):
            result_q.put(("log", "   ⚠  Cannot auto-retry auth/workspace errors — manual fix required."))
            result_q.put(("error", f"[{cause}] {error_msg}"))
            return

        # ── Retry with fixed config ────────────────────────────────────
        result_q.put(("log", ""))
        result_q.put(("log", "━" * 55))
        result_q.put(("log", "🔁  RETRYING WITH HEALED CONFIGURATION…"))
        result_q.put(("log", "━" * 55))
        result_q.put(("log", ""))
        result_q.put(("progress", 10))

        try:
            with contextlib.redirect_stdout(tee):
                retry_result = execute_pipeline(
                    csv_path=csv_path,
                    pipeline_config=fixed_config,
                    schema=schema,
                    log_fn=lambda msg: result_q.put(("log", msg)),
                    progress_fn=prog,
                )
        except Exception as retry_err:
            result_q.put(("error", f"Retry failed: {retry_err}"))
            return

        if retry_result["status"] == "ok":
            result_q.put(("log", ""))
            result_q.put(("log", "━" * 55))
            result_q.put(("log", "✅  PIPELINE RECOVERED SUCCESSFULLY AFTER SELF-HEALING"))
            result_q.put(("log", "    The agent detected the root cause, applied a fix,"))
            result_q.put(("log", "    and the retried pipeline completed without errors."))
            result_q.put(("log", "━" * 55))
            result_q.put(("ok", retry_result))
        else:
            result_q.put(("error", retry_result.get("message", "Retry failed after healing")))

    except Exception as e:
        result_q.put(("error", str(e)))


# ── Monitor thread ─────────────────────────────────────────────────────────────
def run_monitor_thread(result_q: queue.Queue, limit: int = 20):
    try:
        from databricks_api import (
            list_recent_runs, list_all_clusters, get_cluster_info,
            get_cluster_events, parse_run_metrics,
        )
        result_q.put(("monitor_log", "Fetching Databricks job runs..."))
        runs = list_recent_runs(limit=limit)
        result_q.put(("monitor_log", f"Found {len(runs)} recent run(s)"))
        result_q.put(("live_runs", runs))

        result_q.put(("monitor_log", "Fetching cluster list..."))
        clusters = list_all_clusters()
        result_q.put(("monitor_log", f"Found {len(clusters)} cluster(s)"))
        result_q.put(("clusters", clusters))

        cluster_ids = set()
        for r in runs:
            if r.get("cluster_id"):
                cluster_ids.add(r["cluster_id"])
            for t in r.get("tasks", []):
                if t.get("cluster_id"):
                    cluster_ids.add(t["cluster_id"])

        if cluster_ids:
            result_q.put(("monitor_log", f"Fetching compute info for {len(cluster_ids)} cluster(s)..."))
            cache = {}
            for cid in cluster_ids:
                info = get_cluster_info(cid)
                if info:
                    cache[cid] = info
            result_q.put(("cluster_cache", cache))
            result_q.put(("monitor_log", f"Compute info fetched for {len(cache)} cluster(s)"))

        result_q.put(("monitor_log", "Fetching actual runtime events per run..."))
        run_metrics_cache = {}
        for r in runs:
            cid = r.get("cluster_id") or next(
                (t.get("cluster_id") for t in r.get("tasks", []) if t.get("cluster_id")), ""
            )
            if not cid:
                continue
            events = get_cluster_events(cid, r.get("start_time_ms"), r.get("end_time_ms"))
            run_metrics_cache[r["run_id"]] = parse_run_metrics(events)
        result_q.put(("run_metrics_cache", run_metrics_cache))
        result_q.put(("monitor_log", f"Runtime metrics fetched for {len(run_metrics_cache)} run(s)"))
    except Exception as e:
        result_q.put(("monitor_log", f"Monitor error: {e}"))


# ── Render helpers ─────────────────────────────────────────────────────────────
def render_header():
    st.markdown("""
    <div class="app-header">
        <div class="app-logo">🔥</div>
        <div class="app-title-block">
            <div class="app-eyebrow">Databricks</div>
            <div class="app-title">Pipeline Orchestrator</div>
        </div>
    </div>""", unsafe_allow_html=True)


def render_status_badge():
    badge_map = {
        "input":   ("Idle",    "badge-idle"),
        "plan":    ("Ready",   "badge-idle"),
        "running": ("Running", "badge-running"),
        "healing": ("Healing", "badge-healing"),
        "done":    ("Success", "badge-ok"),
        "failed":  ("Failed",  "badge-fail"),
    }
    label, cls = badge_map.get(st.session_state.stage, ("Idle", "badge-idle"))
    dot = {
        "badge-idle":    "#4f6a8a",
        "badge-running": "#ea580c",
        "badge-healing": "#d97706",
        "badge-ok":      "#34d399",
        "badge-fail":    "#f87171",
    }[cls]
    st.markdown(
        f'<span class="badge {cls}">'
        f'<span style="width:6px;height:6px;border-radius:50%;background:{dot};'
        f'display:inline-block;"></span>{label}</span><br><br>',
        unsafe_allow_html=True,
    )


def render_heal_banner():
    """Show a contextual banner when healing has been/is being attempted."""
    cause   = st.session_state.heal_cause
    summary = st.session_state.heal_summary
    if not cause:
        return

    cause_labels = {
        "invalid_transforms":  "Invalid Transform Expressions",
        "null_values":         "Null Value in Column",
        "type_cast_error":     "Type Cast Failure",
        "expression_error":    "PySpark Expression Error",
        "schema_mismatch":     "Schema Mismatch",
        "cluster_oom":         "Cluster Out of Memory",
        "cluster_error":       "Cluster Error",
        "auth_error":          "Authentication Error",
        "dbfs_missing":        "DBFS Path Not Found",
        "workspace_error":     "Workspace Upload Error",
        "job_timeout":         "Job Timeout",
        "notebook_exit_error": "Notebook Did Not Exit Cleanly",
        "copy_failure":        "CSV Copy Stage Failure",
        "unknown":             "Unknown Error",
    }
    label = cause_labels.get(cause, cause)
    lines_html = "".join(f"<div>• {l}</div>" for l in summary[:6])

    st.markdown(f"""
    <div class="healing-banner">
        <b>🛠 Self-Healing Agent activated — {label}</b>
        <div style="margin-top:0.5rem;font-size:0.7rem;opacity:0.85;">{lines_html}</div>
    </div>""", unsafe_allow_html=True)


def render_plan(config: dict, schema: dict, used_fallback: bool = False):
    if used_fallback:
        st.markdown("""
        <div style="background:#fff7ed;border:1px solid #f59e0b;border-radius:8px;padding:12px;margin-bottom:16px;">
            <strong>Using Default Configuration</strong> — Groq API unavailable. Running with default pipeline.
        </div>""", unsafe_allow_html=True)

    cols = st.columns(len(config["containers"]))
    for i, (role, name) in enumerate(config["containers"].items()):
        with cols[i]:
            st.metric(role.upper(), name)
    st.markdown("<br>", unsafe_allow_html=True)

    rows = "".join(
        f"<tr>"
        f"<td>{p['name']}</td>"
        f"<td><span class='pill pill-{p['type']}'>{p['type']}</span></td>"
        f"<td>{p['source_dataset']}</td>"
        f"<td>{p['sink_dataset']}</td>"
        f"<td>{p.get('num_workers', '-')}</td>"
        f"<td>{p.get('shuffle_partitions', '-')}</td>"
        f"</tr>"
        for p in config["pipelines"]
    )
    st.markdown(f"""
    <div class="card-label">Pipelines</div>
    <table class="plan-table">
      <thead><tr><th>Name</th><th>Type</th><th>Source</th><th>Sink</th><th>Workers</th><th>Shuffle Parts</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>""", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    if "recommended_settings" in config:
        rec = config["recommended_settings"]
        st.markdown(f"""
        <div class="card-label">Recommended Cluster Settings</div>
        <table class="plan-table">
          <thead><tr><th>Setting</th><th>Value</th></tr></thead>
          <tbody>
            <tr><td>Node Type</td><td>{rec.get('node_type', 'N/A')}</td></tr>
            <tr><td>Workers</td><td>{rec.get('num_workers', 'N/A')}</td></tr>
            <tr><td>Shuffle Partitions</td><td>{rec.get('shuffle_partitions', 'N/A')}</td></tr>
          </tbody>
        </table>""", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    type_rows = "".join(
        f"<tr><td>{col}</td>"
        f"<td><span class='pill pill-{schema['inferred_types'].get(col,'string')}'>"
        f"{schema['inferred_types'].get(col,'string')}</span></td></tr>"
        for col in schema["columns"]
    )
    st.markdown(f"""
    <div class="card-label">
        CSV Schema — {len(schema['columns'])} cols &nbsp;·&nbsp;
        ~{schema['row_count']:,} rows &nbsp;·&nbsp; {schema['size_hint']}
    </div>
    <table class="plan-table">
      <thead><tr><th>Column</th><th>Type</th></tr></thead>
      <tbody>{type_rows}</tbody>
    </table>""", unsafe_allow_html=True)

    st.markdown(f"""
    <br>
    <div style="font-family:'IBM Plex Mono',monospace;font-size:0.74rem;color:#64748b;line-height:1.9;">
        <b style="color:#475569;">Execution order:</b> {' → '.join(config['execution_order'])}<br>
        <b style="color:#475569;">Reasoning:</b> {config.get('reasoning','N/A')}
    </div>""", unsafe_allow_html=True)


def render_logs():
    def colour(line: str) -> str:
        l = line.lower()
        if any(x in l for x in ["🔧", "🛠", "🔁", "self-heal", "heal"]):
            return f'<span class="log-heal">{line}</span>'
        if any(x in l for x in ["failed", "error", "abort", "invalid", "exception"]):
            return f'<span class="log-error">{line}</span>'
        if any(x in l for x in ["succeeded", "created", "uploaded", "complete", "ready", "done", "✅"]):
            return f'<span class="log-success">{line}</span>'
        if any(x in l for x in ["waiting", "retrying", "timeout", "warn", "purging", "polling", "⚠"]):
            return f'<span class="log-warn">{line}</span>'
        if any(x in l for x in ["databricks", "groq", "dbfs", "spark", "building", "triggering", "creating"]):
            return f'<span class="log-info">{line}</span>'
        if "monitor" in l:
            return f'<span class="log-monitor">{line}</span>'
        return f'<span class="log-default">{line}</span>'

    lines = st.session_state.logs
    content = (
        '<span class="log-default">Waiting for pipeline to start…</span>'
        if not lines
        else "\n".join(colour(l) for l in lines)
    )
    st.markdown(f'<div class="log-box">{content}</div>', unsafe_allow_html=True)


def render_monitor_section():
    def colour(line: str) -> str:
        l = line.lower()
        if any(x in l for x in ["failed", "error", "critical"]):
            return f'<span style="color:#ef4444;font-weight:600;">{line}</span>'
        if any(x in l for x in ["warning", "warn"]):
            return f'<span style="color:#f59e0b;">{line}</span>'
        if any(x in l for x in ["succeeded", "healthy", "found"]):
            return f'<span style="color:#22c55e;">{line}</span>'
        return f'<span style="color:#94a3b8;">{line}</span>'

    lines = st.session_state.monitor_logs
    content = (
        '<span style="color:#94a3b8;">Monitor idle…</span>'
        if not lines
        else "\n".join(colour(l) for l in lines)
    )
    st.markdown(f'<div class="log-box">{content}</div>', unsafe_allow_html=True)


def _status_pill(status: str) -> str:
    s = status.lower().replace(" ", "")
    cls = {
        "succeeded": "succeeded", "failed": "failed",
        "running": "running", "inprogress": "inprogress",
        "queued": "queued",
    }.get(s, "unknown")
    return f"<span class='pill pill-{cls}'>{status}</span>"


def render_live_runs(runs: list):
    if not runs:
        st.markdown(
            '<div style="font-family:IBM Plex Mono,monospace;font-size:0.75rem;'
            'color:#94a3b8;padding:1rem;">No run data yet.</div>',
            unsafe_allow_html=True,
        )
        return

    total     = len(runs)
    succeeded = sum(1 for r in runs if r["status"] == "Succeeded")
    failed    = sum(1 for r in runs if r["status"] == "Failed")
    running   = sum(1 for r in runs if r["status"] == "InProgress")
    rate      = int(100 * succeeded / total) if total else 0
    ok_cls    = "mon-card-ok" if failed == 0 else "mon-card-err"
    rate_cls  = "mon-card-ok" if rate >= 80 else "mon-card-warn"

    st.markdown(f"""
    <div class="mon-grid">
      <div class="mon-card mon-card-blue">
        <div class="mon-card-label">Total Runs</div>
        <div class="mon-card-value">{total}</div>
        <div class="mon-card-sub">fetched</div>
      </div>
      <div class="mon-card {ok_cls}">
        <div class="mon-card-label">Succeeded</div>
        <div class="mon-card-value" style="color:#16a34a">{succeeded}</div>
        <div class="mon-card-sub">{running} in progress</div>
      </div>
      <div class="mon-card {'mon-card-err' if failed>0 else 'mon-card-ok'}">
        <div class="mon-card-label">Failed</div>
        <div class="mon-card-value" style="color:{'#dc2626' if failed>0 else '#16a34a'}">{failed}</div>
        <div class="mon-card-sub">{'needs attention' if failed>0 else 'all clear'}</div>
      </div>
      <div class="mon-card {rate_cls}">
        <div class="mon-card-label">Success Rate</div>
        <div class="mon-card-value">{rate}%</div>
        <div class="mon-card-sub">last {total} runs</div>
      </div>
    </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    for run in runs:
        icon  = {"Succeeded": "✓", "Failed": "✗", "InProgress": "⟳"}.get(run["status"], "·")
        label = (
            f"{icon}  #{run['run_id']}  ·  {run.get('run_name','') or run.get('pipeline','—')}"
            f"  ·  {run['status']}  ·  {run.get('duration','N/A')}"
            f"  ·  {run.get('started','')[:16] or '—'}"
        )

        with st.expander(label):

            def _r(k, v):
                val = str(v) if v not in (None, "", 0, False) else "—"
                return (
                    f"<tr>"
                    f"<td style='color:#64748b;font-weight:500;white-space:nowrap;"
                    f"padding:0.28rem 0.8rem 0.28rem 0;font-size:0.7rem;'>{k}</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
                    f"word-break:break-all;'>{val}</td>"
                    f"</tr>"
                )

            def _section(title):
                return (
                    f"<div style='font-size:0.58rem;text-transform:uppercase;"
                    f"letter-spacing:0.14em;color:#e25a1c;font-weight:600;"
                    f"margin-bottom:0.4rem;margin-top:0.8rem;'>{title}</div>"
                )

            col_a, col_b = st.columns(2)

            with col_a:
                url     = run.get("run_page_url", "")
                url_td  = (
                    f'<a href="{url}" target="_blank" style="color:#ea580c;">'
                    f'{url[:55]}{"…" if len(url)>55 else ""}</a>'
                    if url else "—"
                )
                st.markdown(f"""
                <div style="font-family:'IBM Plex Sans',sans-serif;">
                  {_section("Identity")}
                  <table style="width:100%;border-collapse:collapse;">
                    {_r("Run ID",        run.get("run_id"))}
                    {_r("Job ID",        run.get("job_id"))}
                    {_r("Run Name",      run.get("run_name"))}
                    {_r("# in Job",      run.get("number_in_job"))}
                    {_r("Attempt #",     run.get("attempt_number"))}
                    {_r("Run Type",      run.get("run_type"))}
                    {_r("Format",        run.get("format"))}
                    {_r("Trigger",       run.get("trigger"))}
                    {_r("Creator",       run.get("creator_user_name"))}
                    <tr>
                      <td style='color:#64748b;font-weight:500;white-space:nowrap;
                                 padding:0.28rem 0.8rem 0.28rem 0;font-size:0.7rem;'>Run Page</td>
                      <td style='font-size:0.7rem;'>{url_td}</td>
                    </tr>
                  </table>
                  {_section("Status")}
                  <table style="width:100%;border-collapse:collapse;">
                    <tr>
                      <td style='color:#64748b;font-weight:500;white-space:nowrap;
                                 padding:0.28rem 0.8rem 0.28rem 0;font-size:0.7rem;'>Status</td>
                      <td>{_status_pill(run["status"])}</td>
                    </tr>
                    {_r("Lifecycle State",  run.get("life_cycle_state"))}
                    {_r("Result State",     run.get("result_state"))}
                    {_r("State Message",    run.get("state_message"))}
                    {_r("User Cancelled",   run.get("user_cancelled"))}
                  </table>
                </div>""", unsafe_allow_html=True)

            with col_b:
                setup_s   = run.get("setup_duration_s", 0) or 0
                exec_s    = run.get("exec_duration_s", 0) or 0
                cleanup_s = run.get("cleanup_duration_s", 0) or 0
                seg_total = (setup_s + exec_s + cleanup_s) or 1

                def _bar(label, val, color):
                    pct = max(1, int(100 * val / seg_total)) if val else 0
                    return (
                        f"<tr>"
                        f"<td style='color:#64748b;font-size:0.7rem;white-space:nowrap;"
                        f"padding:0.28rem 0.6rem 0.28rem 0;font-weight:500;'>{label}</td>"
                        f"<td style='width:100%;'>"
                        f"<div style='display:flex;align-items:center;gap:0.35rem;'>"
                        f"<div style='flex:1;background:#e2e8f0;border-radius:3px;"
                        f"height:11px;min-width:40px;overflow:hidden;'>"
                        f"<div style='width:{pct}%;background:{color};height:100%;border-radius:3px;'>"
                        f"</div></div>"
                        f"<span style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;"
                        f"white-space:nowrap;'>{val}s</span>"
                        f"</div></td></tr>"
                    )

                st.markdown(f"""
                <div style="font-family:'IBM Plex Sans',sans-serif;">
                  {_section("Timing")}
                  <table style="width:100%;border-collapse:collapse;">
                    {_r("Started",        run.get("started"))}
                    {_r("Ended",          run.get("ended"))}
                    {_r("Total Duration", run.get("duration"))}
                    {_r("Run Duration",   _fmt_duration(run.get("run_duration_s")))}
                  </table>
                  <table style="width:100%;border-collapse:collapse;margin-top:0.5rem;">
                    {_bar("Setup",     setup_s,   "#3b82f6")}
                    {_bar("Execution", exec_s,    "#e25a1c")}
                    {_bar("Cleanup",   cleanup_s, "#16a34a")}
                  </table>
                  {_section("Cluster")}
                  <table style="width:100%;border-collapse:collapse;">
                    {_r("Cluster ID",       run.get("cluster_id"))}
                    {_r("Spark Context ID", run.get("spark_context_id"))}
                  </table>
                </div>""", unsafe_allow_html=True)

            # Compute Resources
            cid = run.get("cluster_id", "")
            if not cid:
                for t in run.get("tasks", []):
                    if t.get("cluster_id"):
                        cid = t["cluster_id"]
                        break

            cinfo    = st.session_state.get("cluster_cache", {}).get(cid, {})
            rmetrics = st.session_state.get("run_metrics_cache", {}).get(run.get("run_id"))

            st.markdown(
                "<div style='font-size:0.58rem;text-transform:uppercase;"
                "letter-spacing:0.14em;color:#e25a1c;font-weight:600;"
                "margin:0.9rem 0 0.5rem;'>Actual Runtime Resources</div>",
                unsafe_allow_html=True,
            )

            if rmetrics is None:
                st.markdown(
                    "<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
                    "color:#94a3b8;'>Click Refresh to fetch runtime metrics.</div>",
                    unsafe_allow_html=True,
                )
            else:
                peak_ex  = rmetrics.get("peak_executors")
                run_size = rmetrics.get("cluster_size_at_run")
                d_cpu    = rmetrics.get("driver_cpu_pct")
                e_cpu    = rmetrics.get("executor_avg_cpu_pct")
                d_mem_u  = rmetrics.get("driver_mem_used_gb")
                d_mem_t  = rmetrics.get("driver_mem_total_gb")
                e_mem_u  = rmetrics.get("executor_mem_used_gb")
                e_mem_t  = rmetrics.get("executor_mem_total_gb")
                has_data = any(v is not None for v in [peak_ex, run_size, d_cpu, e_cpu, d_mem_u, e_mem_u])

                if has_data:
                    rm1, rm2, rm3, rm4 = st.columns(4)
                    with rm1: st.metric("Peak Executors", peak_ex if peak_ex is not None else "—")
                    with rm2: st.metric("Workers at Run", run_size if run_size is not None else "—")
                    with rm3: st.metric("Driver CPU", f"{d_cpu:.1f}%" if d_cpu is not None else "—")
                    with rm4: st.metric("Executor CPU (avg)", f"{e_cpu:.1f}%" if e_cpu is not None else "—")

                    rm5, rm6, rm7, rm8 = st.columns(4)
                    with rm5:
                        label = (f"{d_mem_u} / {d_mem_t} GB" if d_mem_u is not None and d_mem_t is not None
                                 else (f"{d_mem_u} GB" if d_mem_u is not None else "—"))
                        st.metric("Driver Mem Used/Total", label)
                    with rm6:
                        label = (f"{e_mem_u} / {e_mem_t} GB" if e_mem_u is not None and e_mem_t is not None
                                 else (f"{e_mem_u} GB" if e_mem_u is not None else "—"))
                        st.metric("Executor Mem Used/Total", label)
                    with rm7:
                        shuffle = cinfo.get("spark_conf", {}).get("spark.sql.shuffle.partitions", "—") if cinfo else "—"
                        st.metric("Shuffle Partitions", shuffle)
                    with rm8:
                        st.metric("Exec Duration", _fmt_duration(run.get("exec_duration_s")))

                    event_log = rmetrics.get("event_log", [])
                    if event_log:
                        st.markdown(
                            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.67rem;"
                            f"color:#64748b;margin-top:0.4rem;'>Events: "
                            f"{' → '.join(event_log)}</div>", unsafe_allow_html=True,
                        )
                else:
                    state_msg    = cinfo.get("state_message", "") if cinfo else ""
                    cluster_state = cinfo.get("state", "") if cinfo else ""
                    if state_msg:
                        color = "#dc2626" if cluster_state in ("ERROR", "TERMINATED") else "#d97706"
                        st.markdown(
                            f"<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
                            f"color:{color};line-height:1.6;padding:0.4rem 0.7rem;"
                            f"background:#fef2f2;border-left:3px solid {color};"
                            f"border-radius:0 5px 5px 0;margin-bottom:0.4rem;'>"
                            f"<b>{cluster_state}</b> — {state_msg}</div>", unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            "<div style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
                            "color:#94a3b8;'>No resource events found for this run.</div>",
                            unsafe_allow_html=True,
                        )

            if cinfo:
                autoscale   = cinfo.get("autoscale", {})
                workers_str = (
                    f"{autoscale.get('min_workers','?')}–{autoscale.get('max_workers','?')} (autoscale)"
                    if autoscale else str(cinfo.get("num_workers", 0))
                )
                mem_gb = cinfo.get("cluster_memory_gb", 0)
                cores  = cinfo.get("cluster_cores", 0)

                st.markdown(
                    "<div style='font-size:0.58rem;text-transform:uppercase;"
                    "letter-spacing:0.14em;color:#e25a1c;font-weight:600;"
                    "margin:0.9rem 0 0.4rem;'>Configured Cluster Spec</div>",
                    unsafe_allow_html=True,
                )
                compute_rows = "".join([
                    f"<tr><td style='color:#64748b;font-weight:500;font-size:0.7rem;"
                    f"white-space:nowrap;padding:0.28rem 0.8rem 0.28rem 0;'>{k}</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;font-size:0.7rem;"
                    f"word-break:break-all;'>{v or '—'}</td></tr>"
                    for k, v in [
                        ("Cluster Name",       cinfo.get("cluster_name", "")),
                        ("Cluster ID",         cinfo.get("cluster_id", "")),
                        ("State",              cinfo.get("state", "")),
                        ("Node Type",          cinfo.get("node_type_id", "")),
                        ("Driver Node Type",   cinfo.get("driver_node_type_id", "")),
                        ("Configured Workers", workers_str),
                        ("vCPUs (capacity)",   cores if cores else "—"),
                        ("Memory (capacity)",  f"{mem_gb} GB" if mem_gb else "—"),
                        ("Spark Version",      cinfo.get("spark_version", "")),
                        ("Cluster Source",     cinfo.get("cluster_source", "")),
                        ("Creator",            cinfo.get("creator_user_name", "")),
                        ("Spark Context ID",   run.get("spark_context_id", "")),
                        ("State Message",      cinfo.get("state_message", "")),
                    ]
                ])
                st.markdown(
                    f"<table style='width:100%;border-collapse:collapse;'>"
                    f"<tbody>{compute_rows}</tbody></table>",
                    unsafe_allow_html=True,
                )

            tasks = run.get("tasks", [])
            if tasks:
                task_rows = "".join(
                    f"<tr>"
                    f"<td>{t.get('task_key','')}</td>"
                    f"<td>{_status_pill(t.get('result') or t.get('life_cycle',''))}</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;font-size:0.66rem;'>{t.get('start_time','')}</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;font-size:0.66rem;'>{t.get('end_time','')}</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;'>{t.get('duration','')}</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;'>{t.get('setup_s',0)}s</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;'>{t.get('exec_s',0)}s</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;'>{t.get('cleanup_s',0)}s</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;font-size:0.64rem;'>{t.get('cluster_id','')}</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;'>{t.get('run_id','')}</td>"
                    f"<td style='font-family:IBM Plex Mono,monospace;'>{t.get('attempt',0)}</td>"
                    f"</tr>"
                    for t in tasks
                )
                st.markdown(
                    f"<div style='font-size:0.58rem;text-transform:uppercase;"
                    f"letter-spacing:0.14em;color:#e25a1c;font-weight:600;"
                    f"margin:0.9rem 0 0.45rem;'>Tasks ({len(tasks)})</div>"
                    f"<div style='overflow-x:auto;'>"
                    f"<table class='plan-table' style='font-size:0.67rem;'>"
                    f"<thead><tr><th>Task Key</th><th>Status</th><th>Start</th><th>End</th>"
                    f"<th>Duration</th><th>Setup</th><th>Exec</th><th>Cleanup</th>"
                    f"<th>Cluster ID</th><th>Run ID</th><th>Attempt</th></tr></thead>"
                    f"<tbody>{task_rows}</tbody></table></div>",
                    unsafe_allow_html=True,
                )


# ── Monitor helpers ────────────────────────────────────────────────────────────
def _cluster_state_pill(state: str) -> str:
    cls_map = {
        "RUNNING": "inprogress", "TERMINATED": "unknown",
        "TERMINATING": "queued", "ERROR": "failed",
        "PENDING": "queued",    "RESTARTING": "queued",
    }
    cls = cls_map.get(state, "unknown")
    return f"<span class='pill pill-{cls}'>{state}</span>"


def render_monitor_overview(runs: list, clusters: list):
    total     = len(runs)
    succeeded = sum(1 for r in runs if r["status"] == "Succeeded")
    failed    = sum(1 for r in runs if r["status"] == "Failed")
    running   = sum(1 for r in runs if r["status"] == "InProgress")
    rate      = int(100 * succeeded / total) if total else 0
    durations = [r["duration_s"] for r in runs if r.get("duration_s") is not None]
    avg_dur   = sum(durations) / len(durations) if durations else None
    fastest   = min(durations) if durations else None
    slowest   = max(durations) if durations else None

    st.markdown('<div class="card-label" style="margin-bottom:0.8rem;">Run Summary</div>', unsafe_allow_html=True)
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("Total Runs",  total)
    with c2: st.metric("Succeeded",   succeeded)
    with c3: st.metric("Failed",      failed)
    with c4: st.metric("In Progress", running)
    with c5: st.metric("Success Rate", f"{rate}%")

    st.markdown("<br>", unsafe_allow_html=True)
    c6, c7 = st.columns(2)
    with c6: st.metric("Avg Duration", _fmt_duration(avg_dur))
    with c7:
        if fastest is not None:
            st.metric("Fastest / Slowest", f"{_fmt_duration(fastest)} / {_fmt_duration(slowest)}")

    if clusters:
        st.markdown('<hr class="sec-divider">', unsafe_allow_html=True)
        st.markdown('<div class="card-label" style="margin-bottom:0.8rem;">Cluster Health</div>', unsafe_allow_html=True)
        active = sum(1 for c in clusters if c["state"] == "RUNNING")
        cluster_rows = "".join(
            f"<tr>"
            f"<td style='font-weight:500;'>{c.get('cluster_name','') or c['cluster_id'][:14]+'...'}</td>"
            f"<td>{_cluster_state_pill(c.get('state',''))}</td>"
            f"<td style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;'>{c.get('node_type_id','N/A')}</td>"
            f"<td style='font-family:IBM Plex Mono,monospace;'>{c.get('cluster_cores',0)}</td>"
            f"<td style='font-family:IBM Plex Mono,monospace;'>{c.get('cluster_memory_gb',0)} GB</td>"
            f"<td style='font-family:IBM Plex Mono,monospace;'>{c.get('num_workers',0)}</td>"
            f"</tr>"
            for c in clusters
        )
        st.markdown(f"""
        <div style="font-family:'IBM Plex Mono',monospace;font-size:0.68rem;color:#64748b;margin-bottom:0.6rem;">
          {active}/{len(clusters)} cluster(s) active
        </div>
        <table class="plan-table">
          <thead><tr><th>Name</th><th>State</th><th>Node Type</th><th>Cores</th><th>Memory</th><th>Workers</th></tr></thead>
          <tbody>{cluster_rows}</tbody>
        </table>""", unsafe_allow_html=True)


def render_cluster_tab(clusters: list):
    if not clusters:
        st.markdown(
            '<div style="color:#94a3b8;font-family:IBM Plex Mono,monospace;font-size:0.75rem;padding:1rem;">'
            'No cluster data — click Refresh.</div>', unsafe_allow_html=True,
        )
        return

    states: dict = {}
    for c in clusters:
        s = c.get("state", "UNKNOWN")
        states[s] = states.get(s, 0) + 1

    state_colors = {
        "RUNNING": "#16a34a", "TERMINATED": "#94a3b8", "TERMINATING": "#d97706",
        "ERROR": "#dc2626",   "PENDING": "#3b82f6",    "RESTARTING": "#d97706",
    }
    cols = st.columns(max(len(states), 1))
    for i, (state, count) in enumerate(states.items()):
        col_hex = state_colors.get(state, "#64748b")
        with cols[i]:
            st.markdown(f"""
            <div class="mon-card" style="border-color:{col_hex}55;">
              <div class="mon-card-label">{state}</div>
              <div class="mon-card-value" style="color:{col_hex};">{count}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    cluster_rows = "".join(
        f"<tr>"
        f"<td style='font-weight:500;'>{c.get('cluster_name','') or c['cluster_id'][:16]+'...'}</td>"
        f"<td>{_cluster_state_pill(c.get('state',''))}</td>"
        f"<td style='font-family:IBM Plex Mono,monospace;font-size:0.68rem;'>{c.get('node_type_id','N/A')}</td>"
        f"<td style='font-family:IBM Plex Mono,monospace;'>{c.get('cluster_cores',0)}</td>"
        f"<td style='font-family:IBM Plex Mono,monospace;'>{c.get('cluster_memory_gb',0)} GB</td>"
        f"<td style='font-family:IBM Plex Mono,monospace;'>{c.get('num_workers',0)}</td>"
        f"<td style='font-family:IBM Plex Mono,monospace;font-size:0.66rem;'>{c.get('spark_version','')}</td>"
        f"<td style='font-family:IBM Plex Mono,monospace;font-size:0.66rem;'>{c.get('creator_user_name','')}</td>"
        f"</tr>"
        for c in clusters
    )
    st.markdown(f"""
    <table class="plan-table">
      <thead><tr><th>Name</th><th>State</th><th>Node Type</th><th>Cores</th>
      <th>Memory</th><th>Workers</th><th>Spark</th><th>Owner</th></tr></thead>
      <tbody>{cluster_rows}</tbody>
    </table>""", unsafe_allow_html=True)


def _render_run_detail(data: dict):
    det = data.get("details", {})
    clu = data.get("cluster", {})
    if not det:
        st.warning("No detail data.")
        return

    setup_s   = det.get("setup_duration_s", 0)
    exec_s    = det.get("execution_duration_s", 0)
    cleanup_s = det.get("cleanup_duration_s", 0)
    total_s   = (setup_s + exec_s + cleanup_s) or 1

    def _bar(label, value_s, color):
        pct = max(1, int(100 * value_s / total_s)) if value_s else 0
        return (
            f'<div style="display:flex;gap:0.6rem;align-items:center;margin-bottom:0.7rem;">'
            f'<span style="font-family:IBM Plex Mono,monospace;font-size:0.7rem;'
            f'color:#64748b;width:88px;flex-shrink:0;">{label}</span>'
            f'<div style="flex:1;background:#e2e8f0;border-radius:4px;height:16px;overflow:hidden;">'
            f'<div style="width:{pct}%;background:{color};height:100%;border-radius:4px;"></div></div>'
            f'<span style="font-family:IBM Plex Mono,monospace;font-size:0.7rem;'
            f'color:#1e293b;width:56px;text-align:right;flex-shrink:0;">{value_s}s</span>'
            f'</div>'
        )

    bars = _bar("Setup", setup_s, "#3b82f6") + _bar("Execution", exec_s, "#e25a1c") + _bar("Cleanup", cleanup_s, "#16a34a")
    st.markdown(f"""
    <div class="card" style="margin-top:1rem;">
      <div class="card-label">Timing Breakdown — Run {det.get('run_id')}</div>
      {bars}
      <div style="font-family:IBM Plex Mono,monospace;font-size:0.68rem;color:#64748b;margin-top:0.4rem;">
        Job: {det.get('job_id','-')} &nbsp;·&nbsp; Trigger: {det.get('trigger','-') or 'manual'} &nbsp;·&nbsp; Creator: {det.get('creator_user_name','-')}
      </div>
    </div>""", unsafe_allow_html=True)

    if clu:
        autoscale   = clu.get("autoscale", {})
        workers_str = (
            f"{autoscale['min_workers']}–{autoscale['max_workers']} (auto)"
            if autoscale else str(clu.get("num_workers", 0))
        )
        state_color = {"RUNNING": "#16a34a", "TERMINATED": "#94a3b8", "ERROR": "#dc2626"}.get(clu.get("state", ""), "#64748b")
        shuffle     = clu.get("spark_conf", {}).get("spark.sql.shuffle.partitions", "-")
        st.markdown(f"""
        <div class="card">
          <div class="card-label">Cluster Resources — {clu.get('cluster_name','N/A')}</div>
          <div class="mon-grid">
            <div class="mon-card"><div class="mon-card-label">State</div>
              <div class="mon-card-value" style="font-size:0.9rem;color:{state_color};">{clu.get('state','N/A')}</div></div>
            <div class="mon-card"><div class="mon-card-label">Node Type</div>
              <div class="mon-card-value" style="font-size:0.8rem;">{clu.get('node_type_id','N/A')}</div></div>
            <div class="mon-card"><div class="mon-card-label">Cores / Memory</div>
              <div class="mon-card-value" style="font-size:0.9rem;">{clu.get('cluster_cores',0)} cores / {clu.get('cluster_memory_gb',0)} GB</div></div>
            <div class="mon-card"><div class="mon-card-label">Workers</div>
              <div class="mon-card-value" style="font-size:0.9rem;">{workers_str}</div></div>
          </div>
          <div style="font-family:IBM Plex Mono,monospace;font-size:0.68rem;color:#64748b;margin-top:0.4rem;">
            Spark: {clu.get('spark_version','-')} &nbsp;·&nbsp; Cluster ID: {clu.get('cluster_id','-')} &nbsp;·&nbsp; Shuffle partitions: {shuffle}
          </div>
        </div>""", unsafe_allow_html=True)

    tasks = det.get("tasks", [])
    if tasks:
        task_rows = "".join(
            f"<tr><td>{t['task_key']}</td><td>{_status_pill(t['status'])}</td>"
            f"<td style='font-family:IBM Plex Mono,monospace;'>{t['duration']}</td>"
            f"<td style='font-family:IBM Plex Mono,monospace;'>{t.get('run_id','')}</td>"
            f"<td style='font-family:IBM Plex Mono,monospace;'>{t.get('attempt',0)}</td></tr>"
            for t in tasks
        )
        st.markdown(f"""
        <div class="card-label" style="margin-top:1rem;">Tasks</div>
        <table class="plan-table">
          <thead><tr><th>Task Key</th><th>Status</th><th>Duration</th><th>Run ID</th><th>Attempt</th></tr></thead>
          <tbody>{task_rows}</tbody>
        </table>""", unsafe_allow_html=True)


def render_job_runs_tab(runs: list):
    if not runs:
        st.markdown(
            '<div style="color:#94a3b8;font-family:IBM Plex Mono,monospace;font-size:0.75rem;padding:1rem;">'
            'No run data — click Refresh.</div>', unsafe_allow_html=True,
        )
        return

    render_live_runs(runs)
    st.markdown('<hr class="sec-divider">', unsafe_allow_html=True)
    st.markdown('<div class="card-label">Inspect Run</div>', unsafe_allow_html=True)

    run_options = {str(r["run_id"]): f"Run {r['run_id']} — {r['pipeline']} ({r['status']})" for r in runs}
    col_sel, col_btn = st.columns([3, 1])
    with col_sel:
        selected = st.selectbox("Select run", options=list(run_options.keys()),
                                format_func=lambda x: run_options.get(x, x),
                                label_visibility="collapsed")
    with col_btn:
        if st.button("Load Details", use_container_width=True):
            from databricks_api import get_run_details, get_cluster_info
            with st.spinner("Fetching..."):
                det = get_run_details(int(selected))
                clu = get_cluster_info(det.get("cluster_id", "")) if det.get("cluster_id") else {}
                st.session_state.inspected_run = {"details": det, "cluster": clu}

    if st.session_state.inspected_run:
        _render_run_detail(st.session_state.inspected_run)


def render_analytics_tab(runs: list):
    if len(runs) < 2:
        st.markdown(
            '<div style="color:#94a3b8;font-family:IBM Plex Mono,monospace;font-size:0.75rem;padding:1rem;">'
            'Need 2+ runs for analytics.</div>', unsafe_allow_html=True,
        )
        return

    st.markdown('<div class="card-label">Run Duration (seconds)</div>', unsafe_allow_html=True)
    dur_data = {f"#{r['run_id']}": round(r["duration_s"], 1) for r in runs if r.get("duration_s") is not None}
    if dur_data:
        st.bar_chart(dur_data)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="card-label">Status Distribution</div>', unsafe_allow_html=True)
    status_counts: dict = {}
    for r in runs:
        status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
    if status_counts:
        st.bar_chart(status_counts)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="card-label">Slowest Runs</div>', unsafe_allow_html=True)
    sorted_runs = sorted(
        [r for r in runs if r.get("duration_s") is not None],
        key=lambda x: x["duration_s"], reverse=True,
    )[:5]
    if sorted_runs:
        rows = "".join(
            f"<tr><td>{r['pipeline']}</td><td>#{r['run_id']}</td>"
            f"<td>{_status_pill(r['status'])}</td>"
            f"<td style='font-family:IBM Plex Mono,monospace;'>{r['duration']}</td>"
            f"<td style='font-family:IBM Plex Mono,monospace;font-size:0.67rem;'>{r['started'][:16]}</td></tr>"
            for r in sorted_runs
        )
        st.markdown(f"""
        <table class="plan-table">
          <thead><tr><th>Pipeline</th><th>Run ID</th><th>Status</th><th>Duration</th><th>Started</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>""", unsafe_allow_html=True)


# ── Stage: INPUT ───────────────────────────────────────────────────────────────
def stage_input():
    render_header()
    render_status_badge()

    col_upload, col_settings = st.columns([3, 2])

    with col_upload:
        st.markdown('<div class="card-label">Upload CSV</div>', unsafe_allow_html=True)
        uploaded = st.file_uploader("Upload CSV file", type=["csv"], label_visibility="collapsed")

        st.markdown('<div class="card-label" style="margin-top:1rem;">Pipeline Prompt</div>', unsafe_allow_html=True)
        prompt = st.text_area(
            "Pipeline prompt",
            placeholder="e.g. 'Filter rows where eggs = 1 and uppercase the name column'",
            height=100,
            value=st.session_state.user_prompt,
            label_visibility="collapsed",
        )
        st.session_state.user_prompt = prompt

    with col_settings:
        st.markdown('<div class="card-label">Pipeline Configuration</div>', unsafe_allow_html=True)
        num_stages = st.number_input("Number of stages (2–5)", min_value=2, max_value=5, value=3)

        st.markdown('<div class="card-label" style="margin-top:0.8rem;">Container Names (optional)</div>', unsafe_allow_html=True)
        container_names_raw = st.text_input("Container names", placeholder="incoming,bronze,silver",
                                             label_visibility="collapsed")
        container_names = None
        if container_names_raw.strip():
            parts = [p.strip() for p in container_names_raw.split(",")]
            if len(parts) == num_stages:
                container_names = parts
            else:
                st.warning(f"Expected {num_stages} names, got {len(parts)} — using defaults")

        st.markdown('<div class="card-label" style="margin-top:0.8rem;">Cluster Override (optional)</div>', unsafe_allow_html=True)
        workers_override = st.number_input("Workers (0 = auto)", min_value=0, max_value=64, value=0)
        shuffle_override = st.number_input("Shuffle Partitions (0 = auto)", min_value=0, max_value=256, value=0)

    st.markdown("<br>", unsafe_allow_html=True)
    run_col, mon_col = st.columns([3, 1])
    with run_col:
        run_clicked = st.button("Design & Run Pipeline", use_container_width=True)
    with mon_col:
        mon_clicked = st.button("Monitor Only", use_container_width=True)

    if mon_clicked:
        st.session_state.stage = "monitor"
        st.session_state.monitor_only = True
        st.rerun()

    if run_clicked:
        if uploaded is None:
            st.error("Please upload a CSV file.")
            return
        if not prompt.strip():
            st.error("Please enter a pipeline prompt.")
            return

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded.getvalue())
            tmp_path = tmp.name
        st.session_state.csv_tmp_path = tmp_path

        schema = read_csv_schema(tmp_path)
        st.session_state.schema = schema

        custom = {}
        if workers_override > 0:
            custom["num_workers"] = workers_override
        if shuffle_override > 0:
            custom["shuffle_partitions"] = shuffle_override

        from databricks_groq_brain import decide_pipeline_config
        config, used_fallback = decide_pipeline_config(
            schema=schema, user_prompt=prompt, num_containers=num_stages,
            custom_settings=custom if custom else None, container_names=container_names,
        )

        st.session_state.pipeline_config = config
        st.session_state.used_fallback   = used_fallback
        st.session_state.stage           = "plan"
        st.session_state.logs            = []
        st.session_state.progress        = 0
        # Reset healing state for new run
        st.session_state.heal_attempted  = False
        st.session_state.heal_cause      = None
        st.session_state.heal_summary    = []
        st.rerun()


# ── Stage: PLAN ────────────────────────────────────────────────────────────────
def stage_plan():
    render_header()
    render_status_badge()

    config = st.session_state.pipeline_config
    schema = st.session_state.schema

    st.markdown('<div class="card-label">Review Pipeline Plan</div>', unsafe_allow_html=True)
    render_plan(config, schema, used_fallback=st.session_state.used_fallback)

    st.markdown("<br>", unsafe_allow_html=True)
    col_run, col_back = st.columns([3, 1])
    with col_run:
        if st.button("Deploy & Run on Databricks", use_container_width=True):
            st.session_state.stage                   = "running"
            st.session_state.logs                    = []
            st.session_state.progress                = 0
            st.session_state.pipeline_thread_started = False
            st.session_state.heal_attempted          = False
            st.session_state.heal_cause              = None
            st.session_state.heal_summary            = []
            st.session_state.pipeline_start_ts       = datetime.now(timezone.utc)
            st.rerun()
    with col_back:
        if st.button("Back", use_container_width=True):
            st.session_state.stage = "input"
            st.rerun()


# ── Stage: RUNNING ─────────────────────────────────────────────────────────────
def stage_running():
    render_header()
    render_status_badge()

    # Show healing banner if the agent has already been invoked
    if st.session_state.heal_cause:
        render_heal_banner()

    if not st.session_state.pipeline_thread_started:
        from databricks_api import check_connection
        ok, conn_msg = check_connection()
        if not ok:
            st.session_state.run_error = f"Workspace connection failed: {conn_msg}"
            st.session_state.stage = "failed"
            st.rerun()
            return

        result_q = queue.Queue()
        t = threading.Thread(
            target=run_pipeline_thread,
            args=(
                st.session_state.csv_tmp_path,
                st.session_state.pipeline_config,
                st.session_state.schema,
                result_q,
            ),
            daemon=True,
        )
        t.start()
        st.session_state.pipeline_thread = t
        st.session_state._result_q       = result_q
        st.session_state.pipeline_thread_started = True
    else:
        result_q = st.session_state._result_q

    # Drain queue ──────────────────────────────────────────────
    terminal = None
    while not result_q.empty():
        msg_type, payload = result_q.get_nowait()
        if msg_type == "log":
            st.session_state.logs.append(payload)
        elif msg_type == "progress":
            st.session_state.progress = payload
        elif msg_type == "heal_start":
            # Agent has been invoked — store cause + summary for the banner
            st.session_state.heal_attempted = True
            st.session_state.heal_cause     = payload.get("cause")
            st.session_state.heal_summary   = payload.get("summary", [])
        elif msg_type == "ok":
            terminal = ("ok", payload)
        elif msg_type == "error":
            terminal = ("error", payload)

    if terminal:
        kind, data = terminal
        if kind == "ok":
            st.session_state.stage           = "done"
            st.session_state.stage_paths     = data.get("stage_paths", {})
            st.session_state.pipeline_end_ts = datetime.now(timezone.utc)
            try:
                if data.get("output_csv_bytes"):
                    st.session_state.output_csv      = data["output_csv_bytes"]
                    st.session_state.output_filename = data.get("output_csv_name", "output.csv")
                elif data.get("stage_paths"):
                    last_container = list(st.session_state.pipeline_config["containers"].values())[-1]
                    sink_dbfs = data["stage_paths"].get(last_container, "")
                    if sink_dbfs:
                        from databricks_api import fetch_output_from_dbfs, dbfs_list
                        entries = dbfs_list(sink_dbfs)
                        for entry in entries:
                            if "output.csv" in entry.get("path", "") or "staged.csv" in entry.get("path", ""):
                                out_data, out_name = fetch_output_from_dbfs(entry["path"])
                                if out_data:
                                    st.session_state.output_csv      = out_data
                                    st.session_state.output_filename = out_name or "output.csv"
                                    break
            except Exception as e:
                st.session_state.logs.append(f"Output fetch: {e}")
        else:
            st.session_state.run_error = data
            st.session_state.stage     = "failed"
        st.rerun()

    st.progress(min(st.session_state.progress / 100, 1.0))

    tab_log, tab_jobs = st.tabs(["Pipeline Log", "Live Jobs"])

    with tab_log:
        render_logs()

    with tab_jobs:
        col_check, col_lim = st.columns([2, 1])
        with col_check:
            check_jobs = st.button("↻ Check Databricks Jobs", use_container_width=True, key="running_check_jobs")
        with col_lim:
            jobs_limit = st.number_input("Max runs", min_value=1, value=20,
                                         label_visibility="collapsed", key="running_jobs_limit")

        if check_jobs:
            mq = queue.Queue()
            mt = threading.Thread(target=run_monitor_thread, args=(mq, jobs_limit), daemon=True)
            mt.start()
            mt.join(timeout=20)
            while not mq.empty():
                msg_type, payload = mq.get_nowait()
                if msg_type == "live_runs":    st.session_state.live_runs = payload
                elif msg_type == "clusters":   st.session_state.clusters = payload
                elif msg_type == "cluster_cache":     st.session_state.cluster_cache.update(payload)
                elif msg_type == "run_metrics_cache": st.session_state.run_metrics_cache.update(payload)
                elif msg_type == "monitor_log":       st.session_state.monitor_logs.append(payload)

        if st.session_state.live_runs:
            render_live_runs(st.session_state.live_runs)
        else:
            st.markdown(
                '<div style="font-family:IBM Plex Mono,monospace;font-size:0.74rem;'
                'color:#94a3b8;padding:0.8rem 0;">Click ↻ Check Databricks Jobs to load live run status.</div>',
                unsafe_allow_html=True,
            )

    if st.session_state.pipeline_thread.is_alive():
        time.sleep(2)
        st.rerun()


# ── Stage: DONE ────────────────────────────────────────────────────────────────
def stage_done():
    render_header()
    render_status_badge()

    # Show healing banner + detailed recovery timeline if the run succeeded after healing
    if st.session_state.heal_attempted and st.session_state.heal_cause:
        render_heal_banner()

        cause   = st.session_state.heal_cause
        summary = st.session_state.heal_summary

        cause_labels = {
            "expression_error":  "PySpark Expression Error — bare column used as Python variable",
            "null_values":       "Null Value in Column",
            "type_cast_error":   "Type Cast Failure",
            "schema_mismatch":   "Schema Mismatch",
            "cluster_oom":       "Cluster Out of Memory",
            "cluster_error":     "Cluster Error",
            "auth_error":        "Authentication Error",
            "dbfs_missing":      "DBFS Path Not Found",
            "workspace_error":   "Workspace Upload Error",
            "job_timeout":       "Job Timeout",
            "copy_failure":      "CSV Copy Stage Failure",
            "unknown":           "Unknown Error",
        }
        cause_label = cause_labels.get(cause, cause)

        fix_details = {
            "expression_error": "Wrapped all bare column references in <code>col(\"...\")</code> — PySpark requires column objects, not Python variable names.",
            "null_values":      "Added <code>coalesce()</code> null-safety wrapper to protect against null values at runtime.",
            "type_cast_error":  "Replaced unsafe cast with <code>coalesce(toDouble(...), 0)</code> to handle non-numeric values safely.",
            "schema_mismatch":  "Resolved column name mismatch using fuzzy matching against the actual CSV schema.",
            "cluster_oom":      "Reduced <code>shuffle_partitions</code> to lower memory pressure on the cluster.",
            "cluster_error":    "Flagged cluster for re-provisioning on retry.",
            "dbfs_missing":     "Reconstructed DBFS path references to match available storage.",
            "copy_failure":     "Adjusted CSV delimiter/encoding settings for the copy stage.",
        }
        fix_detail = fix_details.get(cause, "Applied automatic remediation based on detected root cause.")

        summary_html = "".join(f"<div style='margin-bottom:2px;'>• {l}</div>" for l in summary[:6]) if summary else ""

        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
            border: 1px solid #86efac;
            border-left: 4px solid #16a34a;
            border-radius: 10px;
            padding: 1.2rem 1.4rem;
            margin-bottom: 1.2rem;
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.73rem;
        ">
            <div style="font-size:0.82rem;font-weight:700;color:#14532d;margin-bottom:0.8rem;letter-spacing:0.01em;">
                ✅ PIPELINE RECOVERED BY SELF-HEALING AGENT
            </div>

            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:0.6rem;margin-bottom:0.9rem;">
                <div style="background:#fee2e2;border-radius:6px;padding:0.55rem 0.8rem;">
                    <div style="color:#7f1d1d;font-weight:600;font-size:0.65rem;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:3px;">① Initial Run</div>
                    <div style="color:#dc2626;font-weight:700;font-size:0.78rem;">FAILED</div>
                    <div style="color:#991b1b;font-size:0.66rem;margin-top:2px;">{cause_label}</div>
                </div>
                <div style="background:#fef9c3;border-radius:6px;padding:0.55rem 0.8rem;">
                    <div style="color:#713f12;font-weight:600;font-size:0.65rem;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:3px;">② Self-Healing</div>
                    <div style="color:#d97706;font-weight:700;font-size:0.78rem;">APPLIED</div>
                    <div style="color:#92400e;font-size:0.66rem;margin-top:2px;">Auto-fix deployed</div>
                </div>
                <div style="background:#dcfce7;border-radius:6px;padding:0.55rem 0.8rem;">
                    <div style="color:#14532d;font-weight:600;font-size:0.65rem;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:3px;">③ Retry Run</div>
                    <div style="color:#16a34a;font-weight:700;font-size:0.78rem;">SUCCEEDED</div>
                    <div style="color:#166534;font-size:0.66rem;margin-top:2px;">All pipelines passed</div>
                </div>
            </div>

            <div style="background:rgba(255,255,255,0.6);border-radius:6px;padding:0.6rem 0.9rem;margin-bottom:{'0.6rem' if summary_html else '0'};">
                <div style="color:#065f46;font-weight:600;font-size:0.67rem;text-transform:uppercase;letter-spacing:0.07em;margin-bottom:4px;">🔧 Fix Applied</div>
                <div style="color:#1e293b;line-height:1.6;">{fix_detail}</div>
            </div>

            {f'<div style="background:rgba(255,255,255,0.5);border-radius:6px;padding:0.6rem 0.9rem;color:#374151;line-height:1.7;">{summary_html}</div>' if summary_html else ''}
        </div>""", unsafe_allow_html=True)

    start   = st.session_state.pipeline_start_ts
    end     = st.session_state.pipeline_end_ts
    elapsed = _fmt_duration((end - start).total_seconds() if start and end else None)
    config  = st.session_state.pipeline_config

    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Status", "Succeeded")
    with c2: st.metric("Pipelines Run", str(len(config.get("execution_order", []))))
    with c3: st.metric("Total Duration", elapsed)

    st.markdown("<br>", unsafe_allow_html=True)

    if st.session_state.output_csv:
        st.download_button(
            label="Download Output CSV",
            data=st.session_state.output_csv,
            file_name=st.session_state.output_filename,
            mime="text/csv",
            use_container_width=True,
        )

    col_mon, col_new = st.columns([2, 1])
    with col_mon:
        if st.button("View Job Monitor", use_container_width=True):
            st.session_state.stage = "monitor"
            st.rerun()
    with col_new:
        if st.button("New Pipeline", use_container_width=True):
            for k, v in DEFAULTS.items():
                st.session_state[k] = v
            st.rerun()

    st.markdown("<hr class='sec-divider'>", unsafe_allow_html=True)
    st.markdown('<div class="card-label">Run Log</div>', unsafe_allow_html=True)
    render_logs()

    workspace_url = DATABRICKS_HOST.rstrip("/")
    st.markdown(f"""
    <div style="font-family:'IBM Plex Mono',monospace;font-size:0.72rem;color:#64748b;margin-top:1rem;">
        View jobs: <a href="{workspace_url}/#joblist" target="_blank" style="color:#ea580c;">{workspace_url}/#joblist</a>
    </div>""", unsafe_allow_html=True)


# ── Stage: FAILED ──────────────────────────────────────────────────────────────
def stage_failed():
    render_header()
    render_status_badge()

    # Show what the agent tried if it was invoked before the final failure
    if st.session_state.heal_attempted and st.session_state.heal_cause:
        render_heal_banner()
        st.markdown(
            '<div style="background:#fee2e2;border:1px solid #fca5a5;border-radius:8px;'
            'padding:0.7rem 1.1rem;margin-bottom:1rem;font-family:IBM Plex Mono,monospace;'
            'font-size:0.74rem;color:#7f1d1d;">⚠  Self-healing was attempted but the retry still failed.</div>',
            unsafe_allow_html=True,
        )

    st.error(f"Pipeline failed: {st.session_state.run_error}")

    col_retry, col_new = st.columns([1, 1])
    with col_retry:
        if st.button("Retry", use_container_width=True):
            st.session_state.stage                   = "plan"
            st.session_state.logs                    = []
            st.session_state.progress                = 0
            st.session_state.pipeline_thread_started = False
            st.session_state.heal_attempted          = False
            st.session_state.heal_cause              = None
            st.session_state.heal_summary            = []
            for k in ("pipeline_thread", "_result_q"):
                st.session_state.pop(k, None)
            st.rerun()
    with col_new:
        if st.button("Start Over", use_container_width=True):
            for k in ("pipeline_thread", "_result_q"):
                st.session_state.pop(k, None)
            for k, v in DEFAULTS.items():
                st.session_state[k] = v
            st.rerun()

    st.markdown("<hr class='sec-divider'>", unsafe_allow_html=True)
    st.markdown('<div class="card-label">Error Log</div>', unsafe_allow_html=True)
    render_logs()


# ── Stage: MONITOR ─────────────────────────────────────────────────────────────
def stage_monitor():
    render_header()

    col_title, col_back = st.columns([4, 1])
    with col_title:
        st.markdown('<div class="card-label">Databricks Pipeline Monitor</div>', unsafe_allow_html=True)
    with col_back:
        if st.button("← Back", use_container_width=True):
            st.session_state.stage       = "input" if st.session_state.monitor_only else "done"
            st.session_state.monitor_only = False
            st.rerun()

    ctrl1, ctrl2, ctrl3 = st.columns([2, 1, 1])
    with ctrl1:
        refresh = st.button("↻ Refresh", use_container_width=True)
    with ctrl2:
        limit = st.number_input("Max runs", min_value=1, value=20, label_visibility="collapsed")
    with ctrl3:
        auto_refresh = st.toggle("Auto-refresh 30s", value=False, key="mon_auto_refresh")

    if refresh or not st.session_state.live_runs:
        st.session_state.ar_counter   = 30
        st.session_state.inspected_run = None
        with st.spinner("Fetching data from Databricks..."):
            mq = queue.Queue()
            t  = threading.Thread(target=run_monitor_thread, args=(mq, limit), daemon=True)
            t.start()
            t.join(timeout=30)
            while not mq.empty():
                msg_type, payload = mq.get_nowait()
                if msg_type == "live_runs":    st.session_state.live_runs = payload
                elif msg_type == "clusters":   st.session_state.clusters = payload
                elif msg_type == "cluster_cache":     st.session_state.cluster_cache.update(payload)
                elif msg_type == "run_metrics_cache": st.session_state.run_metrics_cache.update(payload)
                elif msg_type == "monitor_log":       st.session_state.monitor_logs.append(payload)

    runs     = st.session_state.live_runs
    clusters = st.session_state.clusters

    tab_overview, tab_runs_t, tab_cluster, tab_analytics, tab_log = st.tabs([
        "Overview", "Job Runs", "Cluster Info", "Analytics", "Monitor Log"
    ])
    with tab_overview:  render_monitor_overview(runs, clusters)
    with tab_runs_t:    render_job_runs_tab(runs)
    with tab_cluster:   render_cluster_tab(clusters)
    with tab_analytics: render_analytics_tab(runs)
    with tab_log:       render_monitor_section()

    if auto_refresh:
        if st.session_state.ar_counter <= 0:
            st.session_state.ar_counter = 30
            st.session_state.live_runs  = []
            st.session_state.clusters   = []
            st.rerun()
        else:
            remaining = st.session_state.ar_counter
            st.caption(f"Auto-refreshing in {remaining}s  ·  toggle off to stop")
            st.session_state.ar_counter -= 1
            time.sleep(1)
            st.rerun()


# ── Router ─────────────────────────────────────────────────────────────────────
STAGE_FN = {
    "input":   stage_input,
    "plan":    stage_plan,
    "running": stage_running,
    "done":    stage_done,
    "failed":  stage_failed,
    "monitor": stage_monitor,
}

fn = STAGE_FN.get(st.session_state.stage, stage_input)
fn()