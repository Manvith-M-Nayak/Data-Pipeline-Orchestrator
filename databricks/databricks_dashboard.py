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
.badge-ok      { background: #dcfce7; color: #16a34a; border: 1px solid #86efac; }
.badge-fail    { background: #fee2e2; color: #dc2626; border: 1px solid #fca5a5; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }

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
    "output_csv":         None,
    "output_filename":    "output.csv",
    "run_error":          None,
    "progress":           0,
    "pipeline_start_ts":  None,
    "pipeline_end_ts":    None,
    "stage_paths":        {},
    "used_fallback":      False,
    "monitor_only":       False,
    # Thread control — True once a thread has been started for this pipeline run.
    # Prevents Streamlit rerenders from spawning duplicate threads after the
    # first thread finishes but before its terminal message is processed.
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


# ── Pipeline execution thread ──────────────────────────────────────────────────
def run_pipeline_thread(csv_path: str, pipeline_config: dict, schema: dict, result_q: queue.Queue):
    class Tee(io.TextIOBase):
        def write(self, s):
            s = s.rstrip("\n")
            if s.strip():
                result_q.put(("log", s))
            return len(s)
        def flush(self): pass

    from contextlib import redirect_stdout
    tee = Tee()
    try:
        with redirect_stdout(tee):
            from databricks_api import execute_pipeline

            def prog(pct):
                result_q.put(("progress", pct))

            result = execute_pipeline(
                csv_path=csv_path,
                pipeline_config=pipeline_config,
                schema=schema,
                log_fn=lambda msg: result_q.put(("log", msg)),
                progress_fn=prog,
            )

        if result["status"] == "ok":
            result_q.put(("ok", result))
        else:
            result_q.put(("error", result.get("message", "Unknown error")))

    except Exception as e:
        result_q.put(("error", str(e)))


# ── Monitor thread ─────────────────────────────────────────────────────────────
def run_monitor_thread(result_q: queue.Queue, limit: int = 20):
    try:
        from databricks_api import list_recent_runs
        result_q.put(("monitor_log", "Fetching Databricks job runs..."))
        runs = list_recent_runs(limit=limit)
        result_q.put(("monitor_log", f"Found {len(runs)} recent run(s)"))
        result_q.put(("live_runs", runs))
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
        "done":    ("Success", "badge-ok"),
        "failed":  ("Failed",  "badge-fail"),
    }
    label, cls = badge_map.get(st.session_state.stage, ("Idle", "badge-idle"))
    dot = {
        "badge-idle": "#4f6a8a", "badge-running": "#ea580c",
        "badge-ok": "#34d399",   "badge-fail": "#f87171",
    }[cls]
    st.markdown(
        f'<span class="badge {cls}">'
        f'<span style="width:6px;height:6px;border-radius:50%;background:{dot};'
        f'display:inline-block;"></span>{label}</span><br><br>',
        unsafe_allow_html=True,
    )


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
        if any(x in l for x in ["failed", "error", "abort", "invalid", "exception"]):
            return f'<span class="log-error">{line}</span>'
        if any(x in l for x in ["succeeded", "created", "uploaded", "complete", "ready", "done"]):
            return f'<span class="log-success">{line}</span>'
        if any(x in l for x in ["waiting", "retrying", "timeout", "warn", "purging", "polling"]):
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

    # Summary counts
    total = len(runs)
    succeeded = sum(1 for r in runs if r["status"] == "Succeeded")
    failed = sum(1 for r in runs if r["status"] == "Failed")
    running = sum(1 for r in runs if r["status"] == "InProgress")

    ok_cls = "mon-card-ok" if failed == 0 else "mon-card-err"
    rate = int(100 * succeeded / total) if total else 0
    rate_cls = "mon-card-ok" if rate >= 80 else "mon-card-warn"

    st.markdown(f"""
    <div class="mon-grid">
      <div class="mon-card mon-card-blue">
        <div class="mon-card-label">Total Runs</div>
        <div class="mon-card-value">{total}</div>
        <div class="mon-card-sub">recent history</div>
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

    for run in runs:
        err_html = ""
        if run.get("message") and run["status"] == "Failed":
            msg = str(run["message"])[:200]
            err_html = f'<div class="run-row-err">{msg}</div>'

        st.markdown(f"""
        <div class="run-row">
            <div class="run-row-header">
                <span class="run-row-name">{run['pipeline']}</span>
                {_status_pill(run['status'])}
                <span class="run-row-meta">run: {str(run.get('run_id',''))}</span>
                <span class="run-row-meta">job: {str(run.get('job_id',''))}</span>
                <span class="run-row-meta">⏱ {run.get('duration','N/A')}</span>
                <span class="run-row-meta">started: {str(run.get('started',''))[:19]}</span>
            </div>
            {err_html}
        </div>""", unsafe_allow_html=True)


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
        container_names_raw = st.text_input(
            "Container names", placeholder="incoming,bronze,silver",
            label_visibility="collapsed",
        )
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

        # Save CSV to temp file
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
            schema=schema,
            user_prompt=prompt,
            num_containers=num_stages,
            custom_settings=custom if custom else None,
            container_names=container_names,
        )

        st.session_state.pipeline_config = config
        st.session_state.used_fallback = used_fallback
        st.session_state.stage = "plan"
        st.session_state.logs = []
        st.session_state.progress = 0
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
            st.session_state.stage = "running"
            st.session_state.logs = []
            st.session_state.progress = 0
            st.session_state.pipeline_thread_started = False
            st.session_state.pipeline_start_ts = datetime.now(timezone.utc)
            st.rerun()

    with col_back:
        if st.button("Back", use_container_width=True):
            st.session_state.stage = "input"
            st.rerun()


# ── Stage: RUNNING ─────────────────────────────────────────────────────────────
def stage_running():
    render_header()
    render_status_badge()

    # Start thread exactly once per pipeline run.
    # Using a "started" flag instead of is_alive() prevents the common Streamlit
    # race condition where the thread finishes between two rerenders and a second
    # thread gets spawned before the terminal message is drained from the queue.
    if not st.session_state.pipeline_thread_started:
        # Validate workspace connectivity before spawning the thread.
        # Fails fast with a user-readable message if DATABRICKS_HOST / TOKEN wrong.
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
        st.session_state._result_q = result_q
        st.session_state.pipeline_thread_started = True
    else:
        result_q = st.session_state._result_q

    # Drain queue
    terminal = None
    while not result_q.empty():
        msg_type, payload = result_q.get_nowait()
        if msg_type == "log":
            st.session_state.logs.append(payload)
        elif msg_type == "progress":
            st.session_state.progress = payload
        elif msg_type == "ok":
            terminal = ("ok", payload)
        elif msg_type == "error":
            terminal = ("error", payload)

    if terminal:
        kind, data = terminal
        if kind == "ok":
            st.session_state.stage = "done"
            st.session_state.stage_paths = data.get("stage_paths", {})
            st.session_state.pipeline_end_ts = datetime.now(timezone.utc)
            # Attempt to fetch output
            try:
                # Prefer output embedded directly in result (Standard plan / no-DBFS path)
                if data.get("output_csv_bytes"):
                    st.session_state.output_csv = data["output_csv_bytes"]
                    st.session_state.output_filename = data.get("output_csv_name", "output.csv")
                elif data.get("stage_paths"):
                    # Legacy DBFS path (Premium plan with DBFS enabled)
                    last_container = list(st.session_state.pipeline_config["containers"].values())[-1]
                    sink_dbfs = data["stage_paths"].get(last_container, "")
                    if sink_dbfs:
                        from databricks_api import fetch_output_from_dbfs, dbfs_list
                        entries = dbfs_list(sink_dbfs)
                        for entry in entries:
                            if "output.csv" in entry.get("path", "") or "staged.csv" in entry.get("path", ""):
                                out_data, out_name = fetch_output_from_dbfs(entry["path"])
                                if out_data:
                                    st.session_state.output_csv = out_data
                                    st.session_state.output_filename = out_name or "output.csv"
                                    break
            except Exception as e:
                st.session_state.logs.append(f"Output fetch: {e}")
        else:
            st.session_state.run_error = data
            st.session_state.stage = "failed"
        st.rerun()

    st.progress(min(st.session_state.progress / 100, 1.0))
    st.markdown('<div class="card-label">Pipeline Log</div>', unsafe_allow_html=True)
    render_logs()

    if st.session_state.pipeline_thread.is_alive():
        time.sleep(2)
        st.rerun()


# ── Stage: DONE ────────────────────────────────────────────────────────────────
def stage_done():
    render_header()
    render_status_badge()

    start = st.session_state.pipeline_start_ts
    end = st.session_state.pipeline_end_ts
    elapsed = _fmt_duration((end - start).total_seconds() if start and end else None)

    config = st.session_state.pipeline_config
    n_pipelines = len(config.get("execution_order", []))

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Status", "Succeeded")
    with c2:
        st.metric("Pipelines Run", str(n_pipelines))
    with c3:
        st.metric("Total Duration", elapsed)

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

    st.error(f"Pipeline failed: {st.session_state.run_error}")

    col_retry, col_new = st.columns([1, 1])
    with col_retry:
        if st.button("Retry", use_container_width=True):
            st.session_state.stage = "plan"
            st.session_state.logs = []
            st.session_state.progress = 0
            st.session_state.pipeline_thread_started = False
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
        st.markdown('<div class="card-label">Databricks Job Monitor</div>', unsafe_allow_html=True)
    with col_back:
        if st.button("Back to Pipeline", use_container_width=True):
            st.session_state.stage = "input" if st.session_state.monitor_only else "done"
            st.session_state.monitor_only = False
            st.rerun()

    # Tabs: Live runs | Log
    tab_runs, tab_log = st.tabs(["Job Runs", "Monitor Log"])

    with tab_runs:
        col_refresh, col_limit = st.columns([2, 1])
        with col_refresh:
            refresh = st.button("Refresh", use_container_width=True)
        with col_limit:
            limit = st.number_input("Max runs", min_value=5, max_value=100, value=20)

        if refresh or not st.session_state.live_runs:
            mq = queue.Queue()
            t = threading.Thread(target=run_monitor_thread, args=(mq, limit), daemon=True)
            t.start()
            t.join(timeout=30)
            while not mq.empty():
                msg_type, payload = mq.get_nowait()
                if msg_type == "live_runs":
                    st.session_state.live_runs = payload
                elif msg_type == "monitor_log":
                    st.session_state.monitor_logs.append(payload)

        render_live_runs(st.session_state.live_runs)

    with tab_log:
        render_monitor_section()


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
