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

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from config import DATABRICKS_HOST, AZURE_DATA_FACTORY

st.set_page_config(
    page_title="Pipeline Orchestrator",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="collapsed",
)

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
[data-testid="stSelectbox"] li[aria-selected="true"] { background-color: #7c3aed !important; }
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
[data-testid="stTextInput"] input:focus { border-color: #7c3aed !important; outline: none !important; }

.stApp > * { animation: none !important; transition: none !important; }
[data-testid="stMetric"], [data-testid="stMetric"] * { opacity: 1 !important; }

.block-container { padding: 2rem 3rem 4rem !important; max-width: 1280px; }

.app-header {
    padding: 1.2rem 0 1rem; border-bottom: 1px solid #e2e8f0; margin-bottom: 1.8rem;
}
.app-title { font-size: 1.2rem; font-weight: 600; color: #0f172a; margin: 0; }

.badge {
    display: inline-flex; align-items: center; gap: 0.4rem;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.65rem;
    font-weight: 600; letter-spacing: 0.1em; text-transform: uppercase;
    padding: 0.22rem 0.7rem; border-radius: 999px;
}
.badge-idle    { background: #f1f5f9; color: #64748b; border: 1px solid #cbd5e1; }
.badge-running { background: #f5f3ff; color: #7c3aed; border: 1px solid #c4b5fd; animation: pulse 1.8s infinite; }
.badge-ok      { background: #dcfce7; color: #16a34a; border: 1px solid #86efac; }
.badge-fail    { background: #fee2e2; color: #dc2626; border: 1px solid #fca5a5; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }

.card { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 14px; padding: 1.5rem 1.7rem; margin-bottom: 1.1rem; box-shadow: 0 2px 12px rgba(0,0,0,0.05); }
.card-label { font-family: 'IBM Plex Mono', monospace; font-size: 0.62rem; font-weight: 600; letter-spacing: 0.18em; text-transform: uppercase; color: #7c3aed; margin-bottom: 1rem; }

.plan-table { width: 100%; border-collapse: collapse; font-family: 'IBM Plex Mono', monospace; font-size: 0.76rem; }
.plan-table th { text-align: left; padding: 0.5rem 0.9rem; border-bottom: 1px solid #e2e8f0; font-size: 0.62rem; letter-spacing: 0.12em; text-transform: uppercase; color: #64748b; font-weight: 600; }
.plan-table td { padding: 0.5rem 0.9rem; border-bottom: 1px solid #f1f5f9; color: #475569; }
.plan-table tr:last-child td { border-bottom: none; }
.plan-table tr:hover td { background: #f8fafc; }

.pill { display: inline-block; padding: 0.15rem 0.55rem; border-radius: 5px; font-size: 0.66rem; font-weight: 500; }
.pill-copy      { background: #dcfce7; color: #16a34a; }
.pill-notebook  { background: #f5f3ff; color: #7c3aed; }
.pill-transform { background: #f5f3ff; color: #7c3aed; }
.pill-integer   { background: #e0e7ff; color: #6366f1; }
.pill-double    { background: #dcfce7; color: #16a34a; }
.pill-string    { background: #fef3c7; color: #d97706; }
.pill-succeeded { background: #dcfce7; color: #16a34a; }
.pill-failed    { background: #fee2e2; color: #dc2626; }
.pill-inprogress, .pill-running { background: #f5f3ff; color: #7c3aed; animation: pulse 1.8s infinite; }
.pill-queued    { background: #fef3c7; color: #d97706; }
.pill-unknown   { background: #f1f5f9; color: #64748b; }

.log-box {
    background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px;
    padding: 1.1rem 1.3rem; font-family: 'IBM Plex Mono', monospace;
    font-size: 0.74rem; line-height: 1.9; min-height: 320px; max-height: 480px;
    overflow-y: auto; white-space: pre-wrap; word-break: break-word; color: #334155;
}
.log-box .log-error   { color: #dc2626; }
.log-box .log-success { color: #16a34a; }
.log-box .log-warn    { color: #d97706; }
.log-box .log-info    { color: #7c3aed; }
.log-box .log-default { color: #64748b; }

.mon-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.9rem; margin-bottom: 1.2rem; }
.mon-card { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 1rem 1.2rem; }
.mon-card-label { font-family: 'IBM Plex Mono', monospace; font-size: 0.58rem; letter-spacing: 0.14em; text-transform: uppercase; color: #64748b; margin-bottom: 0.4rem; }
.mon-card-value { font-family: 'IBM Plex Mono', monospace; font-size: 1.35rem; font-weight: 700; color: #0f172a; }
.mon-card-sub { font-size: 0.65rem; color: #64748b; margin-top: 0.2rem; }
.mon-card-ok   { border-color: #86efac; }
.mon-card-warn { border-color: #fcd34d; }
.mon-card-err  { border-color: #fca5a5; }
.mon-card-blue { border-color: #93c5fd; }
.mon-card-purple { border-color: #c4b5fd; }

[data-testid="stFileUploaderDropzone"] {
    background: #f8fafc !important; border: 2px dashed #cbd5e1 !important; border-radius: 10px !important;
}
[data-testid="stFileUploaderDropzone"]:hover { border-color: #7c3aed !important; background: #f5f3ff !important; }
[data-testid="stFileUploaderDropzone"] button {
    background: #f5f3ff !important; color: #7c3aed !important; border: 1px solid #c4b5fd !important;
    border-radius: 6px !important; font-weight: 600 !important; font-size: 0.82rem !important;
    padding: 0.35rem 1rem !important;
}
textarea { background: #ffffff !important; border: 1px solid #e2e8f0 !important; border-radius: 8px !important; color: #1e293b !important; font-size: 0.88rem !important; }
textarea:focus { border-color: #7c3aed !important; outline: none !important; }

[data-testid="stButton"] > button {
    background: linear-gradient(135deg, #7c3aed, #0ea5e9) !important;
    color: #fff !important; border: none !important; border-radius: 8px !important;
    font-weight: 600 !important; font-size: 0.88rem !important; padding: 0.6rem 1.4rem !important;
    width: 100% !important; box-shadow: 0 2px 8px rgba(124,58,237,0.3) !important;
}
[data-testid="stButton"] > button:hover { opacity: 0.88 !important; transform: translateY(-1px) !important; }
[data-testid="stButton"] > button:disabled { background: #e2e8f0 !important; color: #94a3b8 !important; box-shadow: none !important; }

[data-testid="stDownloadButton"] > button {
    background: #dcfce7 !important; color: #16a34a !important; border: 1px solid #86efac !important;
    border-radius: 8px !important; font-weight: 600 !important; padding: 0.6rem 1.4rem !important; width: 100% !important;
}
[data-testid="stProgress"] > div > div { background: linear-gradient(90deg, #7c3aed, #0ea5e9) !important; border-radius: 999px !important; }
[data-testid="stProgress"] > div { background: #e2e8f0 !important; border-radius: 999px !important; }
[data-testid="stMetric"] { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 1rem 1.2rem; }
label[data-testid="stMetricLabel"] p { color: #64748b !important; font-size: 0.7rem !important; text-transform: uppercase; }
[data-testid="stMetricValue"] { color: #0f172a !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 1.1rem !important; }
[data-testid="stTabs"] [role="tab"] { font-family: 'IBM Plex Mono', monospace !important; font-size: 0.72rem !important; text-transform: uppercase !important; color: #64748b !important; }
[data-testid="stTabs"] [role="tab"][aria-selected="true"] { color: #7c3aed !important; border-bottom-color: #7c3aed !important; }
[data-testid="stExpander"] { background: #ffffff !important; border: 1px solid #e2e8f0 !important; border-radius: 10px !important; }
.sec-divider { border: none; border-top: 1px solid #e2e8f0; margin: 1.2rem 0; }
</style>
""", unsafe_allow_html=True)

# ── Session state ──────────────────────────────────────────────────────────────
DEFAULTS = {
    "stage":             "input",
    "pipeline_config":   None,
    "user_prompt":       "",
    "schema":            None,
    "csv_tmp_path":      None,
    "logs":              [],
    "run_error":         None,
    "progress":          0,
    "pipeline_start_ts": None,
    "pipeline_end_ts":   None,
    "used_fallback":     False,
    "run_result":        None,
    "output_csv":        None,
    "output_filename":   "output.csv",
    "pipeline_thread_started": False,
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ── CSV helpers ────────────────────────────────────────────────────────────────
def read_csv_schema(filepath: str, sample_rows: int = 5) -> dict:
    import csv
    size = os.path.getsize(filepath)
    size_hint = (
        "small (< 5MB)"   if size < 5_242_880  else
        "medium (5–50MB)" if size < 52_428_800 else
        "large (50–200MB)" if size < 209_715_200 else
        "xlarge (> 200MB)"
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


def fetch_output_from_blob(container_name: str) -> tuple:
    try:
        from azure.storage.blob import BlobServiceClient
        from config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
        import csv as _csv
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={AZURE_STORAGE_ACCOUNT};"
            f"AccountKey={AZURE_STORAGE_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        client    = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(container_name)
        blobs     = [b for b in container.list_blobs() if b.size > 0 and not b.name.startswith("*")]
        if not blobs:
            return None, ""

        # Prefer merged/staged CSVs; fall back to largest blob
        csv_blobs = [b for b in blobs if b.name.endswith(".csv")]
        part_blobs = sorted([b for b in csv_blobs if "part-" in b.name], key=lambda b: b.name)

        if part_blobs:
            merged, header = [], None
            for pb in part_blobs:
                content = container.download_blob(pb.name).readall().decode("utf-8")
                rows = list(_csv.reader(io.StringIO(content)))
                if rows:
                    if header is None:
                        header = rows[0]; merged.append(header)
                    merged.extend(rows[1:])
            if merged:
                out = io.StringIO()
                _csv.writer(out).writerows(merged)
                return out.getvalue().encode("utf-8"), "output.csv"

        target = max(csv_blobs or blobs, key=lambda b: b.size)
        data   = container.download_blob(target.name).readall()
        return data, target.name
    except Exception:
        return None, ""


def _elapsed() -> str:
    if st.session_state.pipeline_start_ts is None:
        return "0s"
    end = st.session_state.pipeline_end_ts or time.time()
    return _fmt_duration(end - st.session_state.pipeline_start_ts)


# ── Pipeline execution thread ──────────────────────────────────────────────────
def run_pipeline_thread(csv_path: str, pipeline_config: dict, schema: dict, result_q: queue.Queue):
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
            from executor_agent.executor import execute_pipeline

            # Emit progress milestones based on known steps A–K
            step_progress = {
                "Step A": 5,  "Step B": 15, "Step C": 25,
                "Step D": 35, "Step E": 50, "Step F": 60,
                "Step G": 68, "Step H": 75, "Step I": 82,
                "Step J": 90, "Step K": 95,
            }

            class ProgressTee(io.TextIOBase):
                def write(self, s):
                    s_stripped = s.rstrip("\n")
                    if s_stripped.strip():
                        result_q.put(("log", s_stripped))
                        for step, pct in step_progress.items():
                            if step in s_stripped:
                                result_q.put(("progress", pct))
                                break
                    return len(s)
                def flush(self): pass

            ptee = ProgressTee()
            with contextlib.redirect_stdout(ptee):
                result = execute_pipeline(csv_path, pipeline_config, schema)

        if result["status"] == "ok":
            result_q.put(("progress", 100))
            result_q.put(("ok", result))
        else:
            result_q.put(("error", result.get("message", "Pipeline failed")))

    except Exception as e:
        result_q.put(("error", str(e)))


# ── Render helpers ─────────────────────────────────────────────────────────────
def render_header():
    st.markdown("""
    <div class="app-header">
        <div class="app-title">Pipeline Orchestrator</div>
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
        "badge-idle":    "#4f6a8a",
        "badge-running": "#7c3aed",
        "badge-ok":      "#34d399",
        "badge-fail":    "#f87171",
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
        <div style="background:#fef3c7;border:1px solid #f59e0b;border-radius:8px;padding:12px;margin-bottom:16px;">
            <strong>⚠ Using Default Configuration</strong> — AI planner unavailable. Running with default pipeline.
        </div>""", unsafe_allow_html=True)

    # Container metrics
    containers = config.get("containers_to_create", list(config.get("containers", {}).values()))
    if isinstance(containers, dict):
        containers = list(containers.values())
    cols = st.columns(len(containers))
    for i, name in enumerate(containers):
        with cols[i]:
            label = ["Raw", "Bronze", "Silver", "Gold", "Platinum"][i] if i < 5 else f"Stage {i}"
            st.metric(label, name)
    st.markdown("<br>", unsafe_allow_html=True)

    # Stages table
    stages = config.get("stages", [])
    rows = ""
    for s in stages:
        stype = s.get("type", "")
        pill  = f"<span class='pill pill-{stype}'>{stype}</span>"
        src   = s.get("source_dataset", s.get("source_container", "—"))
        snk   = s.get("sink_dataset",   s.get("sink_container",   "—"))
        extra = ""
        if stype == "copy":
            extra = f"<td>DIU: {s.get('diu', '—')}</td><td>—</td>"
        else:
            extra = f"<td>{s.get('num_workers', '—')} workers</td><td>{s.get('shuffle_partitions', '—')} parts</td>"
        rows += f"<tr><td>{s['name']}</td><td>{pill}</td><td>{src}</td><td>{snk}</td>{extra}</tr>"

    st.markdown(f"""
    <div class="card-label">Pipeline Stages</div>
    <table class="plan-table">
      <thead><tr><th>Name</th><th>Type</th><th>Source</th><th>Sink</th><th>Compute</th><th>Parallelism</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>""", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # Notebook transformations
    nb_stages = [s for s in stages if s.get("type") == "notebook" and s.get("transformations")]
    if nb_stages:
        for s in nb_stages:
            transforms = s.get("transformations", [])
            filter_cond = s.get("filter_condition")
            agg = s.get("aggregation")
            t_rows = "".join(f"<tr><td style='color:#475569'>{t}</td></tr>" for t in transforms)
            f_row  = f"<tr><td style='color:#d97706'>filter: {filter_cond}</td></tr>" if filter_cond else ""
            a_rows = ""
            if agg:
                gb = ", ".join(agg.get("group_by", []))
                a_rows += f"<tr><td style='color:#7c3aed'>groupBy: {gb}</td></tr>"
                for a in agg.get("aggregations", []):
                    a_rows += (f"<tr><td style='color:#7c3aed'>"
                               f"{a.get('alias')} = {a.get('op')}({a.get('column')})</td></tr>")
            st.markdown(f"""
            <div style="font-family:'IBM Plex Mono',monospace;font-size:0.62rem;font-weight:600;
                        letter-spacing:0.18em;text-transform:uppercase;color:#0ea5e9;margin-bottom:0.4rem;">
                {s['name']} — Transformations</div>
            <table class="plan-table" style="margin-bottom:0.8rem;">
              <tbody>{t_rows}{f_row}{a_rows}</tbody>
            </table>""", unsafe_allow_html=True)

    # Recommended settings
    rec = config.get("recommended_settings", {})
    if rec:
        st.markdown(f"""
        <div class="card-label">Recommended Settings</div>
        <table class="plan-table">
          <thead><tr><th>Setting</th><th>Value</th></tr></thead>
          <tbody>
            <tr><td>Node Type</td><td>{rec.get('node_type', 'N/A')}</td></tr>
            <tr><td>DIU (Copy Activity)</td><td>{rec.get('diu', 'N/A')}</td></tr>
            <tr><td>Notebook Workers</td><td>{rec.get('num_workers', 'N/A')}</td></tr>
            <tr><td>Shuffle Partitions</td><td>{rec.get('shuffle_partitions', 'N/A')}</td></tr>
          </tbody>
        </table>""", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    # CSV schema
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

    exec_order = config.get("execution_order", [s["name"] for s in stages])
    st.markdown(f"""
    <br>
    <div style="font-family:'IBM Plex Mono',monospace;font-size:0.74rem;color:#64748b;line-height:1.9;">
        <b style="color:#475569;">Execution order:</b> {' → '.join(exec_order)}<br>
        <b style="color:#475569;">Reasoning:</b> {config.get('reasoning', 'N/A')}
    </div>""", unsafe_allow_html=True)


def render_logs():
    def colour(line: str) -> str:
        l = line.lower()
        if any(x in l for x in ["failed", "error", "abort", "invalid", "exception"]):
            return f'<span class="log-error">{line}</span>'
        if any(x in l for x in ["succeeded", "created", "uploaded", "complete", "ready", "done", "obtained"]):
            return f'<span class="log-success">{line}</span>'
        if any(x in l for x in ["waiting", "propagat", "timeout", "warn", "purging", "polling", "retrying"]):
            return f'<span class="log-warn">{line}</span>'
        if any(x in l for x in ["step", "---", "databricks", "notebook", "adf", "triggering",
                                  "creating", "building", "publishing", "authenticat"]):
            return f'<span class="log-info">{line}</span>'
        return f'<span class="log-default">{line}</span>'

    lines = st.session_state.logs
    content = (
        '<span class="log-default">Waiting for pipeline to start…</span>'
        if not lines
        else "\n".join(colour(l) for l in lines)
    )
    st.markdown(f'<div class="log-box">{content}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
# MAIN UI
# ══════════════════════════════════════════════════════════════════════

render_header()
render_status_badge()

pipeline_in_progress = (
    st.session_state.stage in ["plan", "input"] and
    (st.session_state.get("pipeline_start_ts") is not None or
     st.session_state.get("progress", 0) > 0 or
     "_q" in st.session_state)
)
if pipeline_in_progress and st.session_state.stage != "running":
    st.session_state.stage = "running"
    st.rerun()


# ════════════════════════════════════════════════════════════════
# STAGE: input
# ════════════════════════════════════════════════════════════════
if st.session_state.stage == "input":

    left, right = st.columns([1.1, 1], gap="large")

    with left:
        st.markdown('<div class="card"><div class="card-label">Upload CSV</div>', unsafe_allow_html=True)
        uploaded = st.file_uploader("upload_csv", type=["csv"], label_visibility="collapsed")
        if uploaded:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            tmp.write(uploaded.read())
            tmp.flush(); tmp.close()
            st.session_state.csv_tmp_path = tmp.name
            schema = read_csv_schema(tmp.name)
            st.session_state.schema = schema
            st.markdown(
                f'<p style="font-family:IBM Plex Mono,monospace;font-size:0.74rem;'
                f'color:#64748b;margin-top:0.5rem;">'
                f'{len(schema["columns"])} columns &nbsp;·&nbsp;'
                f'~{schema["row_count"]:,} rows &nbsp;·&nbsp; {schema["size_hint"]}'
                f'</p>',
                unsafe_allow_html=True,
            )
        st.markdown('</div>', unsafe_allow_html=True)

    with right:
        st.markdown('<div class="card"><div class="card-label">Pipeline Prompt</div>', unsafe_allow_html=True)

        if "prompt_text" not in st.session_state:
            st.session_state.prompt_text = ""

        st.text_area(
            "pipeline_prompt", height=180,
            label_visibility="collapsed",
            key="prompt_text",
            placeholder=(
                "e.g. Clean nulls, uppercase the name column, "
                "filter rows where status = 1, and load to silver."
            ),
        )
        st.markdown("<br>", unsafe_allow_html=True)

        if st.button("Generate Plan", use_container_width=True):
            prompt_val = st.session_state.get("prompt_text", "").strip()
            csv_path   = st.session_state.get("csv_tmp_path")

            if not csv_path:
                st.error("Upload a CSV first.")
            elif not prompt_val:
                st.error("Enter a pipeline prompt.")
            elif not st.session_state.get("schema"):
                st.error("Schema not loaded.")
            else:
                with st.spinner("AI designing unified pipeline..."):
                    try:
                        from planner_agent.groq_planner import decide_pipeline_config
                        config, used_fallback = decide_pipeline_config(
                            st.session_state.schema, prompt_val
                        )
                        st.session_state.pipeline_config = config
                        st.session_state.used_fallback   = used_fallback
                        st.session_state.user_prompt     = prompt_val
                        st.session_state.stage           = "plan"
                        st.session_state.logs            = []
                        st.rerun()
                    except Exception as e:
                        st.error(f"Plan generation failed: {e}")

        st.markdown('</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════
# STAGE: plan
# ════════════════════════════════════════════════════════════════
elif st.session_state.stage == "plan":
    used_fallback = st.session_state.get("used_fallback", False)
    config  = st.session_state.pipeline_config
    schema  = st.session_state.schema
    rec     = config.get("recommended_settings", {})
    editable = config.get("editable_settings", {})

    st.markdown('<div class="card"><div class="card-label">Pipeline Plan</div>', unsafe_allow_html=True)
    render_plan(config, schema, used_fallback)
    st.markdown('</div>', unsafe_allow_html=True)

    # Settings panel
    st.markdown('<div class="card" style="background-color:#f8fafc;">', unsafe_allow_html=True)
    st.markdown('<div style="color:#1e293b;font-weight:600;font-size:1rem;margin-bottom:0.5rem;">⚙ Pipeline Configuration</div>', unsafe_allow_html=True)
    st.markdown(f'<div style="color:#64748b;font-size:0.85rem;margin-bottom:1rem;">Recommended for {schema.get("size_hint","medium")} data. Adjust as needed.</div>', unsafe_allow_html=True)

    if "edit_num_stages" not in st.session_state:
        st.session_state.edit_num_stages = config.get("num_containers", 3)
    if "edit_diu" not in st.session_state:
        st.session_state.edit_diu = rec.get("diu", 2)
    if "edit_num_workers" not in st.session_state:
        st.session_state.edit_num_workers = rec.get("num_workers", 0)
    if "edit_shuffle_partitions" not in st.session_state:
        st.session_state.edit_shuffle_partitions = rec.get("shuffle_partitions", 8)
    if "edit_node_type" not in st.session_state:
        st.session_state.edit_node_type = rec.get("node_type", "Standard_D4s_v3")
    if "edit_container_names" not in st.session_state:
        containers = config.get("containers_to_create", list(config.get("containers", {}).values()))
        if isinstance(containers, dict):
            containers = list(containers.values())
        st.session_state.edit_container_names = ", ".join(containers)

    diu_options             = editable.get("diu", [1, 2, 4, 8, 16, 32])
    num_workers_options     = editable.get("num_workers", [0, 2, 4, 8, 16])
    shuffle_parts_options   = editable.get("shuffle_partitions", [4, 8, 16, 32, 64])
    node_type_options       = editable.get("node_type", ["Standard_D4s_v3", "Standard_DS4_v2", "Standard_D8s_v3"])

    st.markdown('<div style="color:#1e293b;font-weight:500;font-size:0.9rem;margin-top:0.5rem;">Number of Stages</div>', unsafe_allow_html=True)
    new_num_stages = st.number_input(
        "stages", min_value=2, max_value=5,
        value=st.session_state.edit_num_stages,
        label_visibility="collapsed",
        help="2–5 stages. First is always a Copy (ingest); rest are Databricks notebooks."
    )
    st.session_state.edit_num_stages = new_num_stages

    col1, col2 = st.columns(2)

    def _safe_idx(opts, val):
        try: return opts.index(val)
        except ValueError: return 0

    with col1:
        st.markdown('<div style="color:#1e293b;font-size:0.85rem;margin-bottom:0.3rem;margin-top:0.8rem;">DIU (Copy Activity)</div>', unsafe_allow_html=True)
        new_diu = st.selectbox("diu", diu_options, index=_safe_idx(diu_options, st.session_state.edit_diu), label_visibility="collapsed")
        st.session_state.edit_diu = new_diu

        st.markdown('<div style="color:#1e293b;font-size:0.85rem;margin-bottom:0.3rem;margin-top:0.5rem;">Notebook Workers</div>', unsafe_allow_html=True)
        new_workers = st.selectbox("num_workers", num_workers_options, index=_safe_idx(num_workers_options, st.session_state.edit_num_workers), label_visibility="collapsed", help="0 = single-node driver only")
        st.session_state.edit_num_workers = new_workers

    with col2:
        st.markdown('<div style="color:#1e293b;font-size:0.85rem;margin-bottom:0.3rem;margin-top:0.8rem;">Shuffle Partitions</div>', unsafe_allow_html=True)
        new_shuffle = st.selectbox("shuffle_partitions", shuffle_parts_options, index=_safe_idx(shuffle_parts_options, st.session_state.edit_shuffle_partitions), label_visibility="collapsed")
        st.session_state.edit_shuffle_partitions = new_shuffle

        st.markdown('<div style="color:#1e293b;font-size:0.85rem;margin-bottom:0.3rem;margin-top:0.5rem;">Node Type</div>', unsafe_allow_html=True)
        new_node = st.selectbox("node_type", node_type_options, index=_safe_idx(node_type_options, st.session_state.edit_node_type), label_visibility="collapsed")
        st.session_state.edit_node_type = new_node

    st.markdown("---")
    new_containers = st.text_input(
        "Container names (comma-separated)",
        value=st.session_state.edit_container_names,
        help="Leave empty to use default names. Must match number of stages.",
    )
    st.session_state.edit_container_names = new_containers

    if st.button("Apply Settings", type="primary", use_container_width=True):
        from planner_agent.groq_planner import decide_pipeline_config
        container_list = None
        if new_containers.strip():
            container_list = [c.strip() for c in new_containers.split(",")]
            if len(container_list) != new_num_stages:
                st.warning(f"Container count ({len(container_list)}) must match stages ({new_num_stages}). Ignoring names.")
                container_list = None

        custom_settings = {
            "diu": new_diu,
            "num_workers": new_workers,
            "shuffle_partitions": new_shuffle,
            "node_type": new_node,
        }
        with st.spinner("Regenerating plan..."):
            try:
                new_config, _ = decide_pipeline_config(
                    schema,
                    st.session_state.get("user_prompt", ""),
                    num_containers=new_num_stages,
                    custom_settings=custom_settings,
                    container_names=container_list,
                )
                st.session_state.pipeline_config = new_config
                containers_out = new_config.get("containers_to_create",
                                                list(new_config.get("containers", {}).values()))
                if isinstance(containers_out, dict):
                    containers_out = list(containers_out.values())
                st.session_state.edit_container_names = ", ".join(containers_out)
                st.success("Settings applied.")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    col_back, col_deploy = st.columns(2)
    with col_back:
        if st.button("← Back", use_container_width=True):
            for key in ["edit_num_stages", "edit_diu", "edit_num_workers",
                        "edit_shuffle_partitions", "edit_node_type", "edit_container_names"]:
                st.session_state.pop(key, None)
            st.session_state.stage = "input"
            st.rerun()
    with col_deploy:
        if st.button("Deploy Pipeline", use_container_width=True):
            st.session_state.stage            = "running"
            st.session_state.logs             = []
            st.session_state.progress         = 0
            st.session_state.pipeline_start_ts = time.time()
            st.session_state.pipeline_end_ts  = None
            st.session_state.run_result       = None
            st.rerun()


# ════════════════════════════════════════════════════════════════
# STAGE: running
# ════════════════════════════════════════════════════════════════
elif st.session_state.stage == "running":

    elapsed = _elapsed()
    prog    = max(0, min(100, st.session_state.progress))

    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:1.5rem;margin-bottom:0.6rem;
                font-family:'IBM Plex Mono',monospace;font-size:0.72rem;color:#64748b;">
        <span>Elapsed: <b style="color:#475569">{elapsed}</b></span>
        <span>Progress: <b style="color:#475569">{prog}%</b></span>
        <span>Stages: <b style="color:#475569">
            {len(st.session_state.pipeline_config.get("stages", []))}</b></span>
        <span>ADF: <b style="color:#475569">{AZURE_DATA_FACTORY}</b></span>
    </div>
    """, unsafe_allow_html=True)
    prog_ph = st.empty()
    prog_ph.progress(prog / 100)
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown('<div class="card"><div class="card-label">Live Execution Log</div>', unsafe_allow_html=True)
    log_ph = st.empty()
    st.markdown('</div>', unsafe_allow_html=True)

    # Spin up thread on first pass
    if "_q" not in st.session_state:
        q: queue.Queue = queue.Queue()
        st.session_state._q = q
        threading.Thread(
            target=run_pipeline_thread,
            args=(st.session_state.csv_tmp_path,
                  st.session_state.pipeline_config,
                  st.session_state.schema, q),
            daemon=True,
        ).start()

    q: queue.Queue = st.session_state._q
    done = False

    while not q.empty():
        kind, payload = q.get_nowait()
        if kind == "log":
            st.session_state.logs.append(payload)
        elif kind == "progress":
            st.session_state.progress = payload
        elif kind == "ok":
            st.session_state.run_result      = payload
            st.session_state.stage           = "done"
            st.session_state.pipeline_end_ts = time.time()
            st.session_state.pop("_q", None)
            done = True; break
        elif kind == "error":
            st.session_state.run_error       = payload
            st.session_state.stage           = "failed"
            st.session_state.pipeline_end_ts = time.time()
            st.session_state.pop("_q", None)
            done = True; break

    prog = max(0, min(100, st.session_state.progress))
    prog_ph.progress(prog / 100)
    with log_ph:
        render_logs()

    if not done:
        time.sleep(1.5)
        st.rerun()
    else:
        st.rerun()


# ════════════════════════════════════════════════════════════════
# STAGE: done
# ════════════════════════════════════════════════════════════════
elif st.session_state.stage == "done":
    total_time = _elapsed()
    result     = st.session_state.run_result or {}
    run_id     = result.get("run_id", "N/A")
    config     = st.session_state.pipeline_config
    stages     = result.get("stages", [s["name"] for s in config.get("stages", [])])

    st.markdown(f"""
    <div style="background:#dcfce7;border:1px solid #86efac;border-radius:10px;
                padding:0.9rem 1.2rem;margin-bottom:1.2rem;
                font-family:'IBM Plex Mono',monospace;font-size:0.78rem;color:#16a34a;
                display:flex;align-items:center;gap:1.2rem;">
        <span>✓ Pipeline completed successfully</span>
        <span style="color:#64748b">·</span>
        <span style="color:#64748b">elapsed: <b style="color:#475569">{total_time}</b></span>
        <span style="color:#64748b">·</span>
        <span style="color:#64748b">run: <b style="color:#475569">{str(run_id)[:16]}…</b></span>
    </div>
    """, unsafe_allow_html=True)

    tab_sum, tab_log = st.tabs(["📋  Summary & Output", "📜  Execution Log"])

    with tab_sum:
        left_col, right_col = st.columns([1.3, 1], gap="large")

        with left_col:
            st.markdown('<div class="card"><div class="card-label">Pipeline Summary</div>', unsafe_allow_html=True)
            render_plan(config, st.session_state.schema, st.session_state.get("used_fallback", False))
            st.markdown('</div>', unsafe_allow_html=True)

        with right_col:
            st.markdown('<div class="card"><div class="card-label">Output File</div>', unsafe_allow_html=True)

            containers = config.get("containers_to_create", list(config.get("containers", {}).values()))
            if isinstance(containers, dict):
                containers = list(containers.values())
            sink_container = containers[-1] if containers else "silver"

            if st.session_state.output_csv is None:
                with st.spinner(f"Fetching output from '{sink_container}'…"):
                    data, fname = fetch_output_from_blob(sink_container)
                    st.session_state.output_csv      = data
                    st.session_state.output_filename = fname or "output.csv"

            if st.session_state.output_csv:
                st.markdown(
                    f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.74rem;'
                    f'color:#64748b;margin-bottom:1rem;line-height:1.9;">'
                    f'Container: <b style="color:#475569">{sink_container}</b><br>'
                    f'File: <b style="color:#475569">{st.session_state.output_filename}</b><br>'
                    f'Size: <b style="color:#475569">{len(st.session_state.output_csv):,} bytes</b>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                st.download_button(
                    label=f"↓ Download {st.session_state.output_filename}",
                    data=st.session_state.output_csv,
                    file_name=st.session_state.output_filename,
                    mime="text/csv",
                )
            else:
                st.warning(f"Could not fetch output from '{sink_container}'. Download from Azure portal.")

            st.markdown('</div>', unsafe_allow_html=True)

            if run_id and run_id != "N/A":
                st.markdown(
                    f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;'
                    f'color:#94a3b8;margin-top:1rem;line-height:1.8;">'
                    f'ADF Run ID: <b style="color:#64748b">{run_id}</b>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Run New Pipeline"):
                for k in list(DEFAULTS.keys()) + ["_q", "prompt_text",
                                                   "edit_num_stages", "edit_diu",
                                                   "edit_num_workers", "edit_shuffle_partitions",
                                                   "edit_node_type", "edit_container_names"]:
                    st.session_state.pop(k, None)
                st.rerun()

    with tab_log:
        st.markdown('<div class="card"><div class="card-label">Run Log</div>', unsafe_allow_html=True)
        render_logs()
        st.markdown('</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════
# STAGE: failed
# ════════════════════════════════════════════════════════════════
elif st.session_state.stage == "failed":

    st.markdown(f"""
    <div style="background:#fee2e2;border:1px solid #fca5a5;border-radius:10px;
                padding:0.9rem 1.2rem;margin-bottom:1.2rem;
                font-family:'IBM Plex Mono',monospace;font-size:0.78rem;color:#dc2626;">
        ✗ Pipeline failed: {st.session_state.run_error}
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="card"><div class="card-label">Run Log</div>', unsafe_allow_html=True)
    render_logs()
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col_retry, col_new = st.columns(2)
    with col_retry:
        if st.button("↩ Retry", use_container_width=True):
            st.session_state.stage            = "running"
            st.session_state.logs             = []
            st.session_state.progress         = 0
            st.session_state.pipeline_start_ts = time.time()
            st.session_state.pipeline_end_ts  = None
            st.session_state.pop("_q", None)
            st.rerun()
    with col_new:
        if st.button("✕ Start Over", use_container_width=True):
            for k in list(DEFAULTS.keys()) + ["_q", "prompt_text",
                                               "edit_num_stages", "edit_diu",
                                               "edit_num_workers", "edit_shuffle_partitions",
                                               "edit_node_type", "edit_container_names"]:
                st.session_state.pop(k, None)
            st.rerun()
