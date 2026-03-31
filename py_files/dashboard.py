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
from config import *

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ADF Pipeline Orchestrator",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS (Light Theme) ────────────────────────────────────────────────────────
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

[data-testid="stHeader"],
[data-testid="stToolbar"],
#MainMenu, footer { display: none !important; }

/* Global text visibility - cover all text elements */
div, span, p, label, li, td, th {
    color: #1e293b !important;
}

/* Select box / dropdown styling */
[data-testid="stSelectbox"] > div > div {
    background-color: #1e293b !important;
    color: #ffffff !important;
    border-radius: 8px !important;
    border: 1px solid #475569 !important;
}

[data-testid="stSelectbox"] input,
[data-testid="stSelectbox"] input *,
[data-testid="stSelectbox"] div,
[data-testid="stSelectbox"] span,
[data-testid="stSelectbox"] p {
    color: #ffffff !important;
    background-color: transparent !important;
}

/* ALL text inside selectbox dropdown - force white */
div[data-baseweb="popover"] *,
ul[data-baseweb="menu"] * {
    color: #ffffff !important;
}

/* Dropdown menu / options - black background, white text */
[data-testid="stSelectbox"] div[data-baseweb="select"],
[data-testid="stSelectbox"] div[data-baseweb="popover"],
[data-testid="stSelectbox"] div[role="listbox"],
[data-testid="stSelectbox"] ul,
[data-testid="stSelectbox"] li,
[data-testid="stSelectbox"] span,
[data-testid="stSelectbox"] div > div > div,
ul[data-baseweb="menu"] li,
div[data-baseweb="popover"] li,
div[data-baseweb="menu"] div {
    background-color: #1e293b !important;
    color: #ffffff !important;
}

/* Dropdown options on hover */
[data-testid="stSelectbox"] li:hover,
[data-testid="stSelectbox"] div[role="option"]:hover,
[data-testid="stSelectbox"] span:hover {
    background-color: #334155 !important;
    color: #ffffff !important;
}

/* Selected option in dropdown */
[data-testid="stSelectbox"] li[aria-selected="true"],
[data-testid="stSelectbox"] div[role="option"][aria-selected="true"] {
    background-color: #3b82f6 !important;
    color: #ffffff !important;
}

/* Hide the label text, show only value */
[data-testid="stSelectbox"] label {
    display: none !important;
}

/* Number input styling */
[data-testid="stNumberInput"],
[data-testid="stNumberInput"] > div,
[data-testid="stNumberInput"] input {
    color: #1e293b !important;
    background-color: #ffffff !important;
    border-radius: 8px !important;
}

[data-testid="stNumberInput"] input {
    border: 1px solid #e2e8f0 !important;
}

[data-testid="stNumberInput"] input:focus {
    border-color: #3b82f6 !important;
    outline: none !important;
}

/* Show value below selectbox only */
[data-testid="stSelectbox"] div[data-testid="stMarkdownContainer"] {
    display: block !important;
}

/* File uploader - all text visibility */
[data-testid="stFileUploader"],
[data-testid="stFileUploader"] *,
.stFileUploader,
.stFileUploader *,
[data-testid="stFileUploaderDropzone"],
[data-testid="stFileUploaderDropzone"] *,
[data-testid="stFileUploaderFileName"],
[data-testid="stFileUploaderFileName"] *,
[data-testid="stFileUploaderFileSize"],
[data-testid="stFileUploaderFileSize"] *,
[data-testid="stFileUploaderFileStatus"],
[data-testid="stFileUploaderFileStatus"] *,
[data-testid="stUploadedFileInfo"],
[data-testid="stUploadedFileInfo"] *,
[data-testid="stUploadedFile"],
[data-testid="stUploadedFile"] * {
    color: #1e293b !important;
}

/* Help text - lighter gray */
[data-testid="stHelpContent"],
.stHelpContent,
small {
    color: #64748b !important;
}

/* Text input styling */
[data-testid="stTextInput"] input,
[data-testid="stTextInput"] input:focus {
    color: #1e293b !important;
    background-color: #ffffff !important;
    border-radius: 8px !important;
    border: 1px solid #e2e8f0 !important;
}

[data-testid="stTextInput"] input:focus {
    border-color: #3b82f6 !important;
    outline: none !important;
}

.block-container { padding: 2rem 3rem 4rem !important; max-width: 1280px; }

/* ── Header ── */
.app-header {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 1.5rem 0 1rem;
    border-bottom: 1px solid #e2e8f0;
    margin-bottom: 1.8rem;
}
.app-logo {
    width: 36px; height: 36px;
    background: linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%);
    border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.1rem;
    box-shadow: 0 0 18px rgba(59,130,246,0.35);
}
.app-title-block {}
.app-eyebrow {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.62rem; letter-spacing: 0.2em;
    text-transform: uppercase; color: #64748b;
    margin-bottom: 0.1rem;
}
.app-title {
    font-size: 1.45rem; font-weight: 700;
    letter-spacing: -0.02em; color: #0f172a; margin: 0;
}

/* ── Status badge ── */
.badge {
    display: inline-flex; align-items: center; gap: 0.4rem;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.65rem;
    font-weight: 600; letter-spacing: 0.1em; text-transform: uppercase;
    padding: 0.22rem 0.7rem; border-radius: 999px;
}
.badge-idle    { background: #f1f5f9; color: #64748b; border: 1px solid #cbd5e1; }
.badge-running { background: #e0e7ff; color: #6366f1; border: 1px solid #c7d2fe;
                 animation: pulse 1.8s infinite; }
.badge-ok      { background: #dcfce7; color: #16a34a; border: 1px solid #86efac; }
.badge-fail    { background: #fee2e2; color: #dc2626; border: 1px solid #fca5a5; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }

/* ── Cards ── */
.card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 14px;
    padding: 1.5rem 1.7rem;
    margin-bottom: 1.1rem;
    box-shadow: 0 2px 12px rgba(0,0,0,0.05);
}
.card-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.62rem; font-weight: 600;
    letter-spacing: 0.18em; text-transform: uppercase;
    color: #3b82f6; margin-bottom: 1rem;
}

/* ── Tables ── */
.plan-table {
    width: 100%; border-collapse: collapse;
    font-family: 'IBM Plex Mono', monospace; font-size: 0.76rem;
}
.plan-table th {
    text-align: left; padding: 0.5rem 0.9rem;
    border-bottom: 1px solid #e2e8f0;
    font-size: 0.62rem; letter-spacing: 0.12em;
    text-transform: uppercase; color: #64748b; font-weight: 600;
}
.plan-table td {
    padding: 0.5rem 0.9rem; border-bottom: 1px solid #f1f5f9; color: #475569;
}
.plan-table tr:last-child td { border-bottom: none; }
.plan-table tr:hover td { background: #f8fafc; }

/* ── Pills ── */
.pill { display: inline-block; padding: 0.15rem 0.55rem; border-radius: 5px; font-size: 0.66rem; font-weight: 500; }
.pill-copy      { background: #dcfce7; color: #16a34a; }
.pill-dataflow  { background: #e0e7ff; color: #6366f1; }
.pill-integer   { background: #e0e7ff; color: #6366f1; }
.pill-double    { background: #dcfce7; color: #16a34a; }
.pill-string    { background: #fef3c7; color: #d97706; }
.pill-succeeded { background: #dcfce7; color: #16a34a; }
.pill-failed    { background: #fee2e2; color: #dc2626; }
.pill-running   { background: #e0e7ff; color: #6366f1; animation: pulse 1.8s infinite; }
.pill-inprogress { background: #e0e7ff; color: #6366f1; animation: pulse 1.8s infinite; }
.pill-queued    { background: #fef3c7; color: #d97706; }
.pill-unknown   { background: #f1f5f9; color: #64748b; }

/* ── Log terminal ── */
.log-box {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 1.1rem 1.3rem;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.74rem; line-height: 1.9;
    min-height: 320px; max-height: 480px;
    overflow-y: auto; white-space: pre-wrap; word-break: break-word;
    color: #334155;
}

/* ── Monitor grid ── */
.mon-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.9rem;
    margin-bottom: 1.2rem;
}
.mon-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 1rem 1.2rem;
}
.mon-card-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.58rem; letter-spacing: 0.14em;
    text-transform: uppercase; color: #64748b;
    margin-bottom: 0.4rem;
}
.mon-card-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.35rem; font-weight: 700; color: #0f172a;
}
.mon-card-sub {
    font-size: 0.65rem; color: #64748b; margin-top: 0.2rem;
}
.mon-card-ok    { border-color: #86efac; }
.mon-card-warn  { border-color: #fcd34d; }
.mon-card-err   { border-color: #fca5a5; }
.mon-card-blue  { border-color: #93c5fd; }

/* ── Run row ── */
.run-row {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 0.8rem 1rem;
    margin-bottom: 0.5rem;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.73rem;
}
.run-row-header {
    display: flex; align-items: center; gap: 0.7rem;
    flex-wrap: wrap;
}
.run-row-name { font-weight: 600; color: #1e293b; }
.run-row-meta { color: #64748b; font-size: 0.67rem; }
.run-row-err {
    margin-top: 0.6rem;
    background: #fee2e2;
    border-left: 3px solid #dc2626;
    padding: 0.4rem 0.7rem;
    border-radius: 0 5px 5px 0;
    color: #dc2626; font-size: 0.69rem;
    line-height: 1.6;
}

/* ── Timeline bar ── */
.tl-bar-bg {
    background: #e2e8f0; border-radius: 999px;
    height: 6px; margin: 0.5rem 0;
}
.tl-bar-fill {
    height: 6px; border-radius: 999px;
    background: linear-gradient(90deg, #3b82f6, #8b5cf6);
    transition: width 0.4s ease;
}

/* ── Section divider ── */
.sec-divider {
    border: none; border-top: 1px solid #e2e8f0; margin: 1.2rem 0;
}

/* ── Streamlit overrides ── */
[data-testid="stFileUploaderDropzone"] {
    background: #f8fafc !important;
    border: 2px dashed #cbd5e1 !important;
    border-radius: 10px !important;
}
[data-testid="stFileUploaderDropzone"]:hover {
    border-color: #3b82f6 !important;
    background: #eff6ff !important;
}
[data-testid="stFileUploaderDropzone"] button,
[data-testid="stFileUploaderDropzone"] > div button {
    background: #e0e7ff !important; color: #6366f1 !important;
    border: 1px solid #c7d2fe !important; border-radius: 6px !important;
    font-family: 'IBM Plex Sans', sans-serif !important;
    font-weight: 600 !important; font-size: 0.82rem !important;
    padding: 0.35rem 1rem !important; width: auto !important;
    box-shadow: none !important;
}

textarea {
    background: #ffffff !important; border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important; color: #1e293b !important;
    font-family: 'IBM Plex Sans', sans-serif !important;
    font-size: 0.88rem !important;
}
textarea:focus { border-color: #3b82f6 !important; outline: none !important; }

[data-testid="stButton"] > button {
    background: linear-gradient(135deg, #3b82f6, #6366f1) !important;
    color: #fff !important; border: none !important;
    border-radius: 8px !important;
    font-family: 'IBM Plex Sans', sans-serif !important;
    font-weight: 600 !important; font-size: 0.88rem !important;
    padding: 0.6rem 1.4rem !important; width: 100% !important;
    box-shadow: 0 2px 8px rgba(59,130,246,0.3) !important;
    transition: opacity 0.15s, transform 0.1s !important;
}
[data-testid="stButton"] > button:hover  { opacity: 0.88 !important; transform: translateY(-1px) !important; }
[data-testid="stButton"] > button:active { transform: translateY(0) !important; }
[data-testid="stButton"] > button:disabled {
    background: #e2e8f0 !important; color: #94a3b8 !important;
    box-shadow: none !important;
}

[data-testid="stDownloadButton"] > button {
    background: #dcfce7 !important; color: #16a34a !important;
    border: 1px solid #86efac !important; border-radius: 8px !important;
    font-family: 'IBM Plex Sans', sans-serif !important;
    font-weight: 600 !important; font-size: 0.88rem !important;
    padding: 0.6rem 1.4rem !important; width: 100% !important;
}
[data-testid="stDownloadButton"] > button:hover { background: #bbf7d0 !important; }

[data-testid="stProgress"] > div > div {
    background: linear-gradient(90deg, #3b82f6, #8b5cf6) !important;
    border-radius: 999px !important;
}
[data-testid="stProgress"] > div {
    background: #e2e8f0 !important; border-radius: 999px !important;
}

[data-testid="stMetric"] {
    background: #ffffff; border: 1px solid #e2e8f0;
    border-radius: 10px; padding: 1rem 1.2rem;
}
label[data-testid="stMetricLabel"] p { color: #64748b !important; font-size: 0.7rem !important; text-transform: uppercase; letter-spacing: 0.08em; }
[data-testid="stMetricValue"] { color: #0f172a !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 1.1rem !important; }

[data-testid="stAlert"] { border-radius: 10px !important; }

/* Tabs */
[data-testid="stTabs"] [role="tab"] {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.72rem !important; letter-spacing: 0.1em !important;
    text-transform: uppercase !important; color: #64748b !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: #6366f1 !important;
    border-bottom-color: #6366f1 !important;
}
[data-testid="stTabs"] [data-testid="stTabContent"] {
    padding-top: 1rem !important;
}

/* Expander */
[data-testid="stExpander"] {
    background: #ffffff !important; border: 1px solid #e2e8f0 !important;
    border-radius: 10px !important;
}
[data-testid="stExpander"] summary {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.75rem !important; color: #475569 !important;
}

/* Spinner */
[data-testid="stSpinner"] { color: #6366f1 !important; }

/* Success/warning/error */
.stSuccess { background: #dcfce7 !important; color: #16a34a !important; border: 1px solid #86efac !important; }
.stWarning { background: #fef3c7 !important; color: #d97706 !important; border: 1px solid #fcd34d !important; }
.stError   { background: #fee2e2 !important; color: #dc2626 !important; border: 1px solid #fca5a5 !important; }

/* Log colors for light theme */
.log-box .log-error { color: #dc2626; }
.log-box .log-success { color: #16a34a; }
.log-box .log-warn { color: #d97706; }
.log-box .log-info { color: #6366f1; }
.log-box .log-monitor { color: #0891b2; }
.log-box .log-default { color: #64748b; }
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
    "monitor_logs_current": [],
    "monitor_logs_all":   [],
    "monitor_report":     None,
    "output_csv":        None,
    "output_filename":   "output.csv",
    "run_error":         None,
    "progress":          0,
    "pipeline_start_ts": None,
    "pipeline_end_ts":   None,
    "scan_past_pipelines": True,
    "monitor_only":      False,
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

if "monitor_logs" not in st.session_state:
    st.session_state["monitor_logs"] = []

if "live_runs" not in st.session_state:
    st.session_state["live_runs"] = []

# ── Helpers ────────────────────────────────────────────────────────────────────

def read_csv_schema(filepath: str, sample_rows: int = 5) -> dict:
    import csv
    file_size = os.path.getsize(filepath)
    size_hint = (
        "small (< 5MB)"   if file_size < 5  * 1024 * 1024 else
        "medium (5–50MB)" if file_size < 50 * 1024 * 1024 else
        "large (> 50MB)"
    )
    with open(filepath, newline="", encoding="utf-8") as f:
        reader  = csv.DictReader(f)
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

    return {
        "columns": columns, "samples": samples,
        "row_count": row_count, "size_hint": size_hint,
        "inferred_types": inferred,
    }


def fetch_output_from_blob(container_name: str, sink_filename: str):
    try:
        from azure.storage.blob import BlobServiceClient
        from config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={AZURE_STORAGE_ACCOUNT};"
            f"AccountKey={AZURE_STORAGE_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        client    = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(container_name)
        blobs     = [b for b in container.list_blobs()
                     if b.size > 0 and not b.name.startswith("*")]
        if not blobs:
            return None, ""
        target = next((b for b in blobs if b.name == sink_filename), None)
        if target is None:
            target = max(blobs, key=lambda b: b.size)
        data = container.download_blob(target.name).readall()
        return data, target.name
    except Exception:
        return None, ""


def _format_duration(seconds):
    if seconds is None:
        return "N/A"
    seconds = int(seconds)
    h, rem  = divmod(seconds, 3600)
    m, s    = divmod(rem, 60)
    if h:   return f"{h}h {m}m {s}s"
    if m:   return f"{m}m {s}s"
    return f"{s}s"


# ── Pipeline + Monitor threads ─────────────────────────────────────────────────

def run_pipeline_thread(csv_path: str, pipeline_config: dict,
                        schema: dict, result_q: queue.Queue):
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
            py_dir = os.path.dirname(os.path.abspath(__file__))
            if py_dir not in sys.path:
                sys.path.insert(0, py_dir)

            from adf_api import (
                get_access_token, create_blob_container, purge_container,
                upload_csv, check_blob_has_rows, create_linked_service,
                create_dataset, create_copy_pipeline, create_dataflow_pipeline,
                publish_factory, trigger_pipeline, check_pipeline_status,
            )

            result_q.put(("log", "--- Step 1: Creating Blob Containers ---"))
            for cname in pipeline_config["containers"].values():
                create_blob_container(cname)
            raw_container = pipeline_config["containers"].get("stage0") or pipeline_config["containers"].get("raw") or list(pipeline_config["containers"].values())[0]
            purge_container(raw_container)
            for key in ["stage1", "stage2"]:
                cname = pipeline_config["containers"].get(key)
                if cname:
                    purge_container(cname)
            result_q.put(("progress", 20))

            result_q.put(("log", "--- Step 2: Uploading CSV ---"))
            raw_container = pipeline_config["containers"].get("stage0") or pipeline_config["containers"].get("raw") or list(pipeline_config["containers"].values())[0]
            upload_csv(csv_path, raw_container)
            if not check_blob_has_rows(raw_container):
                result_q.put(("error", "Upload verification failed — no rows in raw container."))
                return
            result_q.put(("progress", 35))

            result_q.put(("log", "--- Step 3: Setting up ADF resources ---"))
            token = get_access_token()
            create_linked_service(token)
            for ds in pipeline_config["datasets"]:
                r = create_dataset(token, ds)
                if r.status_code not in [200, 201]:
                    result_q.put(("error", f"Dataset creation failed: {ds['name']}"))
                    return
            result_q.put(("progress", 50))

            result_q.put(("log", "--- Step 4: Creating Pipelines ---"))
            for p in pipeline_config["pipelines"]:
                if p["type"] == "copy":
                    r = create_copy_pipeline(token, p)
                elif p["type"] == "dataflow":
                    p["inferred_types"] = schema["inferred_types"]
                    r = create_dataflow_pipeline(token, p, schema["columns"])
                else:
                    continue
                if r is None or r.status_code not in [200, 201]:
                    result_q.put(("error", f"Pipeline creation failed: {p['name']}"))
                    return
            result_q.put(("progress", 65))

            result_q.put(("log", "--- Step 5: Publishing factory ---"))
            publish_factory(token)
            result_q.put(("progress", 72))

            result_q.put(("log", "--- Step 6: Triggering Pipelines ---"))
            copy_names = [p["name"] for p in pipeline_config["pipelines"] if p["type"] == "copy"]
            total      = len(pipeline_config["execution_order"])

            for i, pl_name in enumerate(pipeline_config["execution_order"]):
                run_id = trigger_pipeline(token, pl_name)
                if not run_id:
                    result_q.put(("error", f"Could not trigger: {pl_name}"))
                    return
                result = check_pipeline_status(token, pl_name, run_id)
                if result["status"] != "Succeeded":
                    result_q.put(("error", f"{pl_name} ended with status: {result['status']}"))
                    return
                if pl_name in copy_names:
                    copy_cfg = next(p for p in pipeline_config["pipelines"] if p["name"] == pl_name)
                    sink_ds  = next((d for d in pipeline_config["datasets"]
                                     if d["name"] == copy_cfg["sink_dataset"]), None)
                    if sink_ds and not check_blob_has_rows(sink_ds["container"]):
                        result_q.put(("error", f"Copy wrote nothing to {sink_ds['container']}"))
                        return
                result_q.put(("progress", 72 + int(28 * (i + 1) / total)))

        result_q.put(("ok", pipeline_config))
    except Exception as e:
        result_q.put(("error", str(e)))


def run_monitor_thread(result_q, history_limit=20, current_pipeline_name=None):
    try:
        
        from azure.mgmt.datafactory import DataFactoryManagementClient
        from azure.identity import ClientSecretCredential
        from monitor_agent import MonitoringAgent

        credential = ClientSecretCredential(
            AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
        )

        client = DataFactoryManagementClient(credential, AZURE_SUBSCRIPTION_ID)

        monitor = MonitoringAgent(
            client,
            AZURE_RESOURCE_GROUP,
            AZURE_DATA_FACTORY,
            scan_past_pipelines=True,
            silent=True,
        )

        report = monitor.monitor(silent=True)

        result_q.put(("monitor_log", "Monitoring started..."))

        live_runs = []

        for pl_name, pl_data in report.get("pipelines", {}).items():
            runs = pl_data.get("runs", [])

            actual_count = len(runs)
            limit = min(history_limit, actual_count)
            runs = runs[:limit]

            succ = sum(1 for r in runs if r["status"] == "Succeeded")
            fail = sum(1 for r in runs if r["status"] == "Failed")
            running = sum(1 for r in runs if r["status"] in ["InProgress", "Queued", "Running"])

            result_q.put((
                "monitor_log",
                f"{pl_name} → Last {limit} runs | Success={succ} | Failed={fail} | Running={running}"
            ))

            for run in runs:
                live_runs.append({
                    "pipeline": pl_name,
                    "status": run.get("status"),
                    "duration": run.get("duration_display"),
                    "run_id": run.get("run_id"),
                    "started": run.get("started_at"),
                    "activities": run.get("activities", []),
                    "errors": run.get("errors", []),
                    "flags": run.get("flags", [])
                })

        result_q.put(("live_runs", live_runs))

    except Exception as e:
        result_q.put(("monitor_log", f"Error: {str(e)}"))
# ── Render helpers ─────────────────────────────────────────────────────────────

def render_header():
    st.markdown("""
    <div class="app-header">
        <div class="app-logo">⚡</div>
        <div class="app-title-block">
            <div class="app-eyebrow">Azure Data Factory</div>
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
        "badge-idle": "#4f6a8a", "badge-running": "#818cf8",
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
        <div style="background:#fef3c7;border:1px solid #f59e0b;border-radius:8px;padding:12px;margin-bottom:16px;">
            <strong>⚠️ Using Default Configuration</strong> — Groq API unavailable. Running with default 3-stage pipeline.
        </div>""", unsafe_allow_html=True)
    
    cols = st.columns(len(config["containers"]))
    for i, (role, name) in enumerate(config["containers"].items()):
        with cols[i]:
            st.metric(role.upper(), name)
    st.markdown("<br>", unsafe_allow_html=True)

    rows = "".join(
        f"<tr><td>{p['name']}</td>"
        f"<td><span class='pill pill-{p['type']}'>{p['type']}</span></td>"
        f"<td>{p['source_dataset']}</td><td>{p['sink_dataset']}</td>"
        f"<td>{p.get('compute_type', '-')}</td>"
        f"<td>{p.get('core_count', '-')}</td>"
        f"<td>{p.get('partition_count', '-')}</td></tr>"
        for p in config["pipelines"]
    )
    st.markdown(f"""
    <div class="card-label">Pipelines</div>
    <table class="plan-table">
      <thead><tr><th>Name</th><th>Type</th><th>Source</th><th>Sink</th><th>Compute</th><th>Cores</th><th>Partitions</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>""", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    if "recommended_settings" in config:
        rec = config["recommended_settings"]
        st.markdown(f"""
        <div class="card-label">Recommended Settings</div>
        <table class="plan-table">
          <thead><tr><th>Setting</th><th>Value</th></tr></thead>
          <tbody>
            <tr><td>Compute Type</td><td>{rec.get('compute_type', 'N/A')}</td></tr>
            <tr><td>Core Count</td><td>{rec.get('core_count', 'N/A')}</td></tr>
            <tr><td>Partition Count</td><td>{rec.get('partition_count', 'N/A')}</td></tr>
            <tr><td>Parallel Copies</td><td>{rec.get('parallel_copies', 'N/A')}</td></tr>
            <tr><td>DIU</td><td>{rec.get('diu', 'N/A')}</td></tr>
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
    <div style="font-family:'IBM Plex Mono',monospace;font-size:0.74rem;
                color:#64748b;line-height:1.9;">
        <b style="color:#475569;">Execution order:</b> {' → '.join(config['execution_order'])}<br>
        <b style="color:#475569;">Reasoning:</b> {config.get('reasoning','N/A')}
    </div>""", unsafe_allow_html=True)


def render_logs():
    def colour(line: str) -> str:
        l = line.lower()
        if any(x in l for x in ["failed", "error", "abort", "invalid"]):
            return f'<span class="log-error">{line}</span>'
        if any(x in l for x in ["succeeded", "created", "uploaded", "triggered",
                                  "verified", "ready", "obtained", "done"]):
            return f'<span class="log-success">{line}</span>'
        if any(x in l for x in ["waiting", "retrying", "propagat",
                                  "timeout", "warn", "skipping", "purging"]):
            return f'<span class="log-warn">{line}</span>'
        if any(x in l for x in ["step", "---", "groq", "publishing",
                                  "authenticat", "setting"]):
            return f'<span class="log-info">{line}</span>'
        if "[monitor]" in l or "monitor" in l:
            return f'<span class="log-monitor">{line}</span>'
        return f'<span class="log-default">{line}</span>'

    lines   = st.session_state.logs
    content = (
        '<span class="log-default">Waiting for pipeline to start…</span>'
        if not lines
        else "\n".join(colour(l) for l in lines)
    )
    st.markdown(f'<div class="log-box">{content}</div>', unsafe_allow_html=True)


def render_monitor_logs():
    def colour(line: str) -> str:
        l = line.lower()
        if any(x in l for x in ["anomaly", "failed", "error", "critical"]):
            return f'<span style="color:#ef4444;font-weight:600;">{line}</span>'
        if any(x in l for x in ["warning", "issue", "slow"]):
            return f'<span style="color:#f59e0b;">{line}</span>'
        if any(x in l for x in ["succeeded", "healthy", "all clear"]):
            return f'<span style="color:#22c55e;">{line}</span>'
        return f'<span style="color:#94a3b8;">{line}</span>'
    
    lines   = st.session_state.monitor_logs
    content = (
        '<span style="color:#94a3b8;">Monitoring agent not yet active…</span>'
        if not lines
        else "\n".join(colour(l) for l in lines)
    )
    st.markdown(f'<div class="log-box">{content}</div>', unsafe_allow_html=True)


def render_monitor_logs_with_limit(logs: list):
    def colour(line: str) -> str:
        l = line.lower()
        if any(x in l for x in ["anomaly", "failed", "error", "critical", "✗"]):
            return f'<span style="color:#ef4444;font-weight:600;">{line}</span>'
        if any(x in l for x in ["warning", "issue", "slow", "⚠"]):
            return f'<span style="color:#f59e0b;">{line}</span>'
        if any(x in l for x in ["succeeded", "healthy", "all clear", "✓"]):
            return f'<span style="color:#22c55e;">{line}</span>'
        if any(x in l for x in ["in progress", "running", "queued", "⏳"]):
            return f'<span style="color:#3b82f6;">{line}</span>'
        return f'<span style="color:#94a3b8;">{line}</span>'
    
    content = (
        '<span style="color:#94a3b8;">No logs yet...</span>'
        if not logs
        else "\n".join(colour(l) for l in logs)
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


def render_live_runs(live_runs: list):
    if not live_runs:
        st.markdown(
            '<div style="font-family:IBM Plex Mono,monospace;font-size:0.75rem;'
            'color:#94a3b8;padding:1rem;">No run data yet — waiting for first poll…</div>',
            unsafe_allow_html=True,
        )
        return

    for run in live_runs:
        flags_html = ""
        if "long_run" in run.get("flags", []):
            flags_html += " <span class='pill pill-queued'>⏱ long run</span>"
        if "anomaly" in run.get("flags", []):
            flags_html += " <span class='pill pill-queued'>📊 anomaly</span>"

        err_html = ""
        for err in run.get("errors", []):
            code = err.get("error_code", "")
            msg  = err.get("message", "")[:120]
            act  = err.get("activity", "")
            err_html += (
                f'<div class="run-row-err">'
                f'<b>{act}</b> [{code}]: {msg}'
                f'</div>'
            )

        # Activity mini-table
        act_rows = ""
        for act in run.get("activities", []):
            act_st  = act.get("status", "Unknown")
            act_dur = act.get("duration_display", "N/A")
            act_typ = act.get("activity_type", "")
            act_rows += (
                f"<tr>"
                f"<td style='color:#475569'>{act.get('activity_name','')}</td>"
                f"<td><span class='pill pill-{act_st.lower()}'>{act_st}</span></td>"
                f"<td style='color:#64748b'>{act_typ}</td>"
                f"<td style='color:#64748b'>{act_dur}</td>"
                f"</tr>"
            )
        act_table = ""
        if act_rows:
            act_table = f"""
            <table class="plan-table" style="margin-top:0.7rem">
              <thead><tr>
                <th>Activity</th><th>Status</th><th>Type</th><th>Duration</th>
              </tr></thead>
              <tbody>{act_rows}</tbody>
            </table>"""

        st.markdown(f"""
        <div class="run-row">
            <div class="run-row-header">
                <span class="run-row-name">{run['pipeline']}</span>
                {_status_pill(run['status'])}
                {flags_html}
                <span class="run-row-meta">run: {str(run['run_id'])[:12]}…</span>
                <span class="run-row-meta">⏱ {run['duration']}</span>
                <span class="run-row-meta">started: {str(run.get('started',''))[:19]}</span>
            </div>
            {err_html}
            {act_table}
        </div>""", unsafe_allow_html=True)


def render_monitoring_dashboard(report: dict | None = None):
    """Full monitoring dashboard: summary cards + run list + stats."""
    if report is None:
        # Try loading from disk
        rp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "adf_monitoring_report.json")
        if os.path.exists(rp):
            try:
                with open(rp) as f:
                    report = json.load(f)
            except Exception:
                pass

    if report is None:
        st.markdown(
            '<div style="font-family:IBM Plex Mono,monospace;font-size:0.75rem;'
            'color:#94a3b8;padding:1rem;">No monitoring data yet.</div>',
            unsafe_allow_html=True,
        )
        return

    summary   = report.get("summary", {})
    total     = summary.get("total_runs", 0)
    succeeded = summary.get("succeeded", 0)
    failed    = summary.get("failed", 0)
    in_prog   = summary.get("in_progress", 0)
    rate      = summary.get("success_rate", 0)
    status    = summary.get("status", "unknown")
    gen_at    = report.get("generated_at", "")[:19].replace("T", " ")

    ok_cls   = "mon-card-ok"   if failed == 0 else "mon-card-err"
    rate_cls = "mon-card-ok"   if rate >= 80   else "mon-card-warn"
    st_cls   = "mon-card-ok"   if status == "healthy" else "mon-card-warn"

    st.markdown(f"""
    <div class="mon-grid">
      <div class="mon-card mon-card-blue">
        <div class="mon-card-label">Total Runs</div>
        <div class="mon-card-value">{total}</div>
        <div class="mon-card-sub">last 24 h</div>
      </div>
      <div class="mon-card {ok_cls}">
        <div class="mon-card-label">Succeeded</div>
        <div class="mon-card-value" style="color:#16a34a">{succeeded}</div>
        <div class="mon-card-sub">{in_prog} in progress</div>
      </div>
      <div class="mon-card {'mon-card-err' if failed>0 else 'mon-card-ok'}">
        <div class="mon-card-label">Failed</div>
        <div class="mon-card-value" style="color:{'#dc2626' if failed>0 else '#16a34a'}">{failed}</div>
        <div class="mon-card-sub">{'needs attention' if failed>0 else 'all clear'}</div>
      </div>
      <div class="mon-card {rate_cls}">
        <div class="mon-card-label">Success Rate</div>
        <div class="mon-card-value">{rate}%</div>
        <div class="mon-card-sub">updated {gen_at}</div>
      </div>
    </div>

    <div class="tl-bar-bg">
      <div class="tl-bar-fill" style="width:{rate}%"></div>
    </div>
    """, unsafe_allow_html=True)

    # Issues
    issues = report.get("issues", [])
    if issues:
        with st.expander(f"⚠ {len(issues)} issue(s) detected", expanded=True):
            for iss in issues:
                st.markdown(
                    f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;'
                    f'color:#d97706;padding:0.2rem 0;">• {iss}</div>',
                    unsafe_allow_html=True,
                )
    else:
        st.markdown(
            '<div style="font-family:IBM Plex Mono,monospace;font-size:0.72rem;'
            'color:#16a34a;padding:0.4rem 0;">✓ No issues detected — factory healthy</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<hr class="sec-divider">', unsafe_allow_html=True)

    # Per-pipeline stats + runs
    pipelines = report.get("pipelines", {})
    if pipelines:
        st.markdown(
            '<div class="card-label" style="margin-bottom:0.7rem">Pipeline Detail</div>',
            unsafe_allow_html=True,
        )
        for pl_name, pl_data in pipelines.items():
            stats = pl_data.get("stats", {})
            runs  = pl_data.get("runs", [])

            stat_html = ""
            if stats:
                stat_html = (
                    f"<span style='color:#64748b;margin-left:0.7rem;font-size:0.67rem;'>"
                    f"mean: {_format_duration(stats.get('mean_s'))} &nbsp;·&nbsp; "
                    f"min: {_format_duration(stats.get('min_s'))} &nbsp;·&nbsp; "
                    f"max: {_format_duration(stats.get('max_s'))}"
                    f"</span>"
                )

            latest_status = runs[0]["status"] if runs else "Unknown"
            with st.expander(
                f"  {pl_name}  [{latest_status}]  {stat_html}",
                expanded=(latest_status in ("Failed", "Cancelled")),
            ):
                # Stats row
                if stats:
                    c1, c2, c3, c4, c5 = st.columns(5)
                    c1.metric("Mean",   _format_duration(stats.get("mean_s")))
                    c2.metric("Median", _format_duration(stats.get("median_s")))
                    c3.metric("Stddev", _format_duration(stats.get("stdev_s")))
                    c4.metric("Min",    _format_duration(stats.get("min_s")))
                    c5.metric("Max",    _format_duration(stats.get("max_s")))
                    st.markdown("<br>", unsafe_allow_html=True)

                for run in runs[:8]:
                    flags_html = ""
                    if "long_run" in run.get("flags", []):
                        flags_html += " <span class='pill pill-queued'>⏱ long run</span>"
                    if "anomaly" in run.get("flags", []):
                        flags_html += " <span class='pill pill-queued'>📊 anomaly</span>"

                    err_html = ""
                    for err in run.get("errors", []):
                        code = err.get("error_code", "")
                        msg  = err.get("message", "")[:140]
                        act  = err.get("activity", "")
                        cat  = err.get("category", "")
                        err_html += (
                            f'<div class="run-row-err">'
                            f'<b>{act}</b> · code: {code} · type: {cat}<br>{msg}'
                            f'</div>'
                        )

                    # Activity breakdown
                    act_rows = ""
                    for act in run.get("activities", []):
                        act_st  = act.get("status", "Unknown")
                        act_dur = act.get("duration_display", "N/A")
                        act_typ = act.get("activity_type", "")
                        act_in  = str(act.get("input") or "")[:60]
                        act_out = str(act.get("output") or "")[:60]
                        act_rows += (
                            f"<tr>"
                            f"<td style='color:#475569'>{act.get('activity_name','')}</td>"
                            f"<td>{_status_pill(act_st)}</td>"
                            f"<td style='color:#64748b'>{act_typ}</td>"
                            f"<td style='color:#64748b'>{act_dur}</td>"
                            f"<td style='color:#64748b;max-width:120px;overflow:hidden;text-overflow:ellipsis'>{act_in}</td>"
                            f"<td style='color:#64748b;max-width:120px;overflow:hidden;text-overflow:ellipsis'>{act_out}</td>"
                            f"</tr>"
                        )
                    act_table = ""
                    if act_rows:
                        act_table = f"""
                        <table class="plan-table" style="margin-top:0.6rem">
                          <thead><tr>
                            <th>Activity</th><th>Status</th><th>Type</th>
                            <th>Duration</th><th>Input</th><th>Output</th>
                          </tr></thead>
                          <tbody>{act_rows}</tbody>
                        </table>"""

                    st.markdown(f"""
                    <div class="run-row">
                        <div class="run-row-header">
                            {_status_pill(run['status'])}
                            {flags_html}
                            <span class="run-row-meta">run: {str(run.get('run_id',''))[:14]}…</span>
                            <span class="run-row-meta">⏱ {run.get('duration_display','N/A')}</span>
                            <span class="run-row-meta">{str(run.get('started_at',''))[:19]}</span>
                        </div>
                        {err_html}
                        {act_table}
                    </div>""", unsafe_allow_html=True)

        # Anomaly report
        all_anomalies = []
        for pl_name, pl_data in pipelines.items():
            for a in pl_data.get("anomalies", []):
                a["pipeline"] = pl_name
                all_anomalies.append(a)

        if all_anomalies:
            st.markdown('<hr class="sec-divider">', unsafe_allow_html=True)
            st.markdown(
                '<div class="card-label">Statistical Anomalies</div>', unsafe_allow_html=True
            )
            anom_rows = "".join(
                f"<tr>"
                f"<td style='color:#475569'>{a['pipeline']}</td>"
                f"<td style='color:#d97706'>{a.get('run_id','')[:12]}…</td>"
                f"<td>{_format_duration(a.get('duration_s'))}</td>"
                f"<td>{_format_duration(a.get('mean_s'))}</td>"
                f"<td>{_format_duration(a.get('stdev_s'))}</td>"
                f"<td style='color:#dc2626'>{a.get('z_score','')}</td>"
                f"</tr>"
                for a in all_anomalies
            )
            st.markdown(f"""
            <table class="plan-table">
              <thead><tr>
                <th>Pipeline</th><th>Run ID</th><th>Duration</th>
                <th>Mean</th><th>Std Dev</th><th>Z-Score</th>
              </tr></thead>
              <tbody>{anom_rows}</tbody>
            </table>""", unsafe_allow_html=True)


# ── Elapsed time helper ─────────────────────────────────────────────────────────

def _elapsed() -> str:
    if st.session_state.pipeline_start_ts is None:
        return "0s"
    end = st.session_state.pipeline_end_ts or time.time()
    return _format_duration(end - st.session_state.pipeline_start_ts)


# ══════════════════════════════════════════════════════════════════════
# MAIN UI
# ══════════════════════════════════════════════════════════════════════

render_header()
render_status_badge()

if "history_limit" not in st.session_state:
    st.session_state["history_limit"] = 20

# Global guard: If pipeline is running, redirect to running stage
# This prevents showing config/deploy during execution
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

    def go_to_monitor():
        st.session_state.stage = "monitor"
        st.session_state.monitor_only = True
    
    col_title, col_btn = st.columns([4, 1])
    with col_title:
        st.markdown("""
        <div>
            <h2 style="margin:0;font-weight:700;font-size:1.5rem;color:#1e293b;">ADF Pipeline Orchestrator</h2>
            <p style="margin:0.25rem 0 0;color:#64748b;font-size:0.875rem;">Upload CSV and generate ADF pipelines with AI assistance</p>
        </div>
        """, unsafe_allow_html=True)
    with col_btn:
        st.write("")
        st.write("")
        if st.button("🔍 Monitor", use_container_width=True):
            go_to_monitor()

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
            csv_path = st.session_state.get("csv_tmp_path")
            
            if not csv_path:
                st.error(f"CSV not loaded: csv_tmp_path={csv_path}")
            elif not prompt_val:
                st.error(f"Prompt is empty: prompt_text={prompt_val}")
            elif not st.session_state.get("schema"):
                st.error("Schema not loaded")
            else:
                with st.spinner("Generating pipeline plan with Groq..."):
                    try:
                        py_dir = os.path.dirname(os.path.abspath(__file__))
                        if py_dir not in sys.path:
                            sys.path.insert(0, py_dir)
                        from groq_brain import decide_pipeline_config
                        import time
                        for attempt in range(3):
                            try:
                                config, used_fallback = decide_pipeline_config(st.session_state.schema, prompt_val)
                                st.session_state.pipeline_config = config
                                st.session_state.used_fallback = used_fallback
                                st.session_state.user_prompt = prompt_val
                                st.session_state.stage = "plan"
                                st.session_state.logs = []
                                st.rerun()
                            except Exception as e:
                                if "429" in str(e) and attempt < 2:
                                    time.sleep(2)
                                    continue
                                else:
                                    raise
                    except Exception as e:
                        st.error(f"Error generating plan: {str(e)}")
        
        st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
# STAGE: plan
# ══════════════════════════════════════════════════════════════════════
elif st.session_state.stage == "plan":
    used_fallback = st.session_state.get("used_fallback", False)
    st.markdown('<div class="card"><div class="card-label">Pipeline Plan</div>', unsafe_allow_html=True)
    render_plan(st.session_state.pipeline_config, st.session_state.schema, used_fallback)
    st.markdown('</div>', unsafe_allow_html=True)

    config = st.session_state.pipeline_config
    rec = config.get("recommended_settings", {})
    schema = st.session_state.schema
    
    st.markdown('<div class="card" style="background-color:#f8fafc;padding:1rem;border-radius:0.5rem;margin-bottom:1rem;">', unsafe_allow_html=True)
    st.markdown('<div style="color:#1e293b;font-weight:600;font-size:1rem;margin-bottom:0.5rem;">⚙️ Pipeline Configuration</div>', unsafe_allow_html=True)
    st.markdown(f'<div style="color:#64748b;font-size:0.85rem;margin-bottom:1rem;">Recommended settings for {schema.get("size_hint", "medium")} data. Edit as needed.</div>', unsafe_allow_html=True)
    
    if "edit_num_stages" not in st.session_state:
        st.session_state.edit_num_stages = config.get("num_containers", 3)
    if "edit_compute_type" not in st.session_state:
        st.session_state.edit_compute_type = rec.get("compute_type", "General")
    if "edit_core_count" not in st.session_state:
        st.session_state.edit_core_count = rec.get("core_count", 4)
    if "edit_partition_count" not in st.session_state:
        st.session_state.edit_partition_count = rec.get("partition_count", 4)
    if "edit_parallel_copies" not in st.session_state:
        st.session_state.edit_parallel_copies = rec.get("parallel_copies", 2)
    if "edit_diu" not in st.session_state:
        st.session_state.edit_diu = rec.get("diu", 2)
    
    st.markdown('<div style="color:#ffffff;font-weight:500;font-size:0.9rem;margin-top:1rem;">Number of Stages</div>', unsafe_allow_html=True)
    new_num_stages = st.number_input(
        "stages", 
        min_value=2, max_value=5, 
        value=st.session_state.edit_num_stages,
        label_visibility="collapsed",
        help="2-5 stages. More stages = more processing steps but longer execution time."
    )
    st.session_state.edit_num_stages = new_num_stages
    
    st.markdown('<div style="color:#ffffff;font-weight:500;font-size:0.9rem;margin-top:1rem;">Compute Settings</div>', unsafe_allow_html=True)
    
    compute_options = ["Cost Optimized", "Performance Optimized"]
    current_compute_idx = compute_options.index(st.session_state.edit_compute_type) if st.session_state.edit_compute_type in compute_options else 0
    st.markdown('<div style="color:#ffffff;font-size:0.85rem;margin-bottom:0.3rem;">Compute Type</div>', unsafe_allow_html=True)
    new_compute = st.selectbox(
        "compute_type",
        compute_options,
        index=current_compute_idx,
        label_visibility="collapsed",
        help="Cost Optimized = lower cost | Performance Optimized = faster processing"
    )
    st.session_state.edit_compute_type = new_compute
    
    if new_compute == "Cost Optimized":
        core_options = [4, 8, 16]
        partition_options = [2, 4, 8]
        parallel_options = [1, 2, 4]
        diu_options = [1, 2, 4]
    else:
        core_options = [8, 16, 32]
        partition_options = [8, 16, 32]
        parallel_options = [4, 8, 16]
        diu_options = [4, 8, 16]
    
    if st.session_state.edit_core_count not in core_options:
        st.session_state.edit_core_count = core_options[1]
    if st.session_state.edit_partition_count not in partition_options:
        st.session_state.edit_partition_count = partition_options[1]
    if st.session_state.edit_parallel_copies not in parallel_options:
        st.session_state.edit_parallel_copies = parallel_options[2]
    if st.session_state.edit_diu not in diu_options:
        st.session_state.edit_diu = diu_options[2]
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div style="color:#ffffff;font-size:0.85rem;margin-bottom:0.3rem;">Core Count</div>', unsafe_allow_html=True)
        current_core_idx = core_options.index(st.session_state.edit_core_count)
        new_cores = st.selectbox(
            "core_count",
            core_options,
            index=current_core_idx,
            label_visibility="collapsed",
            help="More cores = faster processing but higher cost"
        )
        st.session_state.edit_core_count = new_cores
        
        st.markdown('<div style="color:#ffffff;font-size:0.85rem;margin-bottom:0.3rem;margin-top:0.5rem;">Parallel Copies</div>', unsafe_allow_html=True)
        current_parallel_idx = parallel_options.index(st.session_state.edit_parallel_copies)
        new_parallel = st.selectbox(
            "parallel_copies",
            parallel_options,
            index=current_parallel_idx,
            label_visibility="collapsed",
            help="Number of parallel copy operations"
        )
        st.session_state.edit_parallel_copies = new_parallel
    
    with col2:
        st.markdown('<div style="color:#ffffff;font-size:0.85rem;margin-bottom:0.3rem;">Partition Count</div>', unsafe_allow_html=True)
        current_partition_idx = partition_options.index(st.session_state.edit_partition_count)
        new_partitions = st.selectbox(
            "partition_count",
            partition_options,
            index=current_partition_idx,
            label_visibility="collapsed",
            help="More partitions = better parallelism but more overhead"
        )
        st.session_state.edit_partition_count = new_partitions
        
        st.markdown('<div style="color:#ffffff;font-size:0.85rem;margin-bottom:0.3rem;margin-top:0.5rem;">DIU (Data Integration Units)</div>', unsafe_allow_html=True)
        current_diu_idx = diu_options.index(st.session_state.edit_diu)
        new_diu = st.selectbox(
            "diu",
            diu_options,
            index=current_diu_idx,
            label_visibility="collapsed",
            help="Higher DIU = faster data movement"
        )
        st.session_state.edit_diu = new_diu
    
    st.markdown("---")
    if "edit_container_names" not in st.session_state:
        current_containers = list(config.get("containers", {}).values())
        st.session_state.edit_container_names = ", ".join(current_containers)
    
    new_containers = st.text_input(
        "Container names (comma-separated)",
        value=st.session_state.edit_container_names,
        placeholder="e.g. raw, bronze, silver",
        label_visibility="visible",
        help="Leave empty to use default names from the plan"
    )
    st.session_state.edit_container_names = new_containers
    
    apply_btn = st.button("Apply Settings", type="primary", use_container_width=True)
    
    if apply_btn:
        py_dir = os.path.dirname(os.path.abspath(__file__))
        if py_dir not in sys.path:
            sys.path.insert(0, py_dir)
        from groq_brain import decide_pipeline_config
        
        container_list = None
        if new_containers.strip():
            container_list = [c.strip() for c in new_containers.split(",")]
        
        custom_settings = {
            "compute_type": new_compute,
            "core_count": new_cores,
            "partition_count": new_partitions,
            "parallel_copies": new_parallel,
            "diu": new_diu
        }
        
        with st.spinner("Regenerating pipeline plan with new settings..."):
            try:
                new_config, _ = decide_pipeline_config(
                    schema,
                    st.session_state.get("user_prompt", ""),
                    num_containers=new_num_stages,
                    custom_settings=custom_settings,
                    container_names=container_list if container_list and len(container_list) == new_num_stages else None
                )
                st.session_state.pipeline_config = new_config
                st.session_state.edit_compute_type = new_compute
                st.session_state.edit_core_count = new_cores
                st.session_state.edit_partition_count = new_partitions
                st.session_state.edit_parallel_copies = new_parallel
                st.session_state.edit_diu = new_diu
                st.session_state.edit_num_stages = new_num_stages
                st.session_state.edit_container_names = ", ".join(list(new_config.get("containers", {}).values()))
                st.success("Settings applied! Review the updated plan above.")
                st.rerun()
            except Exception as e:
                st.error(f"Error applying settings: {str(e)}")
    
    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    col_back, col_deploy = st.columns(2)
    with col_back:
        if st.button("← Back"):
            for key in ["edit_compute_type", "edit_core_count", "edit_partition_count", 
                        "edit_parallel_copies", "edit_diu", "edit_num_stages", 
                        "edit_container_names"]:
                st.session_state.pop(key, None)
            st.session_state.stage = "input"
            st.rerun()
    with col_deploy:
        if st.button("⚡ Deploy to ADF"):
            st.session_state.stage              = "running"
            st.session_state.logs               = []
            st.session_state.monitor_logs_current = []
            st.session_state.monitor_logs_all    = []
            st.session_state.monitor_report     = None
            st.session_state.progress           = 0
            st.session_state.pipeline_start_ts  = time.time()
            st.session_state.pipeline_end_ts    = None
            st.rerun()


# ════════════════════════════════════════════════════════════════
# STAGE: running
# ════════════════════════════════════════════════════════════════
elif st.session_state.stage == "running":

    # ── Top status bar ──────────────────────────────────────────
    elapsed = _elapsed()
    prog    = max(0, min(100, st.session_state.progress))

    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:1.5rem;margin-bottom:0.6rem;
                font-family:'IBM Plex Mono',monospace;font-size:0.72rem;color:#64748b;">
        <span>Elapsed: <b style="color:#475569">{elapsed}</b></span>
        <span>Progress: <b style="color:#475569">{prog}%</b></span>
        <span>Pipelines: <b style="color:#475569">
            {len(st.session_state.pipeline_config.get('execution_order', []))}</b></span>
    </div>
    """, unsafe_allow_html=True)
    prog_ph_top = st.empty()
    prog_ph_top.progress(prog / 100)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Monitor settings ─────────────────────────────────────────
    col_settings, col_tabs = st.columns([1, 4])
    with col_settings:
        scan_past = st.toggle(
            "Scan Past Pipelines",
            value=st.session_state.scan_past_pipelines,
            help="When enabled, monitors both running and past pipelines. When disabled, only scans currently running pipelines for live monitoring."
        )
        if scan_past != st.session_state.scan_past_pipelines:
            st.session_state.scan_past_pipelines = scan_past

    # ── Three tabs ────────────────────────────────────────────────
    tab_exec, tab_mon_curr, tab_mon_all = st.tabs(["⚡ Execution Log", "📊 Monitor (Current)", "🌐 Monitor (All)"])

    with tab_exec:
        st.markdown('<div class="card-label">Live Execution Log</div>', unsafe_allow_html=True)
        log_ph = st.empty()

    with tab_mon_curr:
        st.markdown('<div class="card-label">Monitor Logs (Current Pipeline)</div>', unsafe_allow_html=True)
        mon_log_curr_ph = st.empty()

    with tab_mon_all:
        st.markdown('<div class="card-label">Monitor Logs (All ADF Pipelines)</div>', unsafe_allow_html=True)
        mon_log_all_ph = st.empty()

    # ── Spin up threads on first pass ───────────────────────────
    if "_q" not in st.session_state:
        q: queue.Queue = queue.Queue()
        mq: queue.Queue = queue.Queue()
        st.session_state._q  = q
        st.session_state._mq = mq

        threading.Thread(
            target=run_pipeline_thread,
            args=(st.session_state.csv_tmp_path,
                  st.session_state.pipeline_config,
                  st.session_state.schema, q),
            daemon=True,
        ).start()

        current_pipeline = None
        if st.session_state.pipeline_config and st.session_state.pipeline_config.get("execution_order"):
            current_pipeline = st.session_state.pipeline_config["execution_order"][0]

        threading.Thread(
            target=run_monitor_thread,
            args=(mq, st.session_state["history_limit"]),
            daemon=True,
        ).start()

    q:  queue.Queue = st.session_state._q
    mq: queue.Queue = st.session_state._mq
    done = False

    # Drain pipeline queue
    while not q.empty():
        kind, payload = q.get_nowait()
        if kind == "log":
            st.session_state.logs.append(payload)
        elif kind == "progress":
            st.session_state.progress = payload
        elif kind == "ok":
            st.session_state.pipeline_config = payload
            st.session_state.stage           = "done"
            st.session_state.pipeline_end_ts = time.time()
            st.session_state.pop("_q",  None)
            st.session_state.pop("_mq", None)
            done = True; break
        elif kind == "error":
            st.session_state.run_error   = payload
            st.session_state.stage       = "failed"
            st.session_state.pipeline_end_ts = time.time()
            st.session_state.pop("_q",  None)
            st.session_state.pop("_mq", None)
            done = True; break

    # Drain monitor queue
    while not mq.empty():
        kind, payload = mq.get_nowait()
        if kind == "monitor_log":
            st.session_state.monitor_logs.append(payload)
            st.session_state.monitor_logs_all.append(payload)
        elif kind == "live_runs":
            st.session_state.live_runs = payload

    # Update progress bar at top
    prog = max(0, min(100, st.session_state.progress))
    prog_ph_top.progress(prog / 100)

    # Render execution log
    with tab_exec:
        with log_ph:
            render_logs()

    # Render current pipeline monitor
    with tab_mon_curr:
        limit_curr = st.session_state.get("log_limit_curr", 50)
        logs_to_show = st.session_state.monitor_logs_current[-limit_curr:]
        with mon_log_curr_ph:
            render_monitor_logs_with_limit(logs_to_show)

    # Render all pipelines monitor
    with tab_mon_all:
        limit_all = st.session_state.get("log_limit_all", 50)
        logs_to_show_all = st.session_state.monitor_logs_all[-limit_all:]
        with mon_log_all_ph:
            render_monitor_logs_with_limit(logs_to_show_all)

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

    st.markdown(f"""
    <div style="background:#dcfce7;border:1px solid #86efac;border-radius:10px;
                padding:0.9rem 1.2rem;margin-bottom:1.2rem;
                font-family:'IBM Plex Mono',monospace;font-size:0.78rem;color:#16a34a;
                display:flex;align-items:center;gap:1.2rem;">
        <span>✓ Pipeline completed successfully</span>
        <span style="color:#64748b">·</span>
        <span style="color:#64748b">elapsed: <b style="color:#475569">{total_time}</b></span>
    </div>
    """, unsafe_allow_html=True)

    tab_sum, tab_exec_log, tab_mon = st.tabs(
        ["📋  Summary & Output", "📜  Execution Log", "📊  Monitoring"]
    )

    with tab_sum:
        left_col, right_col = st.columns([1.3, 1], gap="large")

        with left_col:
            st.markdown('<div class="card"><div class="card-label">Pipeline Summary</div>', unsafe_allow_html=True)
            render_plan(st.session_state.pipeline_config, st.session_state.schema, st.session_state.get("used_fallback", False))
            st.markdown('</div>', unsafe_allow_html=True)

        with right_col:
            st.markdown('<div class="card"><div class="card-label">Output File</div>', unsafe_allow_html=True)

            config        = st.session_state.pipeline_config
            stage2        = config["containers"].get("stage2", "silver")
            sink_ds       = next((d for d in config["datasets"] if d.get("role") == "sink"),
                                 {"filename": "output.csv", "container": stage2})
            sink_filename = sink_ds.get("filename", "output.csv")

            if st.session_state.output_csv is None:
                with st.spinner(f"Fetching from '{stage2}'…"):
                    data, fname = fetch_output_from_blob(stage2, sink_filename)
                    st.session_state.output_csv      = data
                    st.session_state.output_filename = fname or sink_filename

            if st.session_state.output_csv:
                st.markdown(
                    f'<div style="font-family:IBM Plex Mono,monospace;font-size:0.74rem;'
                    f'color:#64748b;margin-bottom:1rem;line-height:1.9;">'
                    f'Container: <b style="color:#475569">{stage2}</b><br>'
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
                st.warning(f"Could not fetch output from '{stage2}'. Download from Azure portal.")

            st.markdown('</div>', unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("⚡ Run New Pipeline"):
                for k in list(DEFAULTS.keys()) + ["_q", "_mq", "prompt_text"]:
                    st.session_state.pop(k, None)
                st.rerun()

    with tab_exec_log:
        st.markdown('<div class="card"><div class="card-label">Run Log</div>', unsafe_allow_html=True)
        render_logs()
        st.markdown('</div>', unsafe_allow_html=True)

    with tab_mon:
        st.markdown('<div class="card"><div class="card-label">Monitoring Report</div>', unsafe_allow_html=True)

        # Stats row
        if st.session_state.pipeline_start_ts and st.session_state.pipeline_end_ts:
            elapsed_s = st.session_state.pipeline_end_ts - st.session_state.pipeline_start_ts
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Duration",   _format_duration(elapsed_s))
            c2.metric("Pipelines Run",    str(len(st.session_state.pipeline_config.get("execution_order", []))))
            c3.metric("Datasets Created", str(len(st.session_state.pipeline_config.get("datasets", []))))
            c4.metric("Containers",       str(len(st.session_state.pipeline_config.get("containers", {}))))
            st.markdown("<br>", unsafe_allow_html=True)

        render_monitoring_dashboard(st.session_state.monitor_report)
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('<div class="card"><div class="card-label">Monitor Logs (All ADF Pipelines)</div>', unsafe_allow_html=True)
        render_monitor_logs_with_limit(st.session_state.monitor_logs_all)
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

    tab_log, tab_mon = st.tabs(["📜 Execution Log", "📊 Monitoring"])

    with tab_log:
        st.markdown('<div class="card"><div class="card-label">Run Log</div>', unsafe_allow_html=True)
        render_logs()
        st.markdown('</div>', unsafe_allow_html=True)

    with tab_mon:
        st.markdown('<div class="card"><div class="card-label">Monitor Agent Log</div>', unsafe_allow_html=True)
        render_monitor_logs_with_limit(st.session_state.monitor_logs_all)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col_retry, col_new = st.columns(2)
    with col_retry:
        if st.button("↩ Retry"):
            st.session_state.stage              = "running"
            st.session_state.logs               = []
            st.session_state.monitor_logs_current = []
            st.session_state.monitor_logs_all    = []
            st.session_state.progress           = 0
            st.session_state.pipeline_start_ts  = time.time()
            st.session_state.pipeline_end_ts    = None
            st.session_state.pop("_q",  None)
            st.session_state.pop("_mq", None)
            st.rerun()
    with col_new:
        if st.button("✕ Start Over"):
            for k in list(DEFAULTS.keys()) + ["_q", "_mq", "prompt_text"]:
                st.session_state.pop(k, None)
            st.rerun()


# ════════════════════════════════════════════════════════════════
# STAGE: monitor (Standalone Monitoring)
# ════════════════════════════════════════════════════════════════
elif st.session_state.stage == "monitor":

    st.markdown("""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1.5rem;">
        <div>
            <h2 style="margin:0;font-weight:700;font-size:1.5rem;color:#1e293b;">🔍 Pipeline Monitoring</h2>
            <p style="margin:0.25rem 0 0;color:#64748b;font-size:0.875rem;">Real-time status of all ADF pipelines</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Slider to control number of runs to analyze
    col_slider, col_btn = st.columns([3, 1])
    with col_slider:
        history_limit = st.slider("Number of runs to analyze", 0, 100, 20, key="history_limit_slider", on_change=None)
        st.session_state["history_limit"] = history_limit
    with col_btn:
        st.write("")
        st.write("")
        if st.button("← Back to Pipeline Builder"):
            st.session_state.stage = "input"
            st.session_state.monitor_only = False
            st.session_state.pop("_monitor_q", None)
            st.rerun()

    st.markdown('<hr class="sec-divider">', unsafe_allow_html=True)

    # Check if we need to fetch new data
    trigger_key = "_monitor_trigger"
    report_key = "_cached_report"
    
    current_trigger = st.session_state.get(trigger_key, 0)
    previous_trigger = st.session_state.get("_previous_trigger", -1)
    
    # Only fetch on first entry (previous_trigger == -1) or when refresh is clicked
    should_fetch = (previous_trigger == -1) or (current_trigger > previous_trigger)
    
    if should_fetch:
        from monitor_agent import MonitoringAgent
        from azure.mgmt.datafactory import DataFactoryManagementClient
        from azure.identity import ClientSecretCredential
        from config import (AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP, AZURE_DATA_FACTORY,
                           AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET)
        
        mq = queue.Queue()
        
        def run_standalone_monitor(mq, history_limit=20):
            try:
                credential = ClientSecretCredential(
                    AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
                )
                adf_client = DataFactoryManagementClient(credential, AZURE_SUBSCRIPTION_ID)
                monitor = MonitoringAgent(
                    adf_client,
                    AZURE_RESOURCE_GROUP,
                    AZURE_DATA_FACTORY,
                    silent=True,
                )
                report = monitor.monitor(silent=True, limit=history_limit)
                mq.put(("report", report))
            except Exception as e:
                mq.put(("error", str(e)))
        
        history_limit = st.session_state.get("history_limit", 20)
        monitor_thread = threading.Thread(target=run_standalone_monitor, args=(mq, history_limit), daemon=True)
        monitor_thread.start()
        
        # Wait for report
        report = None
        max_wait = 60
        wait_count = 0
        while report is None and wait_count < max_wait:
            time.sleep(1)
            while not mq.empty():
                kind, data = mq.get_nowait()
                if kind == "report":
                    report = data
                    # Also add to logs
                    for pl_name, pl_data in data.get("pipelines", {}).items():
                        runs = pl_data.get("runs", [])
                        succ = sum(1 for r in runs if r["status"] == "Succeeded")
                        fail = sum(1 for r in runs if r["status"] == "Failed")
                        running = sum(1 for r in runs if r["status"] in ["InProgress", "Queued", "Running"])
                        st.session_state.monitor_logs_all.append(
                            f"{pl_name} → Last {len(runs)} runs | Success={succ} | Failed={fail} | Running={running}"
                        )
                elif kind == "error":
                    st.error(f"Monitor error: {data}")
                    st.session_state.monitor_logs_all.append(f"Error: {data}")
            wait_count += 1
            if report:
                break
        
        # Update trigger to prevent re-fetch
        st.session_state["_previous_trigger"] = current_trigger
        st.session_state[report_key] = report
    else:
        report = st.session_state.get(report_key)
    
    st.markdown('<div class="card"><div class="card-label">Pipeline Status Overview</div>', unsafe_allow_html=True)
    
    if report:
        summary = report.get("summary", {})
        total = summary.get("total_runs", 0)
        succeeded = summary.get("succeeded", 0)
        failed = summary.get("failed", 0)
        in_progress = summary.get("in_progress", 0)
        success_rate = summary.get("success_rate", 0)
        
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total Runs", total)
        with col2:
            st.metric("Succeeded", succeeded)
        with col3:
            st.metric("Failed", failed)
        with col4:
            st.metric("In Progress", in_progress)
        with col5:
            st.metric("Success Rate", f"{success_rate}%")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        issues = report.get("issues", [])
        if issues:
            st.markdown('<div class="card"><div class="card-label" style="color:#ef4444;">Issues Detected</div>', unsafe_allow_html=True)
            for issue in issues[:10]:
                st.markdown(f"- {issue}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        pipelines = report.get("pipelines", {})
        if pipelines:
            st.markdown('<div class="card"><div class="card-label">Pipeline Runs</div>', unsafe_allow_html=True)
            for pl_name, pl_data in pipelines.items():
                runs = pl_data.get("runs", [])
                stats = pl_data.get("stats", {})
                resources = pl_data.get("resources", {})
                
                # Build header with stats
                stat_line = ""
                if stats:
                    mean_dur = stats.get("mean_s")
                    if mean_dur:
                        stat_line = f" | avg: {_format_duration(mean_dur)}"
                
                if runs:
                    with st.expander(f"📦 {pl_name} ({len(runs)} runs){stat_line}"):
                        # Show pipeline resources if available
                        if resources:
                            compute_types = resources.get("cloud_compute", {})
                            act_types = resources.get("activity_type_counts", {})
                            
                            if compute_types:
                                compute_str = ", ".join(compute_types.keys())
                                st.markdown(f"**Compute:** {compute_str}")
                            if act_types:
                                acts_str = ", ".join(f"{k}: {v}" for k, v in act_types.items())
                                st.markdown(f"**Activities:** {acts_str}")
                            st.markdown("---")
                        
                        for run in runs:
                            status = run.get("status", "Unknown")
                            duration = run.get("duration_display", "N/A")
                            run_id = run.get("run_id", "")[:16]
                            started = run.get("started_at", "")[:19] if run.get("started_at") else "N/A"
                            ended = run.get("ended_at", "")[:19] if run.get("ended_at") else "N/A"
                            duration_s = run.get("duration_s")
                            
                            # Get activities info
                            activities = run.get("activities", [])
                            act_types = list(set(a.get("activity_type", "Unknown") for a in activities))
                            
                            flags = run.get("flags", [])
                            flags_str = " | ".join(flags) if flags else ""
                            
                            if status == "Succeeded":
                                st.success(f"✓ {run_id} | {status} | {duration} | started: {started} | ended: {ended}")
                            elif status == "Failed":
                                st.error(f"✗ {run_id} | {status} | {duration} | started: {started} | ended: {ended}")
                                for err in run.get("errors", []):
                                    st.markdown(f"  → **{err.get('activity', 'N/A')}**: {err.get('error_code', '')}: {err.get('message', '')[:100]}")
                            elif status == "InProgress" or status == "Running":
                                st.info(f"⏳ {run_id} | {status} | {duration} | started: {started}")
                            else:
                                st.markdown(f"⚪ {run_id} | {status} | {duration} | started: {started} | ended: {ended}")
                            
                            # Show activity types and flags
                            if act_types:
                                st.markdown(f"  *Activities: {', '.join(act_types)}*")
                            if flags_str:
                                st.markdown(f"  *Flags: {flags_str}*")
            st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.info("Loading pipeline data from ADF...")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Refresh button
    col_refresh = st.columns([1])
    with col_refresh[0]:
        if st.button("🔄 Refresh Now"):
            st.session_state["_monitor_trigger"] = st.session_state.get("_monitor_trigger", 0) + 1
            st.rerun()
    
    # Display logs
    st.markdown('<div class="card"><div class="card-label">Monitor Logs (All ADF Pipelines)</div>', unsafe_allow_html=True)
    render_monitor_logs_with_limit(st.session_state.monitor_logs_all)
    st.markdown('</div>', unsafe_allow_html=True)