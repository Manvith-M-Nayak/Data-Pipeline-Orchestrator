import streamlit as st
import sys
import os
import tempfile
import time
import threading
import queue
import io

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ADF Pipeline Orchestrator",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* ── Base ── */
html, body,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewBlockContainer"],
.main { background: #f9fafb !important; color: #111827; font-family: 'Inter', sans-serif; }

[data-testid="stHeader"]  { display: none !important; }
[data-testid="stToolbar"] { display: none !important; }
#MainMenu, footer         { display: none !important; }

.block-container { padding: 2.5rem 3rem 4rem !important; max-width: 1180px; }

/* ── Header ── */
.app-header { margin-bottom: 2rem; }
.app-eyebrow {
    font-family: 'JetBrains Mono', monospace; font-size: 0.68rem;
    font-weight: 500; letter-spacing: 0.18em; text-transform: uppercase;
    color: #6366f1; margin-bottom: 0.3rem;
}
.app-title {
    font-size: 1.9rem; font-weight: 700; letter-spacing: -0.025em;
    color: #111827; margin: 0;
}

/* ── Status badge ── */
.badge {
    display: inline-flex; align-items: center; gap: 0.4rem;
    font-family: 'JetBrains Mono', monospace; font-size: 0.68rem;
    font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase;
    padding: 0.25rem 0.75rem; border-radius: 999px;
}
.badge-idle    { background:#f3f4f6; color:#6b7280; border:1px solid #e5e7eb; }
.badge-running { background:#eef2ff; color:#6366f1; border:1px solid #c7d2fe;
                 animation: pulse 1.8s infinite; }
.badge-ok      { background:#f0fdf4; color:#16a34a; border:1px solid #bbf7d0; }
.badge-fail    { background:#fef2f2; color:#dc2626; border:1px solid #fecaca; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.55} }

/* ── Cards ── */
.card {
    background: #ffffff; border: 1px solid #e5e7eb;
    border-radius: 12px; padding: 1.5rem 1.6rem; margin-bottom: 1rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
.card-label {
    font-family: 'JetBrains Mono', monospace; font-size: 0.65rem;
    font-weight: 600; letter-spacing: 0.15em; text-transform: uppercase;
    color: #6366f1; margin-bottom: 0.9rem;
}

/* ── Tables ── */
.plan-table {
    width: 100%; border-collapse: collapse;
    font-family: 'JetBrains Mono', monospace; font-size: 0.78rem;
}
.plan-table th {
    text-align: left; padding: 0.45rem 0.8rem;
    border-bottom: 2px solid #e5e7eb; font-size: 0.65rem;
    letter-spacing: 0.1em; text-transform: uppercase; color: #6b7280;
    font-weight: 600;
}
.plan-table td {
    padding: 0.45rem 0.8rem; border-bottom: 1px solid #f3f4f6; color: #374151;
}
.plan-table tr:last-child td { border-bottom: none; }
.plan-table tr:hover td { background: #fafafa; }

/* ── Pills ── */
.pill { display:inline-block; padding:0.15rem 0.55rem; border-radius:5px; font-size:0.68rem; font-weight:500; }
.pill-copy      { background:#f0fdf4; color:#16a34a; }
.pill-dataflow  { background:#eef2ff; color:#6366f1; }
.pill-integer   { background:#eef2ff; color:#6366f1; }
.pill-double    { background:#f0fdf4; color:#16a34a; }
.pill-string    { background:#fffbeb; color:#d97706; }

/* ── Log terminal ── */
.log-box {
    background: #1e1e2e; border-radius: 10px;
    padding: 1.1rem 1.3rem; font-family: 'JetBrains Mono', monospace;
    font-size: 0.76rem; line-height: 1.8; min-height: 300px; max-height: 420px;
    overflow-y: auto; white-space: pre-wrap; word-break: break-word;
}

/* ── Divider ── */
hr { border: none; border-top: 1px solid #e5e7eb; margin: 1.5rem 0; }

/* ── File uploader ── */
[data-testid="stFileUploaderDropzone"] {
    background: #fafafa !important;
    border: 2px dashed #d1d5db !important;
    border-radius: 10px !important;
}
[data-testid="stFileUploaderDropzone"]:hover {
    border-color: #6366f1 !important;
    background: #f5f3ff !important;
}
/* Browse files button inside the dropzone */
[data-testid="stFileUploaderDropzone"] button,
[data-testid="stFileUploaderDropzone"] > div button {
    background: #eef2ff !important;
    color: #6366f1 !important;
    border: 1px solid #c7d2fe !important;
    border-radius: 6px !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.82rem !important;
    padding: 0.35rem 1rem !important;
    width: auto !important;
    box-shadow: none !important;
}
[data-testid="stFileUploaderDropzone"] button:hover,
[data-testid="stFileUploaderDropzone"] > div button:hover {
    background: #e0e7ff !important;
    color: #4f46e5 !important;
    transform: none !important;
}

/* ── Textarea ── */
textarea {
    background: #ffffff !important;
    border: 1px solid #d1d5db !important;
    border-radius: 8px !important;
    color: #111827 !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 0.88rem !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
}
textarea:focus {
    border-color: #6366f1 !important;
    box-shadow: 0 0 0 3px #eef2ff !important;
    outline: none !important;
}

/* ── Primary button ── */
[data-testid="stButton"] > button {
    background: #6366f1 !important; color: #fff !important;
    border: none !important; border-radius: 8px !important;
    font-family: 'Inter', sans-serif !important; font-weight: 600 !important;
    font-size: 0.88rem !important; padding: 0.6rem 1.4rem !important;
    width: 100% !important; transition: background 0.15s, transform 0.1s !important;
    box-shadow: 0 1px 3px rgba(99,102,241,0.3) !important;
}
[data-testid="stButton"] > button:hover  { background: #4f46e5 !important; transform: translateY(-1px) !important; }
[data-testid="stButton"] > button:active { transform: translateY(0) !important; }
[data-testid="stButton"] > button:disabled {
    background: #f3f4f6 !important; color: #9ca3af !important;
    box-shadow: none !important;
}

/* ── Download button ── */
[data-testid="stDownloadButton"] > button {
    background: #f0fdf4 !important; color: #16a34a !important;
    border: 1px solid #bbf7d0 !important; border-radius: 8px !important;
    font-family: 'Inter', sans-serif !important; font-weight: 600 !important;
    font-size: 0.88rem !important; padding: 0.6rem 1.4rem !important;
    width: 100% !important;
}
[data-testid="stDownloadButton"] > button:hover { background: #dcfce7 !important; }

/* ── Progress bar ── */
[data-testid="stProgress"] > div > div { background: #6366f1 !important; border-radius: 999px !important; }
[data-testid="stProgress"] > div { background: #e5e7eb !important; border-radius: 999px !important; }

/* ── Metric ── */
[data-testid="stMetric"] {
    background: #ffffff; border: 1px solid #e5e7eb;
    border-radius: 10px; padding: 1rem 1.2rem;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
}
label[data-testid="stMetricLabel"] p { color: #6b7280 !important; font-size: 0.72rem !important; text-transform: uppercase; letter-spacing: 0.08em; }
[data-testid="stMetricValue"] { color: #111827 !important; font-family: 'JetBrains Mono', monospace !important; font-size: 1.1rem !important; }

/* ── Alert / success / error ── */
[data-testid="stAlert"] { border-radius: 10px !important; }
</style>
""", unsafe_allow_html=True)


# ── Session state defaults ─────────────────────────────────────────────────────
DEFAULTS = {
    "stage":           "input",
    "pipeline_config": None,
    "user_prompt":     "",
    "schema":          None,
    "csv_tmp_path":    None,
    "logs":            [],
    "output_csv":      None,
    "output_filename": "output.csv",
    "run_error":       None,
    "progress":        0,
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ── Helpers ────────────────────────────────────────────────────────────────────

def read_csv_schema(filepath: str, sample_rows: int = 5) -> dict:
    import csv
    file_size = os.path.getsize(filepath)
    size_hint = (
        "small (< 5MB)"   if file_size < 5  * 1024 * 1024 else
        "medium (5-50MB)" if file_size < 50 * 1024 * 1024 else
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

    return {"columns": columns, "samples": samples,
            "row_count": row_count, "size_hint": size_hint,
            "inferred_types": inferred}


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
            for key in ["stage1", "stage2"]:
                cname = pipeline_config["containers"].get(key)
                if cname:
                    purge_container(cname)
            result_q.put(("progress", 20))

            result_q.put(("log", "--- Step 2: Uploading CSV ---"))
            raw_container = pipeline_config["containers"].get("raw", "incoming")
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


# ── Render helpers ─────────────────────────────────────────────────────────────

def render_header():
    st.markdown("""
    <div class="app-header">
        <div class="app-title">ADF Pipeline Orchestrator</div>
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
    dot = {"badge-idle": "#9ca3af", "badge-running": "#6366f1",
           "badge-ok": "#16a34a", "badge-fail": "#dc2626"}[cls]
    st.markdown(
        f'<span class="badge {cls}">'
        f'<span style="width:6px;height:6px;border-radius:50%;'
        f'background:{dot};display:inline-block;"></span>{label}</span><br><br>',
        unsafe_allow_html=True,
    )


def render_plan(config: dict, schema: dict):
    # Containers
    cols = st.columns(len(config["containers"]))
    for i, (role, name) in enumerate(config["containers"].items()):
        with cols[i]:
            st.metric(role.upper(), name)
    st.markdown("<br>", unsafe_allow_html=True)

    # Pipelines
    rows = "".join(
        f"<tr><td>{p['name']}</td>"
        f"<td><span class='pill pill-{p['type']}'>{p['type']}</span></td>"
        f"<td>{p['source_dataset']}</td><td>{p['sink_dataset']}</td></tr>"
        for p in config["pipelines"]
    )
    st.markdown(f"""
    <div class="card-label">Pipelines</div>
    <table class="plan-table">
      <thead><tr><th>Name</th><th>Type</th><th>Source</th><th>Sink</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Schema
    type_rows = "".join(
        f"<tr><td>{col}</td>"
        f"<td><span class='pill pill-{schema['inferred_types'].get(col,'string')}'>"
        f"{schema['inferred_types'].get(col,'string')}</span></td></tr>"
        for col in schema["columns"]
    )
    st.markdown(f"""
    <div class="card-label">
        CSV Schema — {len(schema['columns'])} columns &nbsp;·&nbsp;
        ~{schema['row_count']:,} rows &nbsp;·&nbsp; {schema['size_hint']}
    </div>
    <table class="plan-table">
      <thead><tr><th>Column</th><th>Type</th></tr></thead>
      <tbody>{type_rows}</tbody>
    </table>""", unsafe_allow_html=True)

    st.markdown(f"""
    <br>
    <div style="font-family:'JetBrains Mono',monospace;font-size:0.76rem;color:#6b7280;line-height:1.8;">
        <b style="color:#374151;">Execution order:</b> {' → '.join(config['execution_order'])}<br>
        <b style="color:#374151;">Reasoning:</b> {config.get('reasoning','N/A')}
    </div>""", unsafe_allow_html=True)


def render_logs():
    def colour(line: str) -> str:
        l = line.lower()
        if any(x in l for x in ["failed", "error", "abort", "invalid"]):
            return f'<span style="color:#f87171">{line}</span>'
        if any(x in l for x in ["succeeded", "created", "uploaded", "triggered",
                                  "verified", "ready", "obtained"]):
            return f'<span style="color:#4ade80">{line}</span>'
        if any(x in l for x in ["waiting", "retrying", "propagat",
                                  "timeout", "warn", "skipping", "purging"]):
            return f'<span style="color:#fbbf24">{line}</span>'
        if any(x in l for x in ["step", "---", "groq", "publishing",
                                  "authenticat", "setting"]):
            return f'<span style="color:#818cf8">{line}</span>'
        return f'<span style="color:#a5b4fc">{line}</span>'

    lines   = st.session_state.logs
    content = (
        '<span style="color:#475569">Waiting for pipeline to start...</span>'
        if not lines
        else "\n".join(colour(l) for l in lines)
    )
    st.markdown(f'<div class="log-box">{content}</div>', unsafe_allow_html=True)


# ── Main UI ────────────────────────────────────────────────────────────────────

render_header()
render_status_badge()


# ════════════════════════════════════════════════════════════
# STAGE: input
# ════════════════════════════════════════════════════════════
if st.session_state.stage == "input":

    left, right = st.columns([1.1, 1], gap="large")

    with left:
        st.markdown('<div class="card"><div class="card-label">Upload CSV</div>', unsafe_allow_html=True)
        uploaded = st.file_uploader(
            "upload_csv", type=["csv"],
            label_visibility="collapsed",
        )
        if uploaded:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            tmp.write(uploaded.read())
            tmp.flush(); tmp.close()
            st.session_state.csv_tmp_path = tmp.name
            schema = read_csv_schema(tmp.name)
            st.session_state.schema = schema
            st.markdown(
                f'<p style="font-family:JetBrains Mono,monospace;font-size:0.75rem;'
                f'color:#6b7280;margin-top:0.5rem;">'
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
            "pipeline_prompt", height=130,
            label_visibility="collapsed",
            key="prompt_text",
            placeholder=(
                "e.g. Clean nulls, uppercase the name column, "
                "filter rows where status = 1, and load to silver."
            ),
        )
        st.markdown("<br>", unsafe_allow_html=True)

        def _on_generate():
            prompt_val = st.session_state.prompt_text.strip()
            if not prompt_val or not st.session_state.csv_tmp_path:
                return
            py_dir = os.path.dirname(os.path.abspath(__file__))
            if py_dir not in sys.path:
                sys.path.insert(0, py_dir)
            from groq_brain import decide_pipeline_config
            config = decide_pipeline_config(st.session_state.schema, prompt_val)
            st.session_state.pipeline_config = config
            st.session_state.user_prompt     = prompt_val
            st.session_state.stage           = "plan"
            st.session_state.logs            = []

        can_plan = bool(
            st.session_state.csv_tmp_path
            and st.session_state.prompt_text.strip()
        )
        st.button("Generate Plan", disabled=not can_plan, on_click=_on_generate)
        st.markdown('</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# STAGE: plan
# ════════════════════════════════════════════════════════════
elif st.session_state.stage == "plan":

    st.markdown('<div class="card"><div class="card-label">Pipeline Plan</div>', unsafe_allow_html=True)
    render_plan(st.session_state.pipeline_config, st.session_state.schema)
    st.markdown('</div>', unsafe_allow_html=True)

    col_back, col_deploy = st.columns(2)
    with col_back:
        if st.button("Back"):
            st.session_state.stage = "input"
            st.rerun()
    with col_deploy:
        if st.button("Deploy to ADF"):
            st.session_state.stage    = "running"
            st.session_state.logs     = []
            st.session_state.progress = 0
            st.rerun()


# ════════════════════════════════════════════════════════════
# STAGE: running
# ════════════════════════════════════════════════════════════
elif st.session_state.stage == "running":

    st.markdown('<div class="card-label">Live Log</div>', unsafe_allow_html=True)
    log_ph  = st.empty()
    prog_ph = st.empty()

    if "_q" not in st.session_state:
        q: queue.Queue = queue.Queue()
        st.session_state._q = q
        t = threading.Thread(
            target=run_pipeline_thread,
            args=(
                st.session_state.csv_tmp_path,
                st.session_state.pipeline_config,
                st.session_state.schema,
                q,
            ),
            daemon=True,
        )
        t.start()

    q: queue.Queue = st.session_state._q
    done = False
    while not q.empty():
        kind, payload = q.get_nowait()
        if kind == "log":
            st.session_state.logs.append(payload)
        elif kind == "progress":
            st.session_state.progress = payload
        elif kind == "ok":
            st.session_state.pipeline_config = payload
            st.session_state.stage           = "done"
            st.session_state.pop("_q", None)
            done = True; break
        elif kind == "error":
            st.session_state.run_error = payload
            st.session_state.stage     = "failed"
            st.session_state.pop("_q", None)
            done = True; break

    with log_ph:
        render_logs()
    with prog_ph:
        st.progress(max(0, min(100, st.session_state.progress)) / 100)

    if not done:
        time.sleep(1.5)
        st.rerun()
    else:
        st.rerun()


# ════════════════════════════════════════════════════════════
# STAGE: done
# ════════════════════════════════════════════════════════════
elif st.session_state.stage == "done":

    st.success("Pipeline completed successfully.")

    left_col, right_col = st.columns([2, 1], gap="large")

    with left_col:
        st.markdown('<div class="card"><div class="card-label">Pipeline Summary</div>', unsafe_allow_html=True)
        render_plan(st.session_state.pipeline_config, st.session_state.schema)
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('<div class="card"><div class="card-label">Run Log</div>', unsafe_allow_html=True)
        render_logs()
        st.markdown('</div>', unsafe_allow_html=True)

    with right_col:
        st.markdown('<div class="card"><div class="card-label">Output</div>', unsafe_allow_html=True)

        config        = st.session_state.pipeline_config
        stage2        = config["containers"].get("stage2", "silver")
        sink_ds       = next((d for d in config["datasets"] if d.get("role") == "sink"),
                             {"filename": "output.csv", "container": stage2})
        sink_filename = sink_ds.get("filename", "output.csv")

        if st.session_state.output_csv is None:
            with st.spinner(f"Fetching from '{stage2}'..."):
                data, fname = fetch_output_from_blob(stage2, sink_filename)
                st.session_state.output_csv      = data
                st.session_state.output_filename = fname or sink_filename

        if st.session_state.output_csv:
            st.markdown(
                f'<div style="font-family:JetBrains Mono,monospace;font-size:0.76rem;'
                f'color:#6b7280;margin-bottom:1rem;line-height:1.8;">'
                f'Container: <b style="color:#111827">{stage2}</b><br>'
                f'File: <b style="color:#111827">{st.session_state.output_filename}</b><br>'
                f'Size: <b style="color:#111827">{len(st.session_state.output_csv):,} bytes</b>'
                f'</div>',
                unsafe_allow_html=True,
            )
            st.download_button(
                label=f"Download {st.session_state.output_filename}",
                data=st.session_state.output_csv,
                file_name=st.session_state.output_filename,
                mime="text/csv",
            )
        else:
            st.warning(
                f"Could not fetch output from '{stage2}' automatically. "
                "Download it from the Azure portal."
            )

        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Run New Pipeline"):
            for k in list(DEFAULTS.keys()) + ["_q"]:
                st.session_state.pop(k, None)
            st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# STAGE: failed
# ════════════════════════════════════════════════════════════
elif st.session_state.stage == "failed":

    st.error(f"Pipeline failed: {st.session_state.run_error}")

    st.markdown('<div class="card"><div class="card-label">Run Log</div>', unsafe_allow_html=True)
    render_logs()
    st.markdown('</div>', unsafe_allow_html=True)

    col_retry, col_new = st.columns(2)
    with col_retry:
        if st.button("Retry"):
            st.session_state.stage    = "running"
            st.session_state.logs     = []
            st.session_state.progress = 0
            st.session_state.pop("_q", None)
            st.rerun()
    with col_new:
        if st.button("Start Over"):
            for k in list(DEFAULTS.keys()) + ["_q"]:
                st.session_state.pop(k, None)
            st.rerun()