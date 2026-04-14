import requests
import json
import re
import base64
import time
import datetime
import os
from config import (
    DATABRICKS_HOST, DATABRICKS_TOKEN,
    DATABRICKS_CLUSTER_ID, DATABRICKS_SPARK_VERSION, DATABRICKS_NODE_TYPE,
)


# ============================================================
# AUTH HEADERS
# ============================================================
def _headers() -> dict:
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }


def _url(path: str) -> str:
    # Strip any browser query params (?o=...&l=...) that users sometimes copy
    # from the Databricks UI URL bar. Only keep scheme + host.
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(DATABRICKS_HOST)
    base = urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
    return base.rstrip("/") + path


# ============================================================
# CONNECTION CHECK — call before any pipeline operation
# ============================================================
def check_connection() -> tuple:
    """
    Validate DATABRICKS_HOST + DATABRICKS_TOKEN.
    Returns (ok: bool, message: str).
    Uses Jobs API — works on Standard plan without DBFS enabled.
    """
    try:
        r = requests.get(
            _url("/api/2.1/jobs/list"),
            headers=_headers(),
            params={"limit": 1},
            timeout=15,
        )
        if r.status_code == 200:
            return True, "Connected"
        if r.status_code == 401:
            return False, (
                "Authentication failed (401). "
                "Check DATABRICKS_TOKEN in config.py — it may be expired or invalid."
            )
        if r.status_code == 403:
            return False, (
                "Permission denied (403). "
                "Your token may lack Jobs API permissions. "
                "Regenerate your Personal Access Token in Databricks UI → User Settings → Developer."
            )
        if r.status_code == 404:
            return False, (
                f"Workspace not found (404). "
                f"DATABRICKS_HOST '{DATABRICKS_HOST}' is likely wrong. "
                f"Expected format: https://adb-XXXXXXXX.X.azuredatabricks.net"
            )
        return False, f"Unexpected response {r.status_code}: {r.text[:200]}"
    except requests.exceptions.ConnectionError:
        return False, (
            f"Cannot connect to '{DATABRICKS_HOST}'. "
            "Check the URL is correct and the workspace is reachable."
        )
    except requests.exceptions.Timeout:
        return False, f"Connection timed out reaching '{DATABRICKS_HOST}'."
    except Exception as e:
        return False, f"Connection error: {e}"


# ============================================================
# DBFS: CREATE DIRECTORY
# ============================================================
def dbfs_mkdirs(path: str):
    r = requests.post(_url("/api/2.0/dbfs/mkdirs"), headers=_headers(), json={"path": path})
    if r.status_code == 200:
        print(f"   DBFS dir ready: {path}")
    elif r.status_code in (401, 403):
        raise Exception(
            f"DBFS auth error {r.status_code} on mkdirs '{path}'. "
            "Check DATABRICKS_TOKEN in config.py."
        )
    elif r.status_code == 404:
        raise Exception(
            f"DBFS 404 on mkdirs '{path}'. "
            f"DATABRICKS_HOST '{DATABRICKS_HOST}' is likely wrong. "
            "Expected: https://adb-XXXXXXXX.X.azuredatabricks.net"
        )
    else:
        raise Exception(f"DBFS mkdirs failed '{path}' -> {r.status_code}: {r.text[:200]}")


# ============================================================
# DBFS: LIST FILES
# ============================================================
def dbfs_list(path: str) -> list:
    r = requests.get(_url(f"/api/2.0/dbfs/list"), headers=_headers(), params={"path": path})
    if r.status_code == 200:
        return r.json().get("files", [])
    elif r.status_code == 404:
        return []
    else:
        print(f"   DBFS list error '{path}' -> {r.status_code}")
        return []


# ============================================================
# DBFS: CHECK PATH HAS CSV FILES
# ============================================================
def dbfs_has_files(path: str) -> bool:
    files = dbfs_list(path)
    valid = [
        f for f in files
        if f.get("path", "").endswith(".csv") and f.get("file_size", 0) > 0
    ]
    if not valid:
        print(f"   DBFS '{path}' has no valid CSV files")
        return False
    for f in valid:
        print(f"   Found: {f['path']} ({f.get('file_size', 0):,} bytes)")
    return True


# ============================================================
# DBFS: DELETE A PATH
# ============================================================
def dbfs_delete_path(path: str, recursive: bool = True):
    r = requests.post(
        _url("/api/2.0/dbfs/delete"),
        headers=_headers(),
        json={"path": path, "recursive": recursive},
    )
    if r.status_code == 200:
        print(f"   DBFS deleted: {path}")
    elif r.status_code == 404:
        pass  # already gone
    else:
        print(f"   DBFS delete failed '{path}' -> {r.status_code}: {r.text[:100]}")


# ============================================================
# DBFS: PURGE ALL CSV FILES IN A DIRECTORY
# ============================================================
def dbfs_purge(path: str):
    files = dbfs_list(path)
    csv_files = [f for f in files if f.get("path", "").endswith(".csv")]
    if not csv_files:
        print(f"   DBFS '{path}' already empty")
        return
    print(f"   Purging {len(csv_files)} file(s) from '{path}'...")
    for f in csv_files:
        dbfs_delete_path(f["path"], recursive=False)


# ============================================================
# DBFS: UPLOAD LOCAL FILE (chunked, handles large files)
# ============================================================
def dbfs_upload(local_path: str, dbfs_path: str):
    CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB per chunk

    # Open handle
    r = requests.post(
        _url("/api/2.0/dbfs/open"),
        headers=_headers(),
        json={"path": dbfs_path, "overwrite": True, "flags": "CREATE|OVERWRITE|WRITE"},
    )
    if r.status_code != 200:
        raise Exception(f"DBFS open failed: {r.status_code}: {r.text}")
    handle = r.json()["handle"]

    # Stream chunks
    with open(local_path, "rb") as f:
        chunk_count = 0
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            encoded = base64.b64encode(chunk).decode("utf-8")
            ar = requests.post(
                _url("/api/2.0/dbfs/add-block"),
                headers=_headers(),
                json={"handle": handle, "data": encoded},
            )
            if ar.status_code != 200:
                raise Exception(f"DBFS add-block failed: {ar.status_code}: {ar.text}")
            chunk_count += 1

    # Close handle
    cr = requests.post(_url("/api/2.0/dbfs/close"), headers=_headers(), json={"handle": handle})
    if cr.status_code == 200:
        size = os.path.getsize(local_path)
        print(f"   Uploaded '{os.path.basename(local_path)}' -> '{dbfs_path}' ({size:,} bytes)")
    else:
        raise Exception(f"DBFS close failed: {cr.status_code}: {cr.text}")


# ============================================================
# DBFS: READ FILE AS BYTES (chunked, for output download)
# ============================================================
def dbfs_read_bytes(dbfs_path: str, file_size: int) -> bytes:
    CHUNK_SIZE = 1 * 1024 * 1024
    data = bytearray()
    offset = 0
    while offset < file_size:
        length = min(CHUNK_SIZE, file_size - offset)
        r = requests.get(
            _url("/api/2.0/dbfs/read"),
            headers=_headers(),
            params={"path": dbfs_path, "offset": offset, "length": length},
        )
        if r.status_code != 200:
            raise Exception(f"DBFS read failed: {r.status_code}: {r.text[:200]}")
        chunk = base64.b64decode(r.json()["data"])
        data.extend(chunk)
        offset += length
    return bytes(data)


# ============================================================
# DBFS: FETCH OUTPUT CSV (handles partitioned Spark output)
# ============================================================
def fetch_output_from_dbfs(dbfs_path: str) -> tuple:
    """
    Returns (csv_bytes, filename).
    Spark writes output in part files — merge them if multiple.
    """
    import io
    import csv as csv_mod

    files = dbfs_list(dbfs_path)
    # Part files look like: part-00000-*.csv or output.csv/part-00000
    csv_files = sorted(
        [f for f in files if f.get("file_size", 0) > 0 and not f["path"].endswith("/_SUCCESS")],
        key=lambda f: f["path"],
    )
    if not csv_files:
        return None, ""

    if len(csv_files) == 1:
        raw = dbfs_read_bytes(csv_files[0]["path"], csv_files[0]["file_size"])
        return raw, os.path.basename(csv_files[0]["path"])

    # Merge multiple parts
    merged = []
    header = None
    for f in csv_files:
        content = dbfs_read_bytes(f["path"], f["file_size"]).decode("utf-8")
        reader = csv_mod.reader(io.StringIO(content))
        rows = list(reader)
        if not rows:
            continue
        if header is None:
            header = rows[0]
            merged.append(header)
        # Skip header row for subsequent parts
        merged.extend(r for r in rows[1:] if r)

    if merged:
        out = io.StringIO()
        writer = csv_mod.writer(out)
        writer.writerows(merged)
        return out.getvalue().encode("utf-8"), "merged_output.csv"

    return None, ""


# ============================================================
# PYSPARK EXPRESSION CONVERTER (ADF → PySpark)
# ============================================================
def _adf_to_pyspark_expr(expr: str) -> str:
    """
    Convert ADF Data Flow expression syntax to PySpark expression string.
    Handles the most common ADF functions output by the Groq brain.
    """
    expr = expr.strip()
    subs = [
        # Type casts
        (r"toInteger\((\w+)\)",      r'col("\1").cast("int")'),
        (r"toDouble\((\w+)\)",       r'col("\1").cast("double")'),
        (r"toString\((\w+)\)",       r'col("\1").cast("string")'),
        (r"toLong\((\w+)\)",         r'col("\1").cast("long")'),
        # String
        (r"upper\((\w+)\)",          r'upper(col("\1"))'),
        (r"lower\((\w+)\)",          r'lower(col("\1"))'),
        (r"trim\((\w+)\)",           r'trim(col("\1"))'),
        (r"ltrim\((\w+)\)",          r'ltrim(col("\1"))'),
        (r"rtrim\((\w+)\)",          r'rtrim(col("\1"))'),
        (r"initCap\((\w+)\)",        r'initcap(col("\1"))'),
        (r"length\((\w+)\)",         r'length(col("\1"))'),
        (r"concat\(([^)]+)\)",       r'concat(\1)'),
        (r"substring\((\w+),\s*(\d+),\s*(\d+)\)", r'substring(col("\1"), \2, \3)'),
        # Time
        (r"currentTimestamp\(\)",    "current_timestamp()"),
        (r"currentDate\(\)",         "current_date()"),
        (r"year\((\w+)\)",           r'year(col("\1"))'),
        (r"month\((\w+)\)",          r'month(col("\1"))'),
        (r"dayOfMonth\((\w+)\)",     r'dayofmonth(col("\1"))'),
        (r"hour\((\w+)\)",           r'hour(col("\1"))'),
        (r"minute\((\w+)\)",         r'minute(col("\1"))'),
        (r"second\((\w+)\)",         r'second(col("\1"))'),
        # Math
        (r"round\((\w+)\)",          r'round(col("\1"))'),
        (r"floor\((\w+)\)",          r'floor(col("\1"))'),
        (r"ceil\((\w+)\)",           r'ceil(col("\1"))'),
        (r"abs\((\w+)\)",            r'abs(col("\1"))'),
        (r"sqrt\((\w+)\)",           r'sqrt(col("\1"))'),
        # Null
        (r"isNull\((\w+)\)",         r'col("\1").isNull()'),
        (r"iifNull\((\w+),\s*(.+)\)", r'coalesce(col("\1"), lit(\2))'),
    ]
    for pattern, replacement in subs:
        expr = re.sub(pattern, replacement, expr)

    # Already PySpark-style (contains col() or current_timestamp)
    return expr


def _adf_filter_to_pyspark(filter_expr: str) -> str:
    """Convert ADF filter expression to PySpark filter string."""
    expr = filter_expr.strip()

    patterns = [
        # equals(toInteger(col), val)
        (r"^equals\(toInteger\((\w+)\),\s*(-?\d+)\)$",     r'col("\1").cast("int") == \2'),
        (r"^notEquals\(toInteger\((\w+)\),\s*(-?\d+)\)$",  r'col("\1").cast("int") != \2'),
        (r"^greater\(toInteger\((\w+)\),\s*(-?\d+)\)$",    r'col("\1").cast("int") > \2'),
        (r"^less\(toInteger\((\w+)\),\s*(-?\d+)\)$",       r'col("\1").cast("int") < \2'),
        (r"^greaterOrEqual\(toInteger\((\w+)\),\s*(-?\d+)\)$", r'col("\1").cast("int") >= \2'),
        (r"^lessOrEqual\(toInteger\((\w+)\),\s*(-?\d+)\)$",    r'col("\1").cast("int") <= \2'),
        # equals(col, val) (no cast)
        (r"^equals\((\w+),\s*'([^']+)'\)$",  r'col("\1") == "\2"'),
        (r"^equals\((\w+),\s*(-?\d+)\)$",    r'col("\1") == \2'),
        (r"^notEquals\((\w+),\s*'([^']+)'\)$", r'col("\1") != "\2"'),
        # isNull
        (r"^isNull\((\w+)\)$",   r'col("\1").isNull()'),
        # bare: col == 1 or col != 0
        (r"^(\w+)\s*(==|!=|>=|<=|>|<)\s*(-?\d+)$", r'col("\1") \2 \3'),
        (r"^(\w+)\s*(==|!=)\s*'([^']+)'$",          r'col("\1") \2 "\3"'),
    ]
    for pattern, replacement in patterns:
        m = re.match(pattern, expr, re.IGNORECASE)
        if m:
            return re.sub(pattern, replacement, expr, flags=re.IGNORECASE)

    # Already PySpark-style (contains col() or cast())
    if "col(" in expr or ".cast(" in expr:
        return expr

    return expr  # pass through as-is


# ============================================================
# SCRIPT BUILDER: Copy Pipeline
# No DBFS — CSV embedded as base64. Output returned via notebook.exit().
# ============================================================
def build_copy_script(csv_data_b64: str, shuffle_partitions: int = 4) -> str:
    return f"""# Databricks notebook source
import base64, io
import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "{shuffle_partitions}")

# COMMAND ----------
CSV_DATA_B64 = "{csv_data_b64}"
csv_text = base64.b64decode(CSV_DATA_B64).decode("utf-8")
pdf = pd.read_csv(io.StringIO(csv_text))
df = spark.createDataFrame(pdf)
count = df.count()
print(f"Copy: rows read: {{count}}")

# COMMAND ----------
out_csv = df.toPandas().to_csv(index=False)
out_b64 = base64.b64encode(out_csv.encode("utf-8")).decode("utf-8")
print(f"Copy complete. Rows: {{count}}")
dbutils.notebook.exit(out_b64)
"""


# ============================================================
# SCRIPT BUILDER: Transform Pipeline
# No DBFS — CSV embedded as base64. Output returned via notebook.exit().
# ============================================================
def build_transform_script(
    csv_data_b64: str,
    transformations: list,
    filter_condition: str,
    columns: list,
    inferred_types: dict,
    shuffle_partitions: int = 4,
) -> str:
    derived = []
    active_filter = filter_condition

    for t in transformations:
        if "=" not in t:
            continue
        col_name, expr = t.split("=", 1)
        col_name = col_name.strip()
        expr = expr.strip()

        # Detect filter-intent entries from transformations list
        if col_name.lower() == "filter":
            if active_filter is None:
                active_filter = _adf_filter_to_pyspark(expr)
            continue

        # Convert ADF expression to PySpark
        pyspark_expr = _adf_to_pyspark_expr(expr)
        derived.append((col_name, pyspark_expr))

    # Always include processed_time
    if not any(d[0] == "processed_time" for d in derived):
        derived.append(("processed_time", "current_timestamp()"))

    # Build filter line
    filter_lines = ""
    if active_filter:
        pyspark_filter = _adf_filter_to_pyspark(active_filter)
        filter_lines = (
            f'\nprint("Applying filter: {pyspark_filter.replace(chr(34), chr(39))}")\n'
            f"df = df.filter({pyspark_filter})\n"
            f'print(f"Rows after filter: {{df.count()}}")\n'
        )

    # Build withColumn lines
    derive_lines = "\n".join(
        f'df = df.withColumn("{col}", {expr})'
        for col, expr in derived
    )

    return f"""# Databricks notebook source
import base64, io
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, lit, upper, lower, trim, ltrim, rtrim, initcap,
    concat, substring, length, regexp_replace, coalesce,
    current_timestamp, current_date, year, month, dayofmonth,
    hour, minute, second, when, to_date, to_timestamp,
)
try:
    from pyspark.sql.functions import round, floor, ceil, abs, sqrt
except ImportError:
    pass

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "{shuffle_partitions}")

# COMMAND ----------
CSV_DATA_B64 = "{csv_data_b64}"
csv_text = base64.b64decode(CSV_DATA_B64).decode("utf-8")
pdf = pd.read_csv(io.StringIO(csv_text))
df = spark.createDataFrame(pdf)
print(f"Transform: rows read: {{df.count()}}")

# COMMAND ----------
{filter_lines}
print("Applying transformations...")
{derive_lines}

# COMMAND ----------
out_csv = df.toPandas().to_csv(index=False)
out_b64 = base64.b64encode(out_csv.encode("utf-8")).decode("utf-8")
print(f"Transform complete. Rows: {{df.count()}}")
dbutils.notebook.exit(out_b64)
"""


# ============================================================
# WORKSPACE: UPLOAD NOTEBOOK (Standard-plan workaround — no DBFS needed)
# ============================================================
def workspace_mkdir(path: str):
    """Create a folder in the Databricks Workspace."""
    r = requests.post(
        _url("/api/2.0/workspace/mkdirs"),
        headers=_headers(),
        json={"path": path},
    )
    if r.status_code not in (200, 400):  # 400 = already exists, fine
        raise Exception(f"workspace mkdirs failed '{path}': {r.status_code}: {r.text[:200]}")


def workspace_upload(path: str, content: str):
    """
    Upload a Python source file as a Databricks notebook.
    path: workspace path without leading /Workspace, e.g. /pipeline-scripts/foo
    content: raw Python source code (may contain # Databricks notebook source header)
    """
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    r = requests.post(
        _url("/api/2.0/workspace/import"),
        headers=_headers(),
        json={
            "path": path,
            "language": "PYTHON",
            "format": "SOURCE",
            "content": encoded,
            "overwrite": True,
        },
    )
    if r.status_code != 200:
        raise Exception(f"Workspace import failed '{path}': {r.status_code}: {r.text[:300]}")
    print(f"   Workspace notebook uploaded: {path}")


def workspace_delete(path: str):
    """Delete a workspace object (best-effort, silent on 404)."""
    r = requests.post(
        _url("/api/2.0/workspace/delete"),
        headers=_headers(),
        json={"path": path, "recursive": True},
    )
    if r.status_code == 200:
        print(f"   Workspace deleted: {path}")


# ============================================================
# RUN OUTPUT: RETRIEVE NOTEBOOK EXIT VALUE
# ============================================================
def get_task_run_id(job_run_id: int) -> int:
    """
    Jobs API returns a multi-task run ID. get-output requires the individual
    task's run_id. Fetch the run and extract the first task's run_id.
    """
    r = requests.get(
        _url("/api/2.1/jobs/runs/get"),
        headers=_headers(),
        params={"run_id": job_run_id},
        timeout=30,
    )
    if r.status_code != 200:
        return job_run_id  # fallback
    tasks = r.json().get("tasks", [])
    if tasks and tasks[0].get("run_id"):
        return tasks[0]["run_id"]
    return job_run_id


def get_notebook_output(run_id: int) -> str:
    """
    Retrieve the value passed to dbutils.notebook.exit() for a completed run.
    Returns the exit string, or "" on failure.
    """
    task_run_id = get_task_run_id(run_id)
    r = requests.get(
        _url("/api/2.1/jobs/runs/get-output"),
        headers=_headers(),
        params={"run_id": task_run_id},
        timeout=30,
    )
    if r.status_code != 200:
        print(f"   get-output HTTP {r.status_code}: {r.text[:200]}")
        return ""
    data = r.json()
    return data.get("notebook_output", {}).get("result", "")


# ============================================================
# SCRIPT UPLOAD — Workspace-backed (no DBFS)
# ============================================================
def upload_script(script_content: str, script_name: str) -> str:
    """Upload a PySpark notebook to Workspace. Returns workspace path."""
    script_dir = "/databricks-pipeline-scripts"
    workspace_mkdir(script_dir)
    # Strip .py extension — workspace notebooks don't use it
    nb_name = script_name.replace(".py", "")
    workspace_path = f"{script_dir}/{nb_name}"
    workspace_upload(workspace_path, script_content)
    return workspace_path


# ============================================================
# CLUSTER CONFIG BUILDER
# ============================================================
def _cluster_config(num_workers: int, shuffle_partitions: int) -> dict:
    """Return cluster config dict for a job task."""
    if DATABRICKS_CLUSTER_ID:
        return {"existing_cluster_id": DATABRICKS_CLUSTER_ID}
    cluster = {
        "spark_version": DATABRICKS_SPARK_VERSION,
        "node_type_id": DATABRICKS_NODE_TYPE,
        "spark_conf": {
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
        },
    }
    if num_workers == 0:
        # Single-node cluster — driver only, no workers. Uses 1 VM = minimum quota.
        cluster["num_workers"] = 0
        cluster["spark_conf"]["spark.databricks.cluster.profile"] = "singleNode"
        cluster["custom_tags"] = {"ResourceClass": "SingleNode"}
    else:
        cluster["num_workers"] = num_workers
    return {"new_cluster": cluster}


# ============================================================
# JOB: CREATE AND RUN
# ============================================================
def create_and_run_job(job_name: str, script_dbfs_path: str, num_workers: int, shuffle_partitions: int) -> tuple:
    """
    Create a one-off Databricks job and immediately run it.
    script_dbfs_path: workspace notebook path (e.g. /databricks-pipeline-scripts/foo)
    Returns (job_id, run_id).
    """
    cluster_cfg = _cluster_config(num_workers, shuffle_partitions)

    task = {
        "task_key": "pipeline_task",
        "notebook_task": {
            "notebook_path": script_dbfs_path,
        },
    }
    task.update(cluster_cfg)

    body = {
        "name": job_name,
        "tasks": [task],
        "max_concurrent_runs": 1,
    }

    r = requests.post(_url("/api/2.1/jobs/create"), headers=_headers(), json=body)
    if r.status_code != 200:
        raise Exception(f"Job create failed: {r.status_code}: {r.text}")
    job_id = r.json()["job_id"]
    print(f"   Job created: {job_name} (id={job_id})")

    rr = requests.post(_url("/api/2.1/jobs/run-now"), headers=_headers(), json={"job_id": job_id})
    if rr.status_code != 200:
        raise Exception(f"Job run failed: {rr.status_code}: {rr.text}")
    run_id = rr.json()["run_id"]
    print(f"   Run triggered: run_id={run_id}")

    return job_id, run_id


# ============================================================
# JOB: DELETE (cleanup after run)
# ============================================================
def delete_job(job_id: int):
    r = requests.post(_url("/api/2.1/jobs/delete"), headers=_headers(), json={"job_id": job_id})
    if r.status_code == 200:
        print(f"   Job {job_id} deleted")
    else:
        print(f"   Job delete failed: {r.status_code}: {r.text[:100]}")


# ============================================================
# RUN: POLL STATUS UNTIL TERMINAL STATE
# ============================================================
def check_run_status(run_id: int, max_wait: int = 1800) -> dict:
    """
    Poll /api/2.1/jobs/runs/get until terminal state.
    Terminal: TERMINATED | SKIPPED | INTERNAL_ERROR
    Returns {"status": "Succeeded" | "Failed" | "Timeout", "details": {...}}
    """
    POLL_INTERVAL = 10
    elapsed = 0

    while elapsed < max_wait:
        try:
            r = requests.get(
                _url("/api/2.1/jobs/runs/get"),
                headers=_headers(),
                params={"run_id": run_id},
                timeout=30,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"   Network error polling run {run_id}: {e} — retrying...")
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
            continue

        if r.status_code != 200:
            print(f"   Status check HTTP {r.status_code}: {r.text[:100]}")
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
            continue

        data = r.json()
        life_cycle = data.get("state", {}).get("life_cycle_state", "")
        result_state = data.get("state", {}).get("result_state", "")
        state_msg = data.get("state", {}).get("state_message", "")

        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if result_state == "SUCCESS":
                print(f"   Run {run_id} succeeded")
                return {"status": "Succeeded", "details": data}
            else:
                print(f"   Run {run_id} FAILED — {result_state}: {state_msg}")
                # Pull task-level error detail for better diagnostics
                task_run_id = get_task_run_id(run_id)
                try:
                    out_r = requests.get(
                        _url("/api/2.1/jobs/runs/get-output"),
                        headers=_headers(),
                        params={"run_id": task_run_id},
                        timeout=20,
                    )
                    if out_r.status_code == 200:
                        err = out_r.json().get("error", "")
                        trace = out_r.json().get("error_trace", "")
                        if err:
                            print(f"   Task error: {err}")
                        if trace:
                            print(f"   Traceback:\n{trace[:1500]}")
                        state_msg = err or state_msg
                except Exception:
                    pass
                return {"status": "Failed", "details": data, "message": state_msg}

        print(f"   Run {run_id} -> {life_cycle} ({elapsed}s elapsed)")
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    print(f"   Timeout waiting for run {run_id}")
    return {"status": "Timeout", "run_id": run_id}


# ============================================================
# MONITORING: LIST RECENT JOB RUNS
# ============================================================
def list_recent_runs(limit: int = 20) -> list:
    """
    Return recent pipeline runs from the Databricks Jobs API.
    Filters to jobs created by this tool (name starts with 'DB_Pipeline_').
    """
    r = requests.get(
        _url("/api/2.1/jobs/runs/list"),
        headers=_headers(),
        params={"limit": limit * 3, "active_only": "false"},
        timeout=30,
    )
    if r.status_code != 200:
        print(f"   list_recent_runs failed: {r.status_code}: {r.text[:200]}")
        return []

    runs = r.json().get("runs", [])

    # Filter to our pipeline jobs and format
    formatted = []
    for run in runs:
        job_name = run.get("run_name", "") or run.get("job_id", "")
        life = run.get("state", {}).get("life_cycle_state", "Unknown")
        result = run.get("state", {}).get("result_state", "")
        msg = run.get("state", {}).get("state_message", "")

        # Map to unified status
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            status = "Succeeded" if result == "SUCCESS" else "Failed"
        elif life in ("RUNNING", "PENDING"):
            status = "InProgress"
        else:
            status = life.title()

        start_ms = run.get("start_time", 0)
        end_ms = run.get("end_time", 0)
        duration_s = (end_ms - start_ms) / 1000 if end_ms and start_ms else None

        started_str = ""
        if start_ms:
            started_str = datetime.datetime.fromtimestamp(start_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

        formatted.append({
            "pipeline": str(job_name),
            "run_id": run.get("run_id"),
            "job_id": run.get("job_id"),
            "status": status,
            "duration": _fmt_duration(duration_s),
            "started": started_str,
            "message": msg,
        })

        if len(formatted) >= limit:
            break

    return formatted


# ============================================================
# FORMAT DURATION
# ============================================================
def _fmt_duration(seconds) -> str:
    if seconds is None:
        return "N/A"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {sec}s"
    if m:
        return f"{m}m {sec}s"
    return f"{sec}s"


# ============================================================
# MAIN EXECUTOR: orchestrates entire pipeline run
# Called by both CLI (main.py) and dashboard thread
# ============================================================
def execute_pipeline(
    csv_path: str,
    pipeline_config: dict,
    schema: dict,
    log_fn=print,
    progress_fn=None,
) -> dict:
    """
    Full pipeline execution — no DBFS required (Standard plan compatible).

    Data flow:
      1. Read local CSV → base64-encode in memory
      2. For each pipeline in execution_order:
         a. Embed current CSV data (b64) into a Databricks notebook
         b. Upload notebook to Workspace
         c. Create job + trigger run
         d. Poll until done
         e. Retrieve output CSV from notebook.exit() value
         f. That output becomes input for the next stage
      3. Return final CSV bytes in result dict

    No DBFS paths, no FileStore, no file browser needed.
    """
    def log(msg):
        log_fn(msg)

    def prog(pct):
        if progress_fn:
            progress_fn(pct)

    created_jobs = []
    uploaded_notebooks = []
    try:
        # --- Load raw CSV into memory ---
        log("Reading input CSV")
        with open(csv_path, "rb") as f:
            raw_bytes = f.read()
        current_csv_b64 = base64.b64encode(raw_bytes).decode("utf-8")
        log(f"CSV loaded: {len(raw_bytes):,} bytes")
        prog(15)

        # --- Execute pipelines in order ---
        n = len(pipeline_config["execution_order"])
        final_csv_bytes = raw_bytes  # fallback if no stages run

        for i, pl_name in enumerate(pipeline_config["execution_order"]):
            pl_cfg = next((p for p in pipeline_config["pipelines"] if p["name"] == pl_name), None)
            if pl_cfg is None:
                raise Exception(f"Pipeline '{pl_name}' not found in config")

            num_workers = 0  # single-node to stay within Azure quota (6 cores, 2 free)
            shuffle_partitions = pl_cfg.get("shuffle_partitions", 4)
            pl_type = pl_cfg.get("type", "copy")

            log(f"Building {pl_type} notebook: {pl_name}")

            ts = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            script_name = f"{pl_name}_{ts}.py"

            if pl_type == "copy":
                script = build_copy_script(current_csv_b64, shuffle_partitions)
            else:
                pl_cfg["inferred_types"] = schema.get("inferred_types", {})
                script = build_transform_script(
                    csv_data_b64=current_csv_b64,
                    transformations=pl_cfg.get("transformations", []),
                    filter_condition=pl_cfg.get("filter_condition"),
                    columns=schema.get("columns", []),
                    inferred_types=pl_cfg.get("inferred_types", {}),
                    shuffle_partitions=shuffle_partitions,
                )

            script_path = upload_script(script, script_name)
            uploaded_notebooks.append(script_path)
            prog(15 + int(50 * i / n))

            log(f"Creating job + triggering run: {pl_name}")
            job_name = f"DB_Pipeline_{pl_name}_{ts}"
            job_id, run_id = create_and_run_job(job_name, script_path, num_workers, shuffle_partitions)
            created_jobs.append(job_id)

            log(f"Waiting for run {run_id} ({pl_name})")
            result = check_run_status(run_id)

            if result["status"] != "Succeeded":
                msg = result.get("message", result["status"])
                raise Exception(f"Pipeline '{pl_name}' {result['status']}: {msg}")

            # Fetch output CSV from notebook exit value
            log(f"Fetching output for '{pl_name}'")
            out_b64 = get_notebook_output(run_id)
            if out_b64:
                try:
                    final_csv_bytes = base64.b64decode(out_b64)
                    current_csv_b64 = out_b64  # feed into next stage
                    log(f"Output fetched: {len(final_csv_bytes):,} bytes")
                except Exception as decode_err:
                    log(f"Output decode warning: {decode_err}")
            else:
                log(f"Warning: no output from '{pl_name}', passing input to next stage")

            prog(15 + int(50 * (i + 1) / n))
            log(f"Pipeline '{pl_name}' succeeded")

        prog(100)
        return {
            "status": "ok",
            "config": pipeline_config,
            "stage_paths": {},          # kept for API compat — no DBFS paths used
            "output_csv_bytes": final_csv_bytes,
            "output_csv_name": "output.csv",
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        # Cleanup uploaded notebooks (scripts only, not jobs)
        for nb_path in uploaded_notebooks:
            try:
                workspace_delete(nb_path)
            except Exception:
                pass
