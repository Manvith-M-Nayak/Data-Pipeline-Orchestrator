"""
Unified executor: drives the end-to-end pipeline.

Flow:
    1. Azure auth
    2. Create / purge blob containers, upload source CSV
    3. For every notebook stage, generate PySpark notebook and upload to
       Databricks workspace via Workspace API
    4. If the plan has Copy stages → create ADF resources (blob linked
       service + datasets + copy-only pipeline) and run them via ADF
    5. For every notebook stage → use Databricks Jobs API 2.1 to create
       an ephemeral job with NO cluster spec (= serverless compute in
       serverless-only workspaces), run it, poll, then delete the job
    6. Return structured result
"""

import os
import time
import base64
import traceback
import requests
from concurrent.futures import ThreadPoolExecutor

from config import (
    AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET,
    AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP, AZURE_DATA_FACTORY,
    AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY,
    DATABRICKS_HOST, DATABRICKS_TOKEN,
    DATABRICKS_NOTEBOOK_BASE,
)

from .notebook_builder import build_notebook_source


ADF_API_VERSION       = "2018-06-01"
COPY_PIPELINE_NAME    = "Orchestrator_Copy_Pipeline"
LS_BLOB_NAME          = "LS_Blob_Storage"
MAX_PARALLEL_STAGES   = 3   # matches resource agent MAX_CONCURRENT (student tier)


# ────────────────────────────────────────────────────────────────────────────
# Azure auth
# ────────────────────────────────────────────────────────────────────────────
def get_azure_token() -> str:
    url = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/token"
    data = {
        "grant_type":    "client_credentials",
        "client_id":     AZURE_CLIENT_ID,
        "client_secret": AZURE_CLIENT_SECRET,
        "resource":      "https://management.azure.com/",
    }
    r = requests.post(url, data=data, timeout=30)
    body = r.json()
    if "access_token" not in body:
        raise RuntimeError(f"Azure token request failed: {body}")
    print("   Azure access token obtained")
    return body["access_token"]


# ────────────────────────────────────────────────────────────────────────────
# Azure Blob Storage helpers
# ────────────────────────────────────────────────────────────────────────────
def _blob_service_client():
    from azure.storage.blob import BlobServiceClient
    conn = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={AZURE_STORAGE_ACCOUNT};"
        f"AccountKey={AZURE_STORAGE_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    return BlobServiceClient.from_connection_string(conn)


def create_blob_container(token: str, container_name: str):
    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.Storage/storageAccounts/{AZURE_STORAGE_ACCOUNT}"
        f"/blobServices/default/containers/{container_name}?api-version=2021-09-01"
    )
    r = requests.put(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"properties": {"publicAccess": "None"}},
        timeout=30,
    )
    if r.status_code in (200, 201):
        print(f"   Container '{container_name}' created")
    elif r.status_code == 409:
        print(f"   Container '{container_name}' already exists")
    else:
        print(f"   Container '{container_name}' failed -> {r.status_code}: {r.text[:200]}")


def purge_container(container_name: str):
    try:
        container = _blob_service_client().get_container_client(container_name)
        blobs = list(container.list_blobs())
        if not blobs:
            print(f"   '{container_name}' already empty")
            return
        print(f"   Purging {len(blobs)} blob(s) from '{container_name}'...")
        for b in blobs:
            container.delete_blob(b.name)
    except Exception:
        traceback.print_exc()
        raise


INPUT_EXTENSIONS = (".csv", ".json", ".jsonl", ".ndjson")


def upload_input_file(filepath: str, container_name: str) -> str:
    filename = os.path.basename(filepath)
    if filename.startswith("*") or not filename.lower().endswith(INPUT_EXTENSIONS):
        raise ValueError(
            f"Invalid input filename '{filename}' — expected one of {INPUT_EXTENSIONS}"
        )
    container = _blob_service_client().get_container_client(container_name)
    with open(filepath, "rb") as f:
        container.upload_blob(name=filename, data=f, overwrite=True)
    print(f"   '{filename}' uploaded to '{container_name}'")
    return filename


# Backwards-compatible alias — external callers may still import upload_csv
upload_csv = upload_input_file


def check_blob_has_rows(container_name: str) -> bool:
    try:
        container = _blob_service_client().get_container_client(container_name)
        blobs = list(container.list_blobs())
        valid = [b for b in blobs if b.size and b.size > 0 and b.name != "*.csv"]
        if not valid:
            print(f"   Container '{container_name}' has no valid blobs")
            return False
        for b in valid:
            print(f"   {b.name} — {b.size:,} bytes")
        return True
    except Exception as e:
        print(f"   Blob check error on '{container_name}': {e}")
        return True


# ────────────────────────────────────────────────────────────────────────────
# Databricks workspace: upload notebook
# ────────────────────────────────────────────────────────────────────────────
def _dbx_url(path: str) -> str:
    from urllib.parse import urlparse, urlunparse
    p = urlparse(DATABRICKS_HOST)
    base = urlunparse((p.scheme, p.netloc, "", "", "", ""))
    return base.rstrip("/") + path


def _dbx_headers() -> dict:
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}


def ensure_workspace_dir(path: str):
    r = requests.post(
        _dbx_url("/api/2.0/workspace/mkdirs"),
        headers=_dbx_headers(),
        json={"path": path},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Workspace mkdirs '{path}' failed: {r.status_code} {r.text[:200]}")
    print(f"   Workspace dir ready: {path}")


def upload_notebook(workspace_path: str, source: str):
    r = requests.post(
        _dbx_url("/api/2.0/workspace/import"),
        headers=_dbx_headers(),
        json={
            "path":      workspace_path,
            "format":    "SOURCE",
            "language":  "PYTHON",
            "overwrite": True,
            "content":   base64.b64encode(source.encode("utf-8")).decode("ascii"),
        },
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(
            f"Notebook upload to '{workspace_path}' failed: {r.status_code} {r.text[:300]}"
        )
    print(f"   Notebook uploaded: {workspace_path}")


# ────────────────────────────────────────────────────────────────────────────
# Databricks Jobs API 2.1
# ────────────────────────────────────────────────────────────────────────────
try:
    from config import DATABRICKS_CLUSTER_ID as _DBX_CLUSTER_ID
except ImportError:
    _DBX_CLUSTER_ID = ""


def dbx_create_job(job_name: str, notebook_path: str, parameters: dict) -> int:
    """Create ephemeral job. Returns job_id.

    Cluster priority:
      1. DATABRICKS_CLUSTER_ID set in config → existing interactive cluster
      2. Otherwise → no cluster spec (serverless in serverless-only workspaces)
    """
    task: dict = {
        "task_key": "main",
        "notebook_task": {
            "notebook_path":   notebook_path,
            "base_parameters": parameters,
            "source":          "WORKSPACE",
        },
    }
    if _DBX_CLUSTER_ID:
        task["existing_cluster_id"] = _DBX_CLUSTER_ID
    # else: no cluster key → Databricks uses serverless automatically

    body = {"name": job_name, "tasks": [task]}
    r = requests.post(_dbx_url("/api/2.1/jobs/create"), headers=_dbx_headers(), json=body, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Jobs create failed ({r.status_code}): {r.text[:500]}")
    job_id = r.json()["job_id"]
    print(f"   DBX job created: job_id={job_id}  cluster={'existing:'+_DBX_CLUSTER_ID if _DBX_CLUSTER_ID else 'serverless'}")
    return job_id


def dbx_run_job(job_id: int) -> int:
    """Trigger run-now. Returns run_id."""
    r = requests.post(
        _dbx_url("/api/2.1/jobs/run-now"),
        headers=_dbx_headers(),
        json={"job_id": job_id},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Jobs run-now failed ({r.status_code}): {r.text[:500]}")
    run_id = r.json()["run_id"]
    print(f"   DBX run triggered: run_id={run_id}")
    return run_id


def _dbx_fetch_error(run_body: dict) -> str:
    """Extract actual notebook error from a failed run via jobs/runs/get-output."""
    errors = []
    for task in run_body.get("tasks", []):
        task_run_id = task.get("run_id")
        if not task_run_id:
            continue
        r = requests.get(
            _dbx_url(f"/api/2.1/jobs/runs/get-output?run_id={task_run_id}"),
            headers=_dbx_headers(),
            timeout=20,
        )
        if r.status_code == 200:
            out = r.json()
            err   = out.get("error", "")
            trace = out.get("error_trace", "")
            if err:
                errors.append(err)
            if trace:
                errors.append(trace[:600])
    return "\n".join(errors) if errors else ""


def dbx_poll_run(run_id: int, poll_interval: int = 10, timeout: int = 1800) -> dict:
    """Poll jobs/runs/get until TERMINATED. Returns {result_state, state_message, error, run_page_url}."""
    url = _dbx_url(f"/api/2.1/jobs/runs/get?run_id={run_id}")
    start = time.time()
    final_body: dict = {}
    consecutive_errors = 0
    while time.time() - start < timeout:
        try:
            r = requests.get(url, headers=_dbx_headers(), timeout=30)
        except requests.RequestException as e:
            consecutive_errors += 1
            print(f"   Poll network error #{consecutive_errors}: {e}")
            if consecutive_errors >= 5:
                return {
                    "result_state":  "POLL_ERROR",
                    "state_message": f"Network error polling run {run_id}: {e}",
                    "error":         str(e),
                    "run_page_url":  "",
                    "run_id":        run_id,
                }
            time.sleep(poll_interval)
            continue

        if r.status_code != 200:
            consecutive_errors += 1
            print(f"   Poll HTTP {r.status_code} error #{consecutive_errors}: {r.text[:200]}")
            if consecutive_errors >= 5:
                return {
                    "result_state":  "POLL_ERROR",
                    "state_message": f"HTTP {r.status_code} polling run {run_id}: {r.text[:300]}",
                    "error":         f"HTTP {r.status_code}: {r.text[:400]}",
                    "run_page_url":  "",
                    "run_id":        run_id,
                }
            time.sleep(poll_interval)
            continue

        consecutive_errors = 0
        body        = r.json()
        state       = body.get("state", {})
        life_cycle  = state.get("life_cycle_state", "")
        result_state = state.get("result_state", "")
        print(f"   run {run_id} life_cycle={life_cycle} result={result_state}")
        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            final_body = body
            break
        time.sleep(poll_interval)

    if not final_body:
        return {"result_state": "TIMEOUT", "state_message": "Timed out waiting for run to complete", "error": "", "run_page_url": "", "run_id": run_id}

    state = final_body.get("state", {})
    result_state = state.get("result_state", "TIMEOUT")
    state_message = state.get("state_message", "")

    # For failures, fetch the actual notebook error (traceback) from task output
    error_detail = ""
    if result_state not in ("SUCCESS",):
        error_detail = _dbx_fetch_error(final_body)

    combined_msg = state_message
    if error_detail:
        combined_msg = f"{state_message}\n\n{error_detail}".strip()

    print(f"   run {run_id} finished: {result_state}")
    if error_detail:
        print(f"   error detail:\n{error_detail[:400]}")

    return {
        "result_state":  result_state,
        "state_message": combined_msg,
        "error":         error_detail,
        "run_page_url":  final_body.get("run_page_url", ""),
        "run_id":        run_id,
    }


def dbx_delete_job(job_id: int):
    """Delete ephemeral job after run completes."""
    r = requests.post(
        _dbx_url("/api/2.1/jobs/delete"),
        headers=_dbx_headers(),
        json={"job_id": job_id},
        timeout=30,
    )
    if r.status_code == 200:
        print(f"   DBX job {job_id} deleted")
    else:
        print(f"   DBX job delete {job_id} -> {r.status_code} (non-fatal)")


# ────────────────────────────────────────────────────────────────────────────
# ADF REST helpers (used only for Copy activities)
# ────────────────────────────────────────────────────────────────────────────
def _adf_url(resource_type: str, name: str) -> str:
    return (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{AZURE_DATA_FACTORY}"
        f"/{resource_type}/{name}?api-version={ADF_API_VERSION}"
    )


def adf_delete(token: str, resource_type: str, name: str):
    r = requests.delete(
        _adf_url(resource_type, name),
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    if r.status_code in (200, 204):
        print(f"   Deleted stale {resource_type}/{name}")
    elif r.status_code == 404:
        pass
    else:
        print(f"   Delete {resource_type}/{name} -> {r.status_code}")


def adf_put(token: str, resource_type: str, name: str, body: dict) -> requests.Response:
    r = requests.put(
        _adf_url(resource_type, name),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=60,
    )
    if r.status_code in (200, 201):
        print(f"   Created {resource_type}/{name}")
    else:
        print(f"   Failed {resource_type}/{name} -> {r.status_code}: {r.text[:300]}")
    return r


def create_blob_linked_service(token: str) -> requests.Response:
    body = {
        "properties": {
            "type": "AzureBlobStorage",
            "typeProperties": {
                "connectionString": (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={AZURE_STORAGE_ACCOUNT};"
                    f"AccountKey={AZURE_STORAGE_KEY};"
                    f"EndpointSuffix=core.windows.net"
                ),
            },
        }
    }
    return adf_put(token, "linkedservices", LS_BLOB_NAME, body)


def create_dataset(token: str, ds_config: dict) -> requests.Response:
    role     = ds_config.get("role", "source")
    filename = "output.csv" if role == "sink" else ""
    location = {"type": "AzureBlobStorageLocation", "container": ds_config["container"]}
    if filename:
        location["fileName"] = filename
    body = {
        "properties": {
            "type": "DelimitedText",
            "linkedServiceName": {
                "referenceName": LS_BLOB_NAME,
                "type":          "LinkedServiceReference",
            },
            "typeProperties": {
                "location":         location,
                "columnDelimiter":  ",",
                "quoteChar":        '"',
                "firstRowAsHeader": True,
                "encodingName":     "UTF-8",
            },
        }
    }
    return adf_put(token, "datasets", ds_config["name"], body)


def _copy_activity(stage: dict, prev_activity_name: str = None) -> dict:
    act = {
        "name": stage["name"],
        "type": "Copy",
        "typeProperties": {
            "source": {
                "type":               "DelimitedTextSource",
                "wildcardFolderPath": "",
                "wildcardFileName":   "*.csv",
                "recursive":          True,
                "formatSettings": {
                    "type":       "DelimitedTextReadSettings",
                    "quoteChar":  '"',
                    "escapeChar": '"',
                },
            },
            "sink": {
                "type": "DelimitedTextSink",
                "storeSettings": {
                    "type":     "AzureBlobStorageWriteSettings",
                    "fileName": "staged.csv",
                },
                "formatSettings": {
                    "type":          "DelimitedTextWriteSettings",
                    "fileExtension": ".csv",
                },
                "copyBehavior": "MergeFiles",
            },
            "enableStaging":        False,
            "dataIntegrationUnits": int(stage.get("diu", 2)),
        },
        "inputs":  [{"referenceName": stage["source_dataset"], "type": "DatasetReference"}],
        "outputs": [{"referenceName": stage["sink_dataset"],   "type": "DatasetReference"}],
    }
    if prev_activity_name:
        act["dependsOn"] = [{"activity": prev_activity_name, "dependencyConditions": ["Succeeded"]}]
    return act


def create_copy_pipeline(token: str, copy_stages: list) -> requests.Response:
    activities, prev = [], None
    for stage in copy_stages:
        activities.append(_copy_activity(stage, prev))
        prev = stage["name"]
    body = {"properties": {"activities": activities}}
    return adf_put(token, "pipelines", COPY_PIPELINE_NAME, body)


def trigger_pipeline(token: str, pipeline_name: str) -> str:
    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{AZURE_DATA_FACTORY}"
        f"/pipelines/{pipeline_name}/createRun?api-version={ADF_API_VERSION}"
    )
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={},
        timeout=30,
    )
    if r.status_code not in (200, 202):
        print(f"   Trigger failed -> {r.status_code}: {r.text[:300]}")
        return None
    run_id = r.json().get("runId")
    print(f"   Pipeline '{pipeline_name}' triggered (runId={run_id})")
    return run_id


def check_pipeline_status(token: str, run_id: str, poll_interval: int = 10, timeout: int = 1800) -> dict:
    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{AZURE_DATA_FACTORY}"
        f"/pipelineruns/{run_id}?api-version={ADF_API_VERSION}"
    )
    start = time.time()
    while time.time() - start < timeout:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        if r.status_code != 200:
            print(f"   Status check failed: {r.status_code} {r.text[:200]}")
            time.sleep(poll_interval)
            continue
        body   = r.json()
        status = body.get("status")
        print(f"   ADF run {run_id[:8]}... status={status}")
        if status in ("Succeeded", "Failed", "Cancelled"):
            return {"status": status, "run": body}
        time.sleep(poll_interval)
    return {"status": "Timeout", "run": None}


# ────────────────────────────────────────────────────────────────────────────
# End-to-end driver
# ────────────────────────────────────────────────────────────────────────────
def execute_pipeline(csv_path: str, pipeline_config: dict, schema: dict, progress=None) -> dict:
    def _step(msg: str, dbx_run_id: int = None):
        print(f"\n--- {msg} ---")
        if progress:
            progress(msg, dbx_run_id)

    run_tag = str(int(time.time()))

    stages          = pipeline_config.get("stages", [])
    copy_stages     = [s for s in stages if s.get("type") == "copy"]
    notebook_stages = [s for s in stages if s.get("type") == "notebook"]

    # Validate stage references up front — a malformed config should fail
    # with a clear message before any cloud resources are touched.
    if not pipeline_config.get("containers_to_create"):
        return {"status": "failed", "message": "Config has no containers_to_create"}
    for s in copy_stages:
        if not s.get("source_dataset") or not s.get("sink_dataset"):
            return {"status": "failed",
                    "message": f"Copy stage '{s.get('name', '?')}' missing source_dataset/sink_dataset"}
    for s in notebook_stages:
        if not s.get("source_container") or not s.get("sink_container"):
            return {"status": "failed",
                    "message": f"Notebook stage '{s.get('name', '?')}' missing source_container/sink_container"}

    _step("Authenticating with Azure")
    token = get_azure_token()

    _step("Creating storage containers")
    for name in pipeline_config["containers_to_create"]:
        create_blob_container(token, name)
    for name in pipeline_config["containers_to_create"]:
        purge_container(name)

    raw_container = pipeline_config["containers_to_create"][0]
    input_ext = os.path.splitext(csv_path)[1].lower().lstrip(".") or "csv"
    _step(f"Uploading {input_ext.upper()} input to '{raw_container}'")
    upload_input_file(csv_path, raw_container)
    if not check_blob_has_rows(raw_container):
        return {"status": "failed", "message": f"Upload verification failed on '{raw_container}'"}

    notebook_paths: dict = {}
    if notebook_stages:
        _step(f"Uploading {len(notebook_stages)} notebook(s) to Databricks workspace")
        ensure_workspace_dir(DATABRICKS_NOTEBOOK_BASE)
        for stage in notebook_stages:
            source = build_notebook_source(stage, AZURE_STORAGE_ACCOUNT)
            wpath  = f"{DATABRICKS_NOTEBOOK_BASE.rstrip('/')}/{stage['name']}"
            upload_notebook(wpath, source)
            notebook_paths[stage["name"]] = wpath

    # ── ADF path (Copy activities only) ──────────────────────────────────────
    adf_run_id = None
    if copy_stages:
        _step("Creating ADF blob linked service and datasets")
        create_blob_linked_service(token)
        copy_dataset_names = {stage.get("source_dataset") for stage in copy_stages} | \
                             {stage.get("sink_dataset")   for stage in copy_stages}
        defined_ds = {ds.get("name") for ds in pipeline_config.get("datasets", [])}
        missing_ds = copy_dataset_names - defined_ds
        if missing_ds:
            return {"status": "failed",
                    "message": f"Copy stage references datasets not defined in config: {sorted(missing_ds)}"}
        for ds in pipeline_config.get("datasets", []):
            if ds["name"] in copy_dataset_names:
                r = create_dataset(token, ds)
                if r.status_code not in (200, 201):
                    return {"status": "failed", "message": f"Dataset '{ds['name']}' creation failed"}

        _step("Creating and triggering ADF copy pipeline")
        r = create_copy_pipeline(token, copy_stages)
        if r.status_code not in (200, 201):
            return {"status": "failed", "message": "Copy pipeline creation failed"}

        time.sleep(5)
        adf_run_id = trigger_pipeline(token, COPY_PIPELINE_NAME)
        if not adf_run_id:
            return {"status": "failed", "message": "Copy pipeline trigger failed"}

        _step("Waiting for ADF copy pipeline to complete")
        result = check_pipeline_status(token, adf_run_id)
        if result["status"] != "Succeeded":
            return {
                "status":  "failed",
                "run_id":  adf_run_id,
                "message": f"Copy pipeline finished with status={result['status']}",
                "result":  result["run"],
            }

    # ── Databricks Jobs API path (notebook stages, serverless) ───────────────
    # Stages run group by group per config["execution_groups"]; stages inside
    # one group run concurrently (independent fan-out branches). Falls back to
    # fully sequential when the config carries no groups.
    if notebook_stages:
        nb_by_name = {s["name"]: s for s in notebook_stages}
        raw_groups = pipeline_config.get("execution_groups") or [[s["name"]] for s in notebook_stages]
        groups, seen = [], set()
        for g in raw_groups:
            if not isinstance(g, list):
                g = [g]
            keep = [n for n in g if n in nb_by_name and n not in seen]
            seen.update(keep)
            if keep:
                groups.append(keep)
        for s in notebook_stages:                 # anything the groups missed
            if s["name"] not in seen:
                groups.append([s["name"]])

        def _run_stage(stage: dict) -> tuple:
            """Create job, run, poll, delete. Returns (stage_name, poll_result)."""
            nb_path    = notebook_paths[stage["name"]]
            parameters = {
                "storage_key": AZURE_STORAGE_KEY,
                "run_id":      f"{run_tag}-{stage['name']}",
                "stage_name":  stage["name"],
            }
            job_name = f"orchestrator-{stage['name']}-{run_tag}"
            job_id   = dbx_create_job(job_name, nb_path, parameters)
            try:
                dbx_run_id = dbx_run_job(job_id)
                # Expose the live Databricks run_id so the monitor can track it
                _step(f"Monitoring Databricks run {dbx_run_id} (stage: {stage['name']})", dbx_run_id=dbx_run_id)
                return stage["name"], dbx_poll_run(dbx_run_id)
            finally:
                dbx_delete_job(job_id)

        for gi, group in enumerate(groups, 1):
            tag = " (parallel)" if len(group) > 1 else ""
            _step(f"Running stage group {gi}/{len(groups)}{tag}: {', '.join(group)}")
            if len(group) == 1:
                results = [_run_stage(nb_by_name[group[0]])]
            else:
                with ThreadPoolExecutor(max_workers=min(len(group), MAX_PARALLEL_STAGES)) as pool:
                    results = list(pool.map(_run_stage, [nb_by_name[n] for n in group]))

            failures = [(n, p) for n, p in results if p["result_state"] != "SUCCESS"]
            if failures:
                name, poll = failures[0]
                extra = f" ({len(failures)} stage(s) failed in this group)" if len(failures) > 1 else ""
                return {
                    "status":  "failed",
                    "run_id":  adf_run_id or f"dbx-{run_tag}",
                    "message": (
                        f"Stage '{name}' failed "
                        f"(result_state={poll['result_state']}): "
                        f"{poll['state_message']}{extra}"
                    ),
                    "result":  poll,
                }

    # Last container in the plan is the final sink
    containers = pipeline_config.get("containers_to_create", [])
    sink_container = containers[-1] if containers else None

    return {
        "status":         "ok",
        "run_id":         adf_run_id or f"dbx-{run_tag}",
        "stages":         [s["name"] for s in stages],
        "sink_container": sink_container,
        "result": {
            "copy_stages":     len(copy_stages),
            "notebook_stages": len(notebook_stages),
        },
    }
