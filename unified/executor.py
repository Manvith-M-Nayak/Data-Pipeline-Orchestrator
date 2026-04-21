"""
Unified executor: drives the end-to-end pipeline.

Flow:
    1. Read schema, plan produced by groq_planner
    2. Create / purge blob containers, upload source CSV
    3. For every notebook stage, generate PySpark notebook and upload to workspace
    4. Create ADF linked services:
         - LS_Blob_Storage       (Azure Blob)
         - LS_Databricks         (Azure Databricks, new job cluster OR existing cluster)
    5. Create ADF datasets for every container
    6. Create ONE ADF pipeline containing all activities chained via dependsOn:
         - Copy Activity            for type="copy" stage
         - DatabricksNotebook Act.  for type="notebook" stages
       Pass storage_key + run_id + stage_name as notebook baseParameters
    7. Publish factory (wait for propagation)
    8. Trigger the pipeline, poll run status
    9. Return structured result
"""

import os
import time
import base64
import requests

from config import (
    AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET,
    AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP, AZURE_DATA_FACTORY,
    AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY,
    DATABRICKS_HOST, DATABRICKS_TOKEN,
    DATABRICKS_CLUSTER_ID, DATABRICKS_SPARK_VERSION, DATABRICKS_NODE_TYPE,
    DATABRICKS_NOTEBOOK_BASE,
)

from notebook_builder import build_notebook_source


ADF_API_VERSION = "2018-06-01"
UNIFIED_PIPELINE_NAME = "Unified_Orchestrator_Pipeline"
LS_BLOB_NAME = "LS_Blob_Storage"
LS_DBX_NAME  = "LS_Databricks"


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
    except Exception as e:
        print(f"   Purge of '{container_name}' failed: {e}")


def upload_csv(filepath: str, container_name: str) -> str:
    filename = os.path.basename(filepath)
    if filename.startswith("*") or not filename.lower().endswith(".csv"):
        raise ValueError(f"Invalid CSV filename '{filename}'")

    container = _blob_service_client().get_container_client(container_name)
    with open(filepath, "rb") as f:
        container.upload_blob(name=filename, data=f, overwrite=True)
    print(f"   '{filename}' uploaded to '{container_name}'")
    return filename


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
    if r.status_code not in (200,):
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
# ADF REST helpers
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
        pass  # already gone
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


# ────────────────────────────────────────────────────────────────────────────
# Linked services
# ────────────────────────────────────────────────────────────────────────────
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


def create_databricks_linked_service(token: str, recommended: dict) -> requests.Response:
    type_props = {
        "domain":     DATABRICKS_HOST.rstrip("/"),
        "accessToken": {
            "type":  "SecureString",
            "value": DATABRICKS_TOKEN,
        },
    }
    if DATABRICKS_CLUSTER_ID:
        type_props["existingClusterId"] = DATABRICKS_CLUSTER_ID
    else:
        num_workers = recommended.get("num_workers", 0)
        type_props["newClusterVersion"]  = DATABRICKS_SPARK_VERSION
        type_props["newClusterNodeType"] = DATABRICKS_NODE_TYPE

        if num_workers == 0:
            # Single-node cluster — driver only, no workers.
            # ADF requires explicit single-node tags or it rejects 0-worker clusters.
            type_props["newClusterNumOfWorker"] = "1"
            type_props["newClusterSparkConf"] = {
                "spark.master":                         "local[*, 4]",
                "spark.databricks.cluster.profile":     "singleNode",
            }
            type_props["newClusterCustomTags"] = {
                "ResourceClass": "SingleNode",
            }
        else:
            type_props["newClusterNumOfWorker"] = str(num_workers)

    body = {
        "properties": {
            "type": "AzureDatabricks",
            "typeProperties": type_props,
        }
    }
    return adf_put(token, "linkedservices", LS_DBX_NAME, body)


# ────────────────────────────────────────────────────────────────────────────
# Datasets
# ────────────────────────────────────────────────────────────────────────────
def create_dataset(token: str, ds_config: dict) -> requests.Response:
    role     = ds_config.get("role", "source")
    filename = ""
    if role == "sink":
        filename = "output.csv"

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


# ────────────────────────────────────────────────────────────────────────────
# Unified pipeline builder — ONE pipeline, N chained activities
# ────────────────────────────────────────────────────────────────────────────
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
        act["dependsOn"] = [{
            "activity":             prev_activity_name,
            "dependencyConditions": ["Succeeded"],
        }]
    return act


def _notebook_activity(stage: dict, notebook_path: str, prev_activity_name: str) -> dict:
    act = {
        "name": stage["name"],
        "type": "DatabricksNotebook",
        "linkedServiceName": {
            "referenceName": LS_DBX_NAME,
            "type":          "LinkedServiceReference",
        },
        "typeProperties": {
            "notebookPath":   notebook_path,
            "baseParameters": {
                "storage_key": AZURE_STORAGE_KEY,
                "run_id":      {"value": "@pipeline().RunId", "type": "Expression"},
                "stage_name":  stage["name"],
            },
        },
        "policy": {
            "timeout":            "0.02:00:00",
            "retry":              1,
            "retryIntervalInSeconds": 30,
            "secureOutput":       True,
            "secureInput":        False,
        },
    }
    if prev_activity_name:
        act["dependsOn"] = [{
            "activity":             prev_activity_name,
            "dependencyConditions": ["Succeeded"],
        }]
    return act


def create_unified_pipeline(token: str, pipeline_config: dict, notebook_paths: dict) -> requests.Response:
    activities = []
    prev = None
    for stage in pipeline_config["stages"]:
        if stage["type"] == "copy":
            activities.append(_copy_activity(stage, prev))
        elif stage["type"] == "notebook":
            nb_path = notebook_paths[stage["name"]]
            activities.append(_notebook_activity(stage, nb_path, prev))
        else:
            raise RuntimeError(f"Unknown stage type '{stage['type']}' in '{stage['name']}'")
        prev = stage["name"]

    body = {"properties": {"activities": activities}}
    return adf_put(token, "pipelines", UNIFIED_PIPELINE_NAME, body)


# ────────────────────────────────────────────────────────────────────────────
# Trigger + poll
# ────────────────────────────────────────────────────────────────────────────
def publish_factory():
    print("   No Git integration — resources live immediately via REST")
    print("   Waiting 15s for ADF propagation...")
    time.sleep(15)


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
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"   Status check failed: {r.status_code} {r.text[:200]}")
            time.sleep(poll_interval)
            continue
        body = r.json()
        status = body.get("status")
        print(f"   run {run_id[:8]}... status={status}")
        if status in ("Succeeded", "Failed", "Cancelled"):
            return {"status": status, "run": body}
        time.sleep(poll_interval)
    return {"status": "Timeout", "run": None}


# ────────────────────────────────────────────────────────────────────────────
# End-to-end driver
# ────────────────────────────────────────────────────────────────────────────
def execute_pipeline(csv_path: str, pipeline_config: dict, schema: dict) -> dict:
    print("\n--- Step A: Azure authentication ---")
    token = get_azure_token()

    print("\n--- Step B: Creating blob containers ---")
    for name in pipeline_config["containers_to_create"]:
        create_blob_container(token, name)

    print("\n--- Step C: Purging all containers ---")
    for name in pipeline_config["containers_to_create"]:
        purge_container(name)

    raw_container = pipeline_config["containers_to_create"][0]
    print(f"\n--- Step D: Uploading CSV to '{raw_container}' ---")
    upload_csv(csv_path, raw_container)
    if not check_blob_has_rows(raw_container):
        return {"status": "failed", "message": f"Upload verification failed on '{raw_container}'"}

    print("\n--- Step E: Uploading notebooks to Databricks workspace ---")
    ensure_workspace_dir(DATABRICKS_NOTEBOOK_BASE)
    notebook_paths = {}
    for stage in pipeline_config["stages"]:
        if stage["type"] != "notebook":
            continue
        source = build_notebook_source(stage, AZURE_STORAGE_ACCOUNT)
        wpath = f"{DATABRICKS_NOTEBOOK_BASE.rstrip('/')}/{stage['name']}"
        upload_notebook(wpath, source)
        notebook_paths[stage["name"]] = wpath

    print("\n--- Step F: Creating ADF linked services ---")
    adf_delete(token, "linkedservices", LS_DBX_NAME)
    create_blob_linked_service(token)
    create_databricks_linked_service(token, pipeline_config.get("recommended_settings", {}))

    print("\n--- Step G: Creating ADF datasets ---")
    for ds in pipeline_config["datasets"]:
        r = create_dataset(token, ds)
        if r.status_code not in (200, 201):
            return {"status": "failed", "message": f"Dataset '{ds['name']}' creation failed"}

    print("\n--- Step H: Creating unified ADF pipeline ---")
    r = create_unified_pipeline(token, pipeline_config, notebook_paths)
    if r.status_code not in (200, 201):
        return {"status": "failed", "message": "Pipeline creation failed"}

    print("\n--- Step I: Publishing factory ---")
    publish_factory()

    print("\n--- Step J: Triggering pipeline ---")
    run_id = trigger_pipeline(token, UNIFIED_PIPELINE_NAME)
    if not run_id:
        return {"status": "failed", "message": "Pipeline trigger failed"}

    print("\n--- Step K: Monitoring run ---")
    result = check_pipeline_status(token, run_id)

    if result["status"] == "Succeeded":
        print("\n   Pipeline run succeeded.")
        return {
            "status":  "ok",
            "run_id":  run_id,
            "result":  result["run"],
            "stages":  [s["name"] for s in pipeline_config["stages"]],
        }

    return {
        "status":  "failed",
        "run_id":  run_id,
        "message": f"Pipeline finished with status={result['status']}",
        "result":  result["run"],
    }
