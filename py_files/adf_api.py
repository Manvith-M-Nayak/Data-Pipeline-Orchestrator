import requests
import json
import re
import time
import datetime
from config import *


# ============================================================
# MAP: Compute type to ADF expected values
# ============================================================
def map_compute_type(compute_type: str) -> str:
    mapping = {
        "Cost Optimized": "General",
        "Performance Optimized": "MemoryOptimized"
    }
    return mapping.get(compute_type, compute_type)


# ============================================================
# AUTH: Get Azure Access Token
# ============================================================
def get_access_token() -> str:
    url = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/token"
    payload = {
        "grant_type":    "client_credentials",
        "client_id":     AZURE_CLIENT_ID,
        "client_secret": AZURE_CLIENT_SECRET,
        "resource":      "https://management.azure.com/"
    }
    r = requests.post(url, data=payload)
    data = r.json()
    if "access_token" not in data:
        raise Exception(f"Failed to get access token: {data}")
    print("Azure access token obtained")
    return data["access_token"]


# ============================================================
# BASE: PUT any ADF resource
# ============================================================
def adf_put(token: str, resource_type: str, name: str, body: dict) -> requests.Response:
    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{AZURE_DATA_FACTORY}"
        f"/{resource_type}/{name}?api-version=2018-06-01"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.put(url, headers=headers, json=body)
    if r.status_code in [200, 201]:
        print(f"   Created {resource_type}/{name}")
    else:
        print(f"   Failed {resource_type}/{name} -> {r.status_code}: {r.text}")
    return r


# ============================================================
# DELETE any ADF resource
# ============================================================
def delete_adf_resource(token: str, resource_type: str, name: str):
    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{AZURE_DATA_FACTORY}"
        f"/{resource_type}/{name}?api-version=2018-06-01"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.delete(url, headers=headers)
    if r.status_code not in [200, 204, 404]:
        print(f"   Could not delete {resource_type}/{name} -> {r.status_code}: {r.text[:200]}")


# ============================================================
# PUBLISH: Wait for ADF to register all resources
#
# NOTE: When ADF is used WITHOUT Git integration (which is the
# case here — no "Set up code repository" was configured), all
# REST API PUT calls write directly to the live layer. There is
# no separate draft/publish step and the /publishAll endpoint
# does not exist (returns 404).
#
# With Git integration the flow is: REST -> draft -> publishAll -> live.
# Without Git integration the flow is: REST -> live (immediate).
#
# We simply wait a few seconds to let ADF finish internal
# propagation before triggering runs.
# ============================================================
def publish_factory(token: str):
    print("   No Git integration — resources are live immediately via REST API")
    print("   Waiting 15s for ADF internal resource propagation...")
    time.sleep(15)
    print("   Ready to trigger pipelines")


# ============================================================
# BLOB: Create a container in Azure Storage
# ============================================================
def create_blob_container(container_name: str):
    token_url = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/token"
    payload = {
        "grant_type":    "client_credentials",
        "client_id":     AZURE_CLIENT_ID,
        "client_secret": AZURE_CLIENT_SECRET,
        "resource":      "https://management.azure.com/"
    }
    token_data = requests.post(token_url, data=payload).json()
    if "access_token" not in token_data:
        raise Exception(f"Failed to get access token: {token_data}")
    token = token_data["access_token"]

    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.Storage/storageAccounts/{AZURE_STORAGE_ACCOUNT}"
        f"/blobServices/default/containers/{container_name}?api-version=2021-09-01"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.put(url, headers=headers, json={"properties": {"publicAccess": "None"}})
    if r.status_code in [200, 201]:
        print(f"   Container '{container_name}' created")
    elif r.status_code == 409:
        print(f"   Container '{container_name}' already exists")
    else:
        print(f"   Container '{container_name}' failed -> {r.status_code}: {r.text}")


# ============================================================
# BLOB: Purge all blobs from a container
# ============================================================
def purge_container(container_name: str):
    try:
        from azure.storage.blob import BlobServiceClient
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={AZURE_STORAGE_ACCOUNT};"
            f"AccountKey={AZURE_STORAGE_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        client    = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(container_name)
        blobs = list(container.list_blobs())
        if not blobs:
            print(f"   '{container_name}' is already empty")
            return
        print(f"   Purging {len(blobs)} blob(s) from '{container_name}'...")
        for blob in blobs:
            container.delete_blob(blob.name)
    except ImportError:
        print("   azure-storage-blob not installed — skipping purge")
    except Exception as e:
        print(f"   Purge failed for '{container_name}': {e}")


# ============================================================
# BLOB: Upload CSV — purge container first, then upload only
#       the current file.
#
# Every run wipes ALL existing blobs from the container before
# uploading the new CSV. This guarantees the copy pipeline
# never merges stale files from previous runs into staged.csv.
# ============================================================
def upload_csv(filepath: str, container_name: str) -> str:
    import os
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        print("   azure-storage-blob not installed -> pip install azure-storage-blob")
        print(f"   Manually upload '{filepath}' to container '{container_name}'")
        return os.path.basename(filepath)

    filename = os.path.basename(filepath)

    if not filename or filename.startswith("*") or not filename.lower().endswith(".csv"):
        raise ValueError(
            f"Invalid blob filename '{filename}'. "
            f"Provide the actual CSV file path, not a wildcard pattern."
        )

    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={AZURE_STORAGE_ACCOUNT};"
        f"AccountKey={AZURE_STORAGE_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    client    = BlobServiceClient.from_connection_string(conn_str)
    container = client.get_container_client(container_name)

    existing = list(container.list_blobs())
    if existing:
        print(f"   Purging {len(existing)} existing blob(s) from '{container_name}'...")
        for blob in existing:
            container.delete_blob(blob.name)

    with open(filepath, "rb") as f:
        container.upload_blob(name=filename, data=f, overwrite=True)
    print(f"   '{filename}' uploaded to container '{container_name}'")
    return filename


# ============================================================
# BLOB: Verify container has real data before running dataflow
# ============================================================
def check_blob_has_rows(container_name: str) -> bool:
    try:
        from azure.storage.blob import BlobServiceClient
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={AZURE_STORAGE_ACCOUNT};"
            f"AccountKey={AZURE_STORAGE_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        client    = BlobServiceClient.from_connection_string(conn_str)
        container = client.get_container_client(container_name)
        blobs     = list(container.list_blobs())

        if not blobs:
            print(f"   Container '{container_name}' is empty")
            return False

        valid_blobs = 0
        for blob in blobs:
            if blob.name == "*.csv":
                print(f"   Stale wildcard blob '*.csv' found in '{container_name}'")
            elif blob.size == 0:
                print(f"   '{blob.name}' is 0 bytes — skipping")
            else:
                print(f"   '{blob.name}' — {blob.size:,} bytes")
                valid_blobs += 1

        if valid_blobs == 0:
            print(f"   No valid blobs in '{container_name}'")
            return False
        return True

    except ImportError:
        print("   azure-storage-blob not installed — skipping blob check")
        return True
    except Exception as e:
        print(f"   Blob check error: {e} — continuing anyway")
        return True


# ============================================================
# LINKED SERVICE: ADF <-> Azure Blob Storage
# ============================================================
def create_linked_service(token: str) -> requests.Response:
    body = {
        "properties": {
            "type": "AzureBlobStorage",
            "typeProperties": {
                "connectionString": (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={AZURE_STORAGE_ACCOUNT};"
                    f"AccountKey={AZURE_STORAGE_KEY};"
                    f"EndpointSuffix=core.windows.net"
                )
            }
        }
    }
    return adf_put(token, "linkedservices", "LS_Blob_Storage", body)


# ============================================================
# DATASET: Dynamically created from Groq's dataset config
#
# Source datasets use "*.csv" (wildcard read).
# Sink datasets must never use a wildcard — ADF will write
# a literal file called "*.csv" which breaks subsequent reads.
# Sink filename is enforced as "output.csv".
# Intermediate datasets are written by the copy pipeline as
# "staged.csv" and read by the dataflow source from the same name.
# ============================================================
def create_dataset(token: str, ds_config: dict) -> requests.Response:
    role     = ds_config.get("role", "source")
    filename = ds_config.get("filename", "*.csv")

    if role == "sink" and (not filename or filename.startswith("*")):
        filename = "output.csv"
        print(f"   Sink dataset '{ds_config['name']}': filename set to 'output.csv'")

    if role == "intermediate" and (not filename or filename.startswith("*")):
        filename = "staged.csv"
        print(f"   Intermediate dataset '{ds_config['name']}': filename set to 'staged.csv'")

    body = {
        "properties": {
            "type": "DelimitedText",
            "linkedServiceName": {
                "referenceName": "LS_Blob_Storage",
                "type":          "LinkedServiceReference"
            },
            "typeProperties": {
                "location": {
                    "type":      "AzureBlobStorageLocation",
                    "container": ds_config["container"],
                    "fileName":  filename
                },
                "columnDelimiter":  ",",
                "quoteChar":        '"',
                "firstRowAsHeader": True,
                "encodingName":     "UTF-8"
            }
        }
    }
    return adf_put(token, "datasets", ds_config["name"], body)


# ============================================================
# PIPELINE: Copy Activity (raw -> stage1)
#
# The sink dataset may carry fileName="*.csv" because it is also
# used as a dataflow source (wildcard read). When ADF executes
# a copy sink with fileName="*.csv" it creates a literal blob
# named "*.csv". To prevent this, we override the fileName at
# the pipeline level via storeSettings.fileName = "staged.csv".
# This takes precedence over the dataset-level fileName without
# changing the dataset definition.
# ============================================================
def create_copy_pipeline(token: str, pipeline_config: dict) -> requests.Response:
    body = {
        "properties": {
            "activities": [
                {
                    "name": "CopyActivity",
                    "type": "Copy",
                    "typeProperties": {
                        "source": {
                            "type":               "DelimitedTextSource",
                            "wildcardFolderPath": "",
                            "wildcardFileName":   "*.csv",
                            "recursive":          True,
                            "formatSettings": {
                                "type":       "DelimitedTextReadSettings",
                                "quoteChar":  "\"",
                                "escapeChar": "\""
                            }
                        },
                        "sink": {
                            "type": "DelimitedTextSink",
                            "storeSettings": {
                                "type":     "AzureBlobStorageWriteSettings",
                                "fileName": "staged.csv"
                            },
                            "formatSettings": {
                                "type":          "DelimitedTextWriteSettings",
                                "fileExtension": ".csv"
                            },
                            "copyBehavior": "MergeFiles"
                        },
                        "enableStaging":        False,
                        "parallelCopies":       pipeline_config.get("parallel_copies", 4),
                        "dataIntegrationUnits": pipeline_config.get("diu", 4)
                    },
                    "inputs":  [{"referenceName": pipeline_config["source_dataset"], "type": "DatasetReference"}],
                    "outputs": [{"referenceName": pipeline_config["sink_dataset"],   "type": "DatasetReference"}]
                }
            ]
        }
    }
    return adf_put(token, "pipelines", pipeline_config["name"], body)


# ============================================================
# ADF EXPRESSION FUNCTION ALLOWLIST
# ============================================================
ADF_FUNCTIONS = {
    'true', 'false', 'null',
    'currentTimestamp', 'currentDate', 'currentUTC',
    'toDate', 'toTimestamp', 'toString', 'toInteger', 'toLong',
    'toDouble', 'toFloat', 'toBoolean', 'toDecimal',
    'trim', 'ltrim', 'rtrim', 'upper', 'lower', 'initCap',
    'concat', 'substring', 'length', 'replace', 'regexReplace',
    'split', 'startsWith', 'endsWith', 'contains', 'instr',
    'iifNull', 'iif', 'isNull', 'isNaN', 'isInteger', 'isString',
    'coalesce', 'decode', 'case',
    'round', 'floor', 'ceil', 'abs', 'sqrt', 'mod', 'power',
    'year', 'month', 'dayOfMonth', 'hour', 'minute', 'second',
    'addDays', 'addMonths', 'dateDiff', 'dayOfWeek', 'dayOfYear',
    'md5', 'sha1', 'sha2', 'uuid',
    'array', 'map', 'struct',
    'byName', 'byPosition', 'byIndex',
    'asc', 'desc', 'rowNumber', 'rank', 'denseRank',
    'sum', 'avg', 'min', 'max', 'count', 'countDistinct',
    'first', 'last', 'collect',
    'equals', 'notEquals', 'greater', 'less', 'greaterOrEqual',
    'lessOrEqual', 'and', 'or', 'not', 'in',
    'roundRobin',
}

ADF_RESERVED_WORDS = {
    'true', 'false', 'null', 'and', 'or', 'not', 'in',
    'as', 'like', 'between', 'case', 'when', 'then', 'else', 'end',
}


# ============================================================
# COLUMN REFERENCE REWRITER
# ============================================================
def rewrite_column_refs(expr: str, columns_lower: set) -> str:
    """Wrap column names that clash with ADF reserved words in {}."""
    def replacer(m):
        token = m.group(0)
        tl = token.lower()
        if tl in columns_lower and tl in ADF_RESERVED_WORDS:
            return "{" + token + "}"
        return token
    return re.sub(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', replacer, expr)


# ============================================================
# FILTER EXPRESSION NORMALISER
# ============================================================
def _normalize_filter_expr(expr: str, columns_lower: set) -> str:
    # iif(col == 1, true, false)  ->  equals(toInteger(col), 1)
    iif_eq = re.compile(
        r"iif\(\s*(\w+)\s*==\s*(\d+)\s*,\s*true\s*,\s*false\s*\)", re.IGNORECASE
    )
    expr = iif_eq.sub(lambda m: f"equals(toInteger({m.group(1)}), {m.group(2)})", expr)

    # iif(col != 0, true, false)  ->  notEquals(toInteger(col), 0)
    iif_neq = re.compile(
        r"iif\(\s*(\w+)\s*!=\s*(\d+)\s*,\s*true\s*,\s*false\s*\)", re.IGNORECASE
    )
    expr = iif_neq.sub(lambda m: f"notEquals(toInteger({m.group(1)}), {m.group(2)})", expr)

    # bare: col == 1  ->  equals(toInteger(col), 1)
    bare_eq = re.compile(r"^(\w+)\s*==\s*(\d+)$")
    m = bare_eq.match(expr.strip())
    if m and m.group(1).lower() in columns_lower:
        expr = f"equals(toInteger({m.group(1)}), {m.group(2)})"

    # bare: col != 0  ->  notEquals(toInteger(col), 0)
    bare_neq = re.compile(r"^(\w+)\s*!=\s*(\d+)$")
    m = bare_neq.match(expr.strip())
    if m and m.group(1).lower() in columns_lower:
        expr = f"notEquals(toInteger({m.group(1)}), {m.group(2)})"

    return expr


# ============================================================
# DATAFLOW SCRIPT BUILDER
# ============================================================
def _adf_type(inferred: str) -> str:
    """Map inferred Python type to an ADF DataFlow type string."""
    return {"integer": "integer", "double": "double"}.get(inferred, "string")


def build_dataflow_script(
    derived_columns: list,
    partition_count: int,
    columns: list,
    inferred_types: dict,
    filter_condition: str = None,
) -> tuple:
    """
    Returns (script_string, use_filter_bool).

    Declares every column with its type inside output() so ADF knows
    the schema at design/compile time. Without this the Source node
    shows "Columns: 0 total" and column references in filter() or
    derive() resolve to nothing (DF-EXPR-010).
    """
    schema_parts = []
    for col in columns:
        adf_t = _adf_type(inferred_types.get(col, "string"))
        schema_parts.append(f"          {col} as {adf_t}")
    schema_str = ",\n".join(schema_parts)

    col_expr = (
        ", ".join(f"{d['name']} = {d['expression']}" for d in derived_columns)
        if derived_columns
        else "processed_time = currentTimestamp()"
    )

    source_block = (
        "source(output(\n"
        + schema_str + "\n"
        + "     ),\n"
        "     allowSchemaDrift: true,\n"
        "     validateSchema: false,\n"
        "     ignoreNoFilesFound: false) ~> Source"
    )

    use_filter = bool(filter_condition and filter_condition.strip())

    if use_filter:
        middle = (
            "Source filter(" + filter_condition + ") ~> FilterRows\n"
            "FilterRows derive(" + col_expr + ") ~> DerivedColumns"
        )
    else:
        middle = "Source derive(" + col_expr + ") ~> DerivedColumns"

    sink_block = (
        "DerivedColumns sink(allowSchemaDrift: true,\n"
        "     validateSchema: false,\n"
        "     skipDuplicateMapInputs: true,\n"
        "     skipDuplicateMapOutputs: true,\n"
        "     partitionBy('hash', 1)) ~> Sink"
    )

    return "\n".join([source_block, middle, sink_block]), use_filter


# ============================================================
# PIPELINE: Data Flow Activity (stage1 -> stage2)
# ============================================================
def create_dataflow_pipeline(token: str, pipeline_config: dict, columns: list) -> requests.Response:
    columns_lower = {c.lower() for c in columns}

    derived_columns  = []
    filter_condition = None

    for t in pipeline_config.get("transformations", []):
        if "=" not in t:
            continue

        col, expr = t.split("=", 1)
        col  = col.strip()
        expr = expr.strip()

        _iif_pattern = re.compile(
            r"^iif\(\s*(\w+)\s*(==|!=)\s*(\d+)\s*,\s*true\s*,\s*false\s*\)$",
            re.IGNORECASE
        )
        _bare_pattern = re.compile(r"^(\w+)\s*(==|!=)\s*(\d+)$")

        is_filter_intent = (
            col.lower() == "filter"
            or bool(_iif_pattern.match(expr.strip()))
            or bool(_bare_pattern.match(expr.strip()))
        )

        if is_filter_intent:
            if filter_condition is not None:
                continue

            candidate = _normalize_filter_expr(expr.strip(), columns_lower)
            candidate = rewrite_column_refs(candidate, columns_lower)

            tokens  = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', candidate)
            invalid = [
                tk for tk in tokens
                if tk.lower() not in columns_lower and tk not in ADF_FUNCTIONS
            ]
            if invalid:
                print(f"   Filter references unknown columns {invalid} — dropping filter")
                continue

            filter_condition = candidate
            print(f"   Filter promoted from derived column '{col}': {filter_condition}")

        else:
            tokens  = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expr)
            invalid = [
                tk for tk in tokens
                if tk.lower() not in columns_lower and tk not in ADF_FUNCTIONS
            ]
            if invalid:
                print(f"   Skipping derived column '{col}': unknown references {invalid}")
                continue

            derived_columns.append({
                "name":       col,
                "expression": rewrite_column_refs(expr, columns_lower),
            })

    if not any(d["name"] == "processed_time" for d in derived_columns):
        derived_columns.append({
            "name":       "processed_time",
            "expression": "currentTimestamp()",
        })

    pipeline_name   = pipeline_config["name"]
    source_dataset  = pipeline_config["source_dataset"]
    sink_dataset    = pipeline_config["sink_dataset"]
    partition_count = pipeline_config.get("partition_count", 4)

    _ts     = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    df_name = f"DF_{pipeline_name}_{_ts}"

    script, use_filter = build_dataflow_script(
        derived_columns  = derived_columns,
        partition_count  = partition_count,
        columns          = columns,
        inferred_types   = pipeline_config.get("inferred_types", {}),
        filter_condition = filter_condition,
    )

    # Delete the pipeline before the dataflow — ADF returns 400 if
    # you try to delete a dataflow that a pipeline still references.
    delete_adf_resource(token, "pipelines", pipeline_name)
    time.sleep(5)
    legacy_df_name = f"DF_{pipeline_name}"
    delete_adf_resource(token, "dataflows", legacy_df_name)
    time.sleep(3)

    structured_transforms = []
    if use_filter:
        structured_transforms.append({"name": "FilterRows"})
    structured_transforms.append({"name": "DerivedColumns"})

    df_body = {
        "properties": {
            "type": "MappingDataFlow",
            "typeProperties": {
                "sources": [
                    {
                        "name": "Source",
                        "dataset": {
                            "referenceName": source_dataset,
                            "type": "DatasetReference"
                        }
                    }
                ],
                "sinks": [
                    {
                        "name": "Sink",
                        "dataset": {
                            "referenceName": sink_dataset,
                            "type": "DatasetReference"
                        }
                    }
                ],
                "transformations": structured_transforms,
                "script": script
            }
        }
    }

    r_df = adf_put(token, "dataflows", df_name, df_body)
    if r_df.status_code not in [200, 201]:
        print("   Dataflow creation failed")
        return r_df

    time.sleep(5)

    compute_type = map_compute_type(pipeline_config.get("compute_type", "General"))
    core_count = pipeline_config.get("core_count", 8)
    print(f"   Pipeline compute: type={compute_type}, cores={core_count}")

    pipeline_body = {
        "properties": {
            "activities": [
                {
                    "name": "DataFlowActivity",
                    "type": "ExecuteDataFlow",
                    "typeProperties": {
                        "dataflow": {
                            "referenceName": df_name,
                            "type":          "DataFlowReference"
                        },
                        "compute": {
                            "coreCount":   core_count,
                            "computeType": compute_type
                        },
                        "staging": {
                            "linkedService": {
                                "referenceName": "LS_Blob_Storage",
                                "type":          "LinkedServiceReference"
                            }
                        }
                    },
                    "inputs":  [{"referenceName": source_dataset, "type": "DatasetReference"}],
                    "outputs": [{"referenceName": sink_dataset,   "type": "DatasetReference"}]
                }
            ]
        }
    }

    r_pl = adf_put(token, "pipelines", pipeline_name, pipeline_body)

    if r_pl.status_code in [200, 201]:
        cleanup_old_dataflows(token, pipeline_name, df_name)

    return r_pl


# ============================================================
# CLEANUP: Remove old timestamped dataflow versions
# ============================================================
def cleanup_old_dataflows(token: str, pipeline_name: str, current_df_name: str):
    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{AZURE_DATA_FACTORY}"
        f"/dataflows?api-version=2018-06-01"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            return
        prefix = f"DF_{pipeline_name}_"
        for item in r.json().get("value", []):
            name = item.get("name", "")
            if name.startswith(prefix) and name != current_df_name:
                delete_adf_resource(token, "dataflows", name)
    except Exception as e:
        print(f"   Cleanup skipped: {e}")


# ============================================================
# TRIGGER: Run a pipeline immediately
# ============================================================
def trigger_pipeline(token: str, pipeline_name: str):
    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{AZURE_DATA_FACTORY}"
        f"/pipelines/{pipeline_name}/createRun?api-version=2018-06-01"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json={})
    if r.status_code == 200:
        run_id = r.json().get("runId", "unknown")
        print(f"   '{pipeline_name}' triggered | Run ID: {run_id}")
        return run_id
    else:
        print(f"   '{pipeline_name}' trigger failed -> {r.status_code}: {r.text}")
    return None


# ============================================================
# GET WITH RETRY
# ============================================================
def _get_with_retry(url: str, headers: dict, max_retries: int = 5) -> requests.Response:
    delay = 5
    for attempt in range(1, max_retries + 1):
        try:
            return requests.get(url, headers=headers, timeout=30)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            if attempt == max_retries:
                raise
            print(f"   Network error (attempt {attempt}/{max_retries}): {e}")
            print(f"   Retrying in {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 60)


# ============================================================
# CHECK PIPELINE STATUS
# ============================================================
def check_pipeline_status(token: str, pipeline_name: str, run_id: str, max_wait: int = 600) -> dict:
    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{AZURE_DATA_FACTORY}"
        f"/pipelineruns/{run_id}?api-version=2018-06-01"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    elapsed = 0
    while elapsed < max_wait:
        try:
            r = _get_with_retry(url, headers)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            print(f"   Persistent network error: {e} — waiting 30s...")
            time.sleep(30)
            elapsed += 30
            continue

        if r.status_code == 200:
            data   = r.json()
            status = data.get("status", "Unknown")

            if status == "Succeeded":
                print(f"   Pipeline '{pipeline_name}' succeeded")
                return {"status": "Succeeded", "details": data}

            elif status == "Failed":
                print(f"   Pipeline '{pipeline_name}' FAILED")
                act_url = (
                    f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
                    f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
                    f"/providers/Microsoft.DataFactory/factories/{AZURE_DATA_FACTORY}"
                    f"/pipelineruns/{run_id}/queryActivityruns?api-version=2018-06-01"
                )
                try:
                    act_r = _get_with_retry(act_url, headers)
                    if act_r.status_code == 200:
                        for activity in act_r.json().get("value", []):
                            if activity.get("status") == "Failed":
                                error = activity.get("error", {})
                                print(f"   Activity error: {error.get('message', 'Unknown')}")
                except Exception:
                    pass
                return {"status": "Failed", "details": data}

            else:
                print(f"   Pipeline '{pipeline_name}' -> {status} ({elapsed}s elapsed)")
                time.sleep(10)
                elapsed += 10

        elif r.status_code == 401:
            print("   Access token expired — refreshing...")
            try:
                token   = get_access_token()
                headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            except Exception as e:
                print(f"   Token refresh failed: {e}")
                return {"status": "Failed", "details": {"error": str(e)}}

        else:
            print(f"   Status check HTTP {r.status_code}: {r.text[:200]}")
            time.sleep(10)
            elapsed += 10

    print(f"   Timeout waiting for pipeline '{pipeline_name}'")
    return {"status": "Timeout", "run_id": run_id}