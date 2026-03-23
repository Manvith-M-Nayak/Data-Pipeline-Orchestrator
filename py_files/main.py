import csv
import os
import json
from groq_brain import decide_pipeline_config
from adf_api import (
    get_access_token,
    create_blob_container,
    purge_container,
    upload_csv,
    check_blob_has_rows,
    create_linked_service,
    create_dataset,
    create_copy_pipeline,
    create_dataflow_pipeline,
    publish_factory,
    trigger_pipeline,
    check_pipeline_status,
)
from config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY


# ============================================================
# READ CSV: Extract schema + sample data for Groq
# ============================================================
def read_csv_schema(filepath: str, sample_rows: int = 5) -> dict:
    file_size = os.path.getsize(filepath)

    if file_size < 5 * 1024 * 1024:
        size_hint = "small (< 5MB)"
    elif file_size < 50 * 1024 * 1024:
        size_hint = "medium (5MB - 50MB)"
    else:
        size_hint = "large (> 50MB)"

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        samples = []
        row_count = 0
        for row in reader:
            row_count += 1
            if len(samples) < sample_rows:
                samples.append(dict(row))

    inferred_types = {}
    for col in columns:
        sample_values = [str(s.get(col, "")) for s in samples if s.get(col)]
        if all(v.isdigit() for v in sample_values if v):
            inferred_types[col] = "integer"
        elif all(_is_float(v) for v in sample_values if v):
            inferred_types[col] = "double"
        else:
            inferred_types[col] = "string"

    print(f"CSV read: {len(columns)} columns, ~{row_count} rows, {size_hint}")
    return {
        "columns":        columns,
        "samples":        samples,
        "row_count":      row_count,
        "size_hint":      size_hint,
        "inferred_types": inferred_types,
    }


def _is_float(val: str) -> bool:
    try:
        float(val)
        return True
    except ValueError:
        return False


# ============================================================
# PREVIEW: Show Groq's decisions before deploying
# ============================================================
def preview_config(pipeline_config: dict):
    print("\n" + "=" * 60)
    print("  PIPELINE PLAN (decided by Groq LLaMA 3.3 70B)")
    print("=" * 60)

    print("\nContainers to create:")
    for label, name in pipeline_config["containers"].items():
        print(f"  {label:10} -> '{name}'")

    print("\nDatasets:")
    for ds in pipeline_config["datasets"]:
        print(
            f"  {ds['name']:25} | container: {ds['container']:15} "
            f"| file: {ds.get('filename', '*.csv'):15} | role: {ds.get('role', 'source')}"
        )

    print("\nPipelines:")
    for p in pipeline_config["pipelines"]:
        print(f"  {p['name']}")
        print(f"    type   : {p['type']}")
        print(f"    source : {p['source_dataset']}  ->  sink: {p['sink_dataset']}")
        if p["type"] == "dataflow":
            print(f"    transforms ({len(p.get('transformations', []))}):")
            for t in p.get("transformations", []):
                print(f"      - {t}")
            print(
                f"    compute: {p.get('compute_type')} "
                f"| cores: {p.get('core_count')} "
                f"| partitions: {p.get('partition_count')}"
            )
        elif p["type"] == "copy":
            print(f"    parallel_copies: {p.get('parallel_copies')} | DIU: {p.get('diu')}")

    print(f"\nExecution order: {' -> '.join(pipeline_config['execution_order'])}")
    print(f"\nReasoning: {pipeline_config.get('reasoning', 'N/A')}")
    print("=" * 60)


# ============================================================
# MAIN
# ============================================================
def main():
    print("\n" + "=" * 60)
    print("  ADF Dynamic Pipeline Generator — Powered by Groq LLaMA 3.3 70B")
    print("=" * 60 + "\n")

    # -- User inputs
    csv_filepath = input("Path to your CSV file        : ").strip()
    user_prompt  = input("What should the pipeline do? : ").strip()

    if not os.path.isfile(csv_filepath):
        print(f"File not found: '{csv_filepath}'")
        return
    if not csv_filepath.lower().endswith(".csv"):
        print(f"File must be a .csv — got: '{csv_filepath}'")
        return

    # -- Step 1: Read CSV schema
    print("\n--- Step 1: Reading CSV ---")
    schema = read_csv_schema(csv_filepath)

    # -- Step 2: Groq decides the pipeline config
    print("\n--- Step 2: Groq AI is deciding pipeline configuration ---")
    pipeline_config = decide_pipeline_config(schema, user_prompt)

    preview_config(pipeline_config)
    confirm = input("\nDeploy this pipeline to ADF? (yes/no): ").strip().lower()
    if confirm not in ["yes", "y"]:
        print("Aborted. No changes made to Azure.")
        return

    # -- Step 3: Create Blob Containers
    print("\n--- Step 3: Creating Blob Containers ---")
    for container_name in pipeline_config["containers"].values():
        create_blob_container(container_name)

    # -- Step 3b: Purge intermediate and output containers
    print("\n--- Step 3b: Purging intermediate and output containers ---")
    for key in ["stage1", "stage2"]:
        cname = pipeline_config["containers"].get(key)
        if cname:
            purge_container(cname)

    # -- Step 4: Upload CSV to raw container
    print("\n--- Step 4: Uploading CSV to raw container ---")
    raw_container = pipeline_config["containers"].get("raw", "incoming")
    uploaded_filename = upload_csv(csv_filepath, raw_container)

    print(f"\n   Verifying upload in '{raw_container}'...")
    if not check_blob_has_rows(raw_container):
        print("Upload verification failed — aborting before touching ADF")
        return

    # -- Step 5: Authenticate with Azure
    print("\n--- Step 5: Authenticating with Azure ---")
    token = get_access_token()

    # -- Step 6: Create Linked Service
    print("\n--- Step 6: Creating Linked Service ---")
    create_linked_service(token)

    # -- Step 7: Create Datasets
    print("\n--- Step 7: Creating Datasets ---")
    for ds in pipeline_config["datasets"]:
        r = create_dataset(token, ds)
        if r.status_code not in [200, 201]:
            print(f"Dataset creation failed for '{ds['name']}' — aborting")
            return

    # -- Step 8: Create Pipelines
    print("\n--- Step 8: Creating Pipelines ---")
    for p in pipeline_config["pipelines"]:
        if p["type"] == "copy":
            r = create_copy_pipeline(token, p)
        elif p["type"] == "dataflow":
            p["inferred_types"] = schema["inferred_types"]
            r = create_dataflow_pipeline(token, p, schema["columns"])
        else:
            print(f"Unknown pipeline type '{p['type']}' — skipping")
            continue

        if r is None or r.status_code not in [200, 201]:
            print(f"Pipeline creation failed for '{p['name']}' — aborting")
            return

    # -- Step 9: Publish to live layer
    print("\n--- Step 9: Publishing factory to live layer ---")
    publish_factory(token)

    # -- Step 10: Trigger and monitor pipelines
    print("\n--- Step 10: Triggering Pipelines and Monitoring ---")
    all_succeeded = True

    for pipeline_name in pipeline_config["execution_order"]:
        run_id = trigger_pipeline(token, pipeline_name)
        if not run_id:
            print(f"Could not trigger '{pipeline_name}' — aborting")
            all_succeeded = False
            break

        result = check_pipeline_status(token, pipeline_name, run_id)

        if result["status"] != "Succeeded":
            print(f"Pipeline '{pipeline_name}' did not succeed (status: {result['status']}). Stopping.")
            all_succeeded = False
            break

        copy_pipeline_names = [
            p["name"] for p in pipeline_config["pipelines"] if p["type"] == "copy"
        ]
        if pipeline_name in copy_pipeline_names:
            copy_cfg = next(
                p for p in pipeline_config["pipelines"] if p["name"] == pipeline_name
            )
            sink_ds_name = copy_cfg["sink_dataset"]
            sink_ds = next(
                (d for d in pipeline_config["datasets"] if d["name"] == sink_ds_name),
                None
            )
            if sink_ds:
                sink_container = sink_ds["container"]
                print(f"\n   Verifying data landed in '{sink_container}' after copy...")
                if not check_blob_has_rows(sink_container):
                    print(
                        f"Copy pipeline '{pipeline_name}' wrote nothing to '{sink_container}'. "
                        f"Check your source container and dataset configuration."
                    )
                    all_succeeded = False
                    break

    config_output = csv_filepath.replace(".csv", "_pipeline_config.json")
    with open(config_output, "w") as f:
        json.dump(pipeline_config, f, indent=2)
    print(f"\nPipeline config saved to: {config_output}")

    if all_succeeded:
        print("\nAll done! Monitor your pipelines at:")
        print("   https://adf.azure.com/en/monitoring/pipelineruns\n")
    else:
        print("\nPipeline run incomplete. Check the errors above.\n")


if __name__ == "__main__":
    main()