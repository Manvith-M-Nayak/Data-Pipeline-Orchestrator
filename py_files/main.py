import csv
import os
import json
from groq_brain import decide_pipeline_config, get_recommended_settings
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
from config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, AZURE_SUBSCRIPTION_ID, AZURE_RESOURCE_GROUP, AZURE_DATA_FACTORY
from azure.mgmt.datafactory import DataFactoryManagementClient
from monitor_agent import MonitoringAgent


def read_csv_schema(filepath: str, sample_rows: int = 5) -> dict:
    file_size = os.path.getsize(filepath)

    if file_size < 5 * 1024 * 1024:
        size_hint = "small (< 5MB)"
    elif file_size < 50 * 1024 * 1024:
        size_hint = "medium (5MB - 50MB)"
    elif file_size < 200 * 1024 * 1024:
        size_hint = "large (50MB - 200MB)"
    else:
        size_hint = "xlarge (> 200MB)"

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


def preview_config(pipeline_config: dict, show_editable: bool = True):
    print("\n" + "=" * 60)
    print("  PIPELINE PLAN (decided by Groq LLaMA 3.3 70B)")
    print("=" * 60)

    print(f"\nNumber of stages: {pipeline_config.get('num_containers', len(pipeline_config['containers']))}")
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
    
    if show_editable and "recommended_settings" in pipeline_config:
        print("\n" + "-" * 40)
        print("  RECOMMENDED SETTINGS (based on data size):")
        print("-" * 40)
        rec = pipeline_config["recommended_settings"]
        for key, val in rec.items():
            print(f"  {key:20}: {val}")
        
        if "editable_settings" in pipeline_config:
            print("\n  Available options:")
            edit = pipeline_config["editable_settings"]
            for key, options in edit.items():
                print(f"    {key}: {options}")
    
    print(f"\nReasoning: {pipeline_config.get('reasoning', 'N/A')}")
    print("=" * 60)


def edit_pipeline_config(pipeline_config: dict) -> dict:
    """Allow user to edit pipeline configuration settings."""
    print("\n--- Edit Pipeline Configuration ---")
    print("Current recommended settings:")
    rec = pipeline_config.get("recommended_settings", {})
    for key, val in rec.items():
        print(f"  {key}: {val}")
    
    print("\nAvailable settings to edit:")
    print("  1. compute_type (General/MemoryOptimized)")
    print("  2. core_count")
    print("  3. partition_count")
    print("  4. parallel_copies")
    print("  5. diu")
    print("  6. Add custom transformations")
    print("  7. Done - proceed with current config")
    
    while True:
        choice = input("\nEnter choice (1-7): ").strip()
        
        if choice == "7":
            break
        
        elif choice == "1":
            print(f"Current: {pipeline_config['pipelines'][0].get('compute_type', 'General')}")
            new_val = input("Enter compute_type (General/MemoryOptimized): ").strip()
            if new_val in ["General", "MemoryOptimized"]:
                for p in pipeline_config["pipelines"]:
                    if p["type"] == "dataflow":
                        p["compute_type"] = new_val
                print(f"  Updated compute_type to {new_val}")
        
        elif choice == "2":
            print(f"Current core_count: {pipeline_config['pipelines'][0].get('core_count', 4)}")
            try:
                new_val = int(input("Enter core_count (4/8/16/32): ").strip())
                if new_val in [4, 8, 16, 32]:
                    for p in pipeline_config["pipelines"]:
                        if p["type"] == "dataflow":
                            p["core_count"] = new_val
                    print(f"  Updated core_count to {new_val}")
            except ValueError:
                print("  Invalid number")
        
        elif choice == "3":
            print(f"Current partition_count: {pipeline_config['pipelines'][0].get('partition_count', 4)}")
            try:
                new_val = int(input("Enter partition_count: ").strip())
                if new_val > 0:
                    for p in pipeline_config["pipelines"]:
                        if p["type"] == "dataflow":
                            p["partition_count"] = new_val
                    print(f"  Updated partition_count to {new_val}")
            except ValueError:
                print("  Invalid number")
        
        elif choice == "4":
            print(f"Current parallel_copies: {pipeline_config['pipelines'][0].get('parallel_copies', 2)}")
            try:
                new_val = int(input("Enter parallel_copies: ").strip())
                if new_val > 0:
                    for p in pipeline_config["pipelines"]:
                        if p["type"] == "copy":
                            p["parallel_copies"] = new_val
                    print(f"  Updated parallel_copies to {new_val}")
            except ValueError:
                print("  Invalid number")
        
        elif choice == "5":
            print(f"Current DIU: {pipeline_config['pipelines'][0].get('diu', 2)}")
            try:
                new_val = int(input("Enter DIU: ").strip())
                if new_val > 0:
                    for p in pipeline_config["pipelines"]:
                        if p["type"] == "copy":
                            p["diu"] = new_val
                    print(f"  Updated DIU to {new_val}")
            except ValueError:
                print("  Invalid number")
        
        elif choice == "6":
            print("\nCurrent transformations:")
            for i, p in enumerate(pipeline_config["pipelines"]):
                if p["type"] == "dataflow":
                    print(f"  Pipeline {i+1}: {p.get('transformations', [])}")
            
            print("\nEnter new transformation (format: column = expression)")
            print("Examples:")
            print("  name = upper(name)")
            print("  amount = toDouble(amount)")
            print("  email = lower(email)")
            print("(Press Enter with empty line to stop adding)")
            
            while True:
                trans = input("Add transformation (or Enter to skip): ").strip()
                if not trans:
                    break
                if "=" in trans:
                    for p in pipeline_config["pipelines"]:
                        if p["type"] == "dataflow":
                            if "transformations" not in p:
                                p["transformations"] = []
                            if "processed_time = currentTimestamp()" not in trans:
                                p["transformations"].append(trans)
                    print(f"  Added: {trans}")
    
    return pipeline_config


def main():
    print("\n" + "=" * 60)
    print("  ADF Dynamic Pipeline Generator — Powered by Groq LLaMA 3.3 70B")
    print("=" * 60 + "\n")

    csv_filepath = input("Path to your CSV file        : ").strip()
    user_prompt  = input("What should the pipeline do? : ").strip()

    if not os.path.isfile(csv_filepath):
        print(f"File not found: '{csv_filepath}'")
        return
    if not csv_filepath.lower().endswith(".csv"):
        print(f"File must be a .csv — got: '{csv_filepath}'")
        return

    print("\n--- Step 1: Reading CSV ---")
    schema = read_csv_schema(csv_filepath)

    print("\n--- Step 2: Configure Pipeline ---")
    print("Number of stages (containers/pipelines):")
    print("  2 stages: raw -> silver (copy + dataflow)")
    print("  3 stages: incoming -> bronze -> silver (copy + 2 dataflows)")
    print("  4 stages: raw -> stage1 -> stage2 -> stage3")
    print("  5 stages: raw -> stage1 -> stage2 -> stage3 -> stage4")
    
    num_containers_input = input("Number of stages (default 3, min 2, max 5): ").strip()
    num_containers = 3
    if num_containers_input:
        try:
            num_containers = max(2, min(5, int(num_containers_input)))
        except ValueError:
            print("Invalid input, using default 3 stages")
    
    rec_settings = get_recommended_settings(schema["size_hint"])
    print(f"\nRecommended settings for {schema['size_hint']} data:")
    for k, v in rec_settings.items():
        print(f"  {k}: {v}")
    
    customize = input("\nCustomize settings? (yes/no, default no): ").strip().lower()
    custom_settings = None
    if customize == "yes" or customize == "y":
        custom_settings = {}
        for key in ["compute_type", "core_count", "partition_count", "parallel_copies", "diu"]:
            val = input(f"  {key} (recommended: {rec_settings.get(key)}): ").strip()
            if val:
                if key == "compute_type":
                    custom_settings[key] = val
                else:
                    try:
                        custom_settings[key] = int(val)
                    except ValueError:
                        pass
    
    custom_container_names = input("\nCustom container names? (comma-separated, or Enter for default): ").strip()
    container_names = None
    if custom_container_names:
        container_names = [c.strip() for c in custom_container_names.split(",")]
        if len(container_names) != num_containers:
            print(f"Warning: Expected {num_containers} names, got {len(container_names)}. Using defaults.")
            container_names = None

    print("\n--- Step 3: Groq AI is deciding pipeline configuration ---")
    pipeline_config = decide_pipeline_config(
        schema, 
        user_prompt,
        num_containers=num_containers,
        custom_settings=custom_settings,
        container_names=container_names
    )

    preview_config(pipeline_config)
    
    edit_choice = input("\nEdit pipeline settings before deployment? (yes/no): ").strip().lower()
    if edit_choice == "yes" or edit_choice == "y":
        pipeline_config = edit_pipeline_config(pipeline_config)
        preview_config(pipeline_config, show_editable=False)

    confirm = input("\nDeploy this pipeline to ADF? (yes/no): ").strip().lower()
    if confirm not in ["yes", "y"]:
        print("Aborted. No changes made to Azure.")
        return

    print("\n--- Step 4: Creating Blob Containers ---")
    for container_name in pipeline_config["containers"].values():
        create_blob_container(container_name)

    raw_container = pipeline_config["containers"].get("stage0") or pipeline_config["containers"].get("raw") or list(pipeline_config["containers"].values())[0]
    purge_container(raw_container)

    print("\n--- Step 4b: Purging intermediate and output containers ---")
    for key in list(pipeline_config["containers"].keys())[1:]:
        cname = pipeline_config["containers"].get(key)
        if cname:
            purge_container(cname)

    print("\n--- Step 5: Uploading CSV to raw container ---")
    raw_container = pipeline_config["containers"].get("stage0") or pipeline_config["containers"].get("raw") or list(pipeline_config["containers"].values())[0]
    uploaded_filename = upload_csv(csv_filepath, raw_container)

    print(f"\n   Verifying upload in '{raw_container}'...")
    if not check_blob_has_rows(raw_container):
        print("Upload verification failed — aborting before touching ADF")
        return

    print("\n--- Step 6: Authenticating with Azure ---")
    token = get_access_token()

    print("\n--- Step 7: Creating Linked Service ---")
    create_linked_service(token)

    print("\n--- Step 8: Creating Datasets ---")
    for ds in pipeline_config["datasets"]:
        r = create_dataset(token, ds)
        if r.status_code not in [200, 201]:
            print(f"Dataset creation failed for '{ds['name']}' — aborting")
            return

    print("\n--- Step 9: Creating Pipelines ---")
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

    print("\n--- Step 10: Publishing factory to live layer ---")
    publish_factory(token)

    print("\n--- Step 11: Triggering Pipelines and Monitoring ---")
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

    if all_succeeded:
        print("\n--- Step 12: Monitoring Pipelines ---")

        from azure.identity import ClientSecretCredential
        from azure.mgmt.datafactory import DataFactoryManagementClient
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
            silent=False
        )

        report = monitor.monitor()

        summary = report.get("summary", {})

        print("\nSUMMARY:")
        print(f"Total Runs: {summary.get('total_runs')}")
        print(f"Succeeded : {summary.get('succeeded')}")
        print(f"Failed    : {summary.get('failed')}")
        print(f"Running   : {summary.get('in_progress')}")

        print("\nAll done! Monitor your pipelines at:")
        print("   https://adf.azure.com/en/monitoring/pipelineruns\n")
    else:
        print("\nPipeline run incomplete. Check the errors above.\n")


if __name__ == "__main__":
    main()
