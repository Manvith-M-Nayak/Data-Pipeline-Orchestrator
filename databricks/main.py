"""
main.py  —  Databricks Pipeline Orchestrator  (CLI entry point)
---------------------------------------------------------------
Updated pipeline flow:

  1. Read + inspect the CSV schema
  2. Ask Groq to design the pipeline config
  3. ✅ NEW: validate_and_heal_config() — pre-execution validation + healing
  4. Preview the plan and let the user tweak it
  5. Run the pipeline via databricks_api.execute_pipeline()
  6. On runtime failure → invoke DatabricksSelfHealingAgent.heal() + retry
  7. Report outcome

Run from the databricks/ directory:
    python main.py
or from the repo root:
    python databricks/main.py
"""

import csv
import os
import sys
import json

# ── Make sure the databricks/ folder is always importable ─────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from databricks_groq_brain        import decide_pipeline_config, get_recommended_settings
from databricks_api                import execute_pipeline, check_connection
from databricks_self_healing_agent import DatabricksSelfHealingAgent
from databricks_validator          import validate_and_heal_config, print_healing_log


# ── CSV helpers ────────────────────────────────────────────────────────────────
def read_csv_schema(filepath: str, sample_rows: int = 5) -> dict:
    file_size = os.path.getsize(filepath)
    size_hint = (
        "small (< 5MB)"     if file_size < 5_242_880   else
        "medium (5–50MB)"   if file_size < 52_428_800  else
        "large (50–200MB)"  if file_size < 209_715_200 else
        "xlarge (> 200MB)"
    )

    with open(filepath, newline="", encoding="utf-8") as f:
        reader   = csv.DictReader(f)
        columns  = reader.fieldnames or []
        samples, row_count = [], 0
        for row in reader:
            row_count += 1
            if len(samples) < sample_rows:
                samples.append(dict(row))

    def _is_float(v):
        try: float(v); return True
        except ValueError: return False

    inferred_types = {}
    for col in columns:
        vals = [str(s.get(col, "")) for s in samples if s.get(col)]
        if all(v.isdigit() for v in vals if v):
            inferred_types[col] = "integer"
        elif all(_is_float(v) for v in vals if v):
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


# ── Plan preview ───────────────────────────────────────────────────────────────
def preview_config(config: dict, schema: dict, used_fallback: bool = False):
    print("\n" + "=" * 65)
    source = "Default Configuration" if used_fallback else "Groq LLaMA 3.3 70B"
    print(f"  PIPELINE PLAN  ({source})")
    print("=" * 65)

    print(f"\nNumber of stages : {len(config['containers'])}")
    print("\nContainers:")
    for label, name in config["containers"].items():
        print(f"  {label:10} → '{name}'")

    print("\nPipelines:")
    for p in config["pipelines"]:
        print(f"  {p['name']}")
        print(f"    type            : {p['type']}")
        print(f"    source → sink   : {p['source_dataset']} → {p['sink_dataset']}")
        print(f"    workers         : {p.get('num_workers', 'N/A')}")
        print(f"    shuffle_partitions: {p.get('shuffle_partitions', 'N/A')}")
        if p["type"] == "transform":
            transforms = p.get("transformations", [])
            print(f"    transformations ({len(transforms)}):")
            for t in transforms:
                print(f"      - {t}")

    print(f"\nExecution order  : {' → '.join(config['execution_order'])}")

    if "recommended_settings" in config:
        print("\nRecommended cluster settings:")
        for k, v in config["recommended_settings"].items():
            print(f"  {k:22}: {v}")

    print(f"\nReasoning: {config.get('reasoning', 'N/A')}")
    print("\nCSV schema:")
    for col in schema["columns"]:
        t = schema["inferred_types"].get(col, "string")
        print(f"  {col:30} {t}")
    print("=" * 65)


# ── Config editor (CLI) ────────────────────────────────────────────────────────
def edit_pipeline_config(config: dict) -> dict:
    """Let the user tweak settings before deploying."""
    print("\n--- Edit Pipeline Configuration ---")
    print("Options:")
    print("  1. num_workers")
    print("  2. shuffle_partitions")
    print("  3. Add / replace a transformation")
    print("  4. Done — proceed with current config")

    while True:
        choice = input("\nChoice (1–4): ").strip()

        if choice == "4":
            break

        elif choice == "1":
            for p in config["pipelines"]:
                cur = p.get("num_workers", "N/A")
                print(f"  '{p['name']}' current num_workers: {cur}")
            try:
                val = int(input("New num_workers (0 = single-node): ").strip())
                for p in config["pipelines"]:
                    p["num_workers"] = val
                print(f"  Updated num_workers → {val}")
            except ValueError:
                print("  Invalid — skipped")

        elif choice == "2":
            for p in config["pipelines"]:
                cur = p.get("shuffle_partitions", "N/A")
                print(f"  '{p['name']}' current shuffle_partitions: {cur}")
            try:
                val = int(input("New shuffle_partitions: ").strip())
                if val > 0:
                    for p in config["pipelines"]:
                        p["shuffle_partitions"] = val
                    print(f"  Updated shuffle_partitions → {val}")
            except ValueError:
                print("  Invalid — skipped")

        elif choice == "3":
            for i, p in enumerate(config["pipelines"]):
                if p["type"] == "transform":
                    print(f"\n  Pipeline {i+1}: '{p['name']}'")
                    for t in p.get("transformations", []):
                        print(f"    - {t}")

            print("\nEnter transformation  (format: output_col = expression)")
            print("Examples:")
            print("  total_revenue = quantity * unit_price")
            print("  region_upper  = upper(Region)")
            print("  processed_time = currentTimestamp()")
            print("(empty line to stop)")

            while True:
                trans = input("Add transformation: ").strip()
                if not trans:
                    break
                if "=" in trans:
                    for p in config["pipelines"]:
                        if p["type"] == "transform":
                            existing = p.setdefault("transformations", [])
                            lhs = trans.split("=", 1)[0].strip()
                            p["transformations"] = [
                                t for t in existing
                                if not t.strip().startswith(f"{lhs} =")
                                and not t.strip().startswith(f"{lhs}=")
                            ]
                            p["transformations"].append(trans)
                    print(f"  Added: {trans}")
                else:
                    print("  No '=' found — skipped")

    return config


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "=" * 65)
    print("  Databricks Pipeline Orchestrator — CLI")
    print("  Powered by Groq LLaMA 3.3 70B  +  Apache Spark")
    print("=" * 65 + "\n")

    # ── Step 0: workspace connectivity ────────────────────────────────────────
    print("--- Step 0: Checking Databricks workspace connectivity ---")
    ok, conn_msg = check_connection()
    if not ok:
        print(f"❌  Cannot reach Databricks workspace: {conn_msg}")
        print("    Check DATABRICKS_HOST and DATABRICKS_TOKEN in config.py")
        return
    print(f"✅  {conn_msg}")

    # ── Step 1: CSV ────────────────────────────────────────────────────────────
    csv_filepath = input("\nPath to your CSV file        : ").strip()
    user_prompt  = input("What should the pipeline do? : ").strip()

    if not os.path.isfile(csv_filepath):
        print(f"File not found: '{csv_filepath}'")
        return
    if not csv_filepath.lower().endswith(".csv"):
        print(f"File must be .csv — got: '{csv_filepath}'")
        return

    print("\n--- Step 1: Reading CSV schema ---")
    schema = read_csv_schema(csv_filepath)

    # ── Step 2: Stage count ────────────────────────────────────────────────────
    print("\n--- Step 2: Configure pipeline stages ---")
    print("  2 stages: raw → silver")
    print("  3 stages: incoming → bronze → silver  (default)")
    print("  4 stages: raw → stage1 → stage2 → stage3")
    print("  5 stages: raw → stage1 → stage2 → stage3 → stage4")

    num_input      = input("Number of stages (2–5, default 3): ").strip()
    num_containers = 3
    if num_input:
        try:
            num_containers = max(2, min(5, int(num_input)))
        except ValueError:
            print("Invalid input — using 3 stages")

    # ── Step 3: Cluster overrides (optional) ──────────────────────────────────
    rec = get_recommended_settings(schema["size_hint"])
    print(f"\nRecommended settings for {schema['size_hint']} data:")
    for k, v in rec.items():
        print(f"  {k}: {v}")

    customize = input("\nCustomise settings? (yes/no, default no): ").strip().lower()
    custom_settings = None
    if customize in ("yes", "y"):
        custom_settings = {}
        for key in ("num_workers", "shuffle_partitions"):
            val = input(f"  {key} (recommended: {rec.get(key, 'N/A')}): ").strip()
            if val:
                try:
                    custom_settings[key] = int(val)
                except ValueError:
                    pass

    custom_names_raw = input("\nCustom container names? (comma-separated, or Enter for default): ").strip()
    container_names  = None
    if custom_names_raw:
        parts = [c.strip() for c in custom_names_raw.split(",")]
        if len(parts) == num_containers:
            container_names = parts
        else:
            print(f"  Expected {num_containers} names, got {len(parts)} — using defaults")

    # ── Step 4: Groq planning ──────────────────────────────────────────────────
    print("\n--- Step 3: AI pipeline planning ---")
    config, used_fallback = decide_pipeline_config(
        schema=schema,
        user_prompt=user_prompt,
        num_containers=num_containers,
        custom_settings=custom_settings,
        container_names=container_names,
    )

    if used_fallback:
        print("⚠️  Using default configuration (Groq API unavailable)")
    else:
        print("✅  Groq planned the pipeline")

    # ── Step 4b: Pre-execution validation + healing ────────────────────────────
    print("\n--- Step 3b: Pre-execution validation + healing ---")
    try:
        config, healing_log, is_valid = validate_and_heal_config(config, schema)
        print_healing_log(healing_log)
    except ValueError as validation_err:
        print(f"\n{validation_err}")
        print("\n⛔  Pipeline aborted during pre-execution validation.")
        print("    Fix the transform expression(s) above and re-run.")
        return

    preview_config(config, schema, used_fallback=used_fallback)

    edit_choice = input("\nEdit pipeline settings before deployment? (yes/no): ").strip().lower()
    if edit_choice in ("yes", "y"):
        config = edit_pipeline_config(config)
        # Re-validate after manual edits
        print("\n--- Re-validating after manual edits ---")
        try:
            config, healing_log, _ = validate_and_heal_config(config, schema)
            print_healing_log(healing_log)
        except ValueError as ve:
            print(f"\n{ve}")
            print("\n⛔  Validation failed after manual edits. Aborting.")
            return
        preview_config(config, schema, used_fallback=used_fallback)

    confirm = input("\nDeploy this pipeline to Databricks? (yes/no): ").strip().lower()
    if confirm not in ("yes", "y"):
        print("Aborted — no changes made to Databricks.")
        return

    # ── Step 5: First run attempt ──────────────────────────────────────────────
    print("\n--- Step 4: Executing pipeline ---")
    result = execute_pipeline(
        csv_path=csv_filepath,
        pipeline_config=config,
        schema=schema,
    )

    if result["status"] == "ok":
        _print_success(result, config)
        return

    # ── Step 6: Runtime self-healing ───────────────────────────────────────────
    error_msg = result.get("message", "Unknown error")
    print(f"\n⚠  Pipeline run failed: {error_msg}")
    print("\n" + "=" * 65)
    print("  RUNTIME SELF-HEALING AGENT")
    print("=" * 65)

    agent = DatabricksSelfHealingAgent()
    _, fixed_config = agent.heal(
        error_message=error_msg,
        pipeline_config=config,
        csv_columns=schema.get("columns"),
    )

    # Detect cause so we can warn about unretryable errors
    cause_info = agent.detect_root_cause(error_msg, config)
    cause      = cause_info.get("cause", "unknown")

    from databricks_self_healing_agent import CAUSE_AUTH_ERROR, CAUSE_WORKSPACE_ERROR
    if cause in (CAUSE_AUTH_ERROR, CAUSE_WORKSPACE_ERROR):
        print(f"\n❌  Cannot auto-retry — {cause} requires manual intervention.")
        print("    Please fix the issue described above and re-run the script.")
        return

    retry = input("\nRetry pipeline with healed config? (yes/no, default yes): ").strip().lower()
    if retry in ("no", "n"):
        print("Retry skipped. Exiting.")
        return

    # ── Step 7: Retry with fixed config ───────────────────────────────────────
    print("\n--- Step 5: Retrying with healed config ---")
    retry_result = execute_pipeline(
        csv_path=csv_filepath,
        pipeline_config=fixed_config,
        schema=schema,
    )

    if retry_result["status"] == "ok":
        print("\n✅  Healed pipeline completed successfully.")
        _print_success(retry_result, fixed_config)
    else:
        print(f"\n❌  Retry also failed: {retry_result.get('message','Unknown error')}")
        print("    The self-healing agent was unable to resolve this error automatically.")
        print("    Check the Databricks Jobs UI for the full stack trace:")
        from config import DATABRICKS_HOST
        print(f"    {DATABRICKS_HOST.rstrip('/')}/#joblist")


def _print_success(result: dict, config: dict):
    print("\n" + "=" * 65)
    print("  PIPELINE COMPLETE")
    print("=" * 65)
    print(f"  Status : {result.get('status','ok').upper()}")

    stage_paths = result.get("stage_paths", {})
    if stage_paths:
        print("\n  Stage outputs:")
        for container, path in stage_paths.items():
            print(f"    {container:20} → {path}")

    if result.get("output_csv_bytes"):
        out_name = result.get("output_csv_name", "output.csv")
        out_path = os.path.join(os.getcwd(), out_name)
        with open(out_path, "wb") as f:
            f.write(result["output_csv_bytes"])
        print(f"\n  Output CSV saved to: {out_path}")
    else:
        from config import DATABRICKS_HOST
        ws = DATABRICKS_HOST.rstrip("/")
        print(f"\n  View your job runs at: {ws}/#joblist")

    print("\nAll done! 🎉")
    print("=" * 65)


if __name__ == "__main__":
    main()