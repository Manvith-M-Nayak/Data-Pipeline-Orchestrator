"""
Shared, backend-neutral planner logic.

Used by every planner backend (ollama_planner, groq_planner) so the produced
pipeline config has an identical shape and passes identical validation
regardless of which LLM generated it:

  - container naming conventions + Azure-safe normalization
  - recommended / editable resource settings
  - deterministic default config (the fallback when the LLM is unreachable)
  - structural validation (stage types, processed_time, aggregation, container names)

Each stage of a config is either:
  - "copy"     : ADF Copy Activity moves blob-to-blob as-is (ingestion)
  - "notebook" : ADF DatabricksNotebook Activity runs a generated PySpark notebook
"""

import json
import re


MAX_CONTAINERS = 10


CONTAINER_NAMING_CONVENTIONS = [
    ["raw", "bronze", "silver", "gold", "platinum", "diamond", "curated", "serving", "archive", "export"],
    ["incoming", "bronze", "silver", "gold", "platinum", "diamond", "curated", "serving", "archive", "export"],
    ["raw", "stage", "curated", "refined", "enriched", "aggregated", "modeled", "serving", "archive", "export"],
    ["landing", "processing", "output", "refined", "enriched", "aggregated", "modeled", "serving", "archive", "export"],
    ["input", "intermediate", "final", "refined", "enriched", "aggregated", "modeled", "serving", "archive", "export"],
]


RECOMMENDED_SETTINGS = {
    "small":  {"diu": 2, "num_workers": 0, "shuffle_partitions": 4,  "node_type": "Standard_D4s_v3"},
    "medium": {"diu": 4, "num_workers": 0, "shuffle_partitions": 8,  "node_type": "Standard_D4s_v3"},
    "large":  {"diu": 8, "num_workers": 2, "shuffle_partitions": 16, "node_type": "Standard_D4s_v3"},
    "xlarge": {"diu": 16, "num_workers": 4, "shuffle_partitions": 32, "node_type": "Standard_DS4_v2"},
}


DEFAULT_EDITABLE_SETTINGS = {
    "diu":                [1, 2, 4, 8, 16, 32],
    "num_workers":        [0, 2, 4, 8, 16],
    "shuffle_partitions": [4, 8, 16, 32, 64],
    "node_type":          ["Standard_D4s_v3", "Standard_DS4_v2", "Standard_D8s_v3"],
}


def get_recommended_settings(size_hint: str) -> dict:
    s = (size_hint or "").lower()
    if "small" in s or "< 5" in s:
        return dict(RECOMMENDED_SETTINGS["small"])
    if "medium" in s or "5–50" in s or "5-50" in s:
        return dict(RECOMMENDED_SETTINGS["medium"])
    if "xlarge" in s or "> 200" in s or ">200" in s:
        return dict(RECOMMENDED_SETTINGS["xlarge"])
    if "large" in s:
        return dict(RECOMMENDED_SETTINGS["large"])
    return dict(RECOMMENDED_SETTINGS["medium"])


def _resolve_container_names(num_containers: int, container_names: list) -> list:
    if container_names and len(container_names) == num_containers:
        return container_names
    for conv in CONTAINER_NAMING_CONVENTIONS:
        if len(conv) >= num_containers:
            return conv[:num_containers]
    return [f"stage{i}" for i in range(num_containers)]


def _build_datasets(clist: list) -> list:
    n = len(clist)
    out = []
    for i, name in enumerate(clist):
        if i == 0:
            role = "source"
        elif i == n - 1:
            role = "sink"
        else:
            role = "intermediate"
        out.append({
            "name":      f"DS_{name.title().replace('_', '')}",
            "container": name,
            "role":      role,
        })
    return out


def _build_stages(clist: list, rec: dict) -> list:
    """Default stage plan: stage0→stage1 is copy (ingest), every later hop is a notebook."""
    n = len(clist)
    stages = []
    for i in range(n - 1):
        src_container = clist[i]
        sink_container = clist[i + 1]
        if i == 0:
            stages.append({
                "name": f"Ingest_{src_container.title()}_To_{sink_container.title()}",
                "type": "copy",
                "source_dataset": f"DS_{src_container.title().replace('_', '')}",
                "sink_dataset":   f"DS_{sink_container.title().replace('_', '')}",
                "diu": rec["diu"],
            })
        else:
            stages.append({
                "name": f"Transform_{src_container.title()}_To_{sink_container.title()}",
                "type": "notebook",
                "source_container": src_container,
                "sink_container":   sink_container,
                "transformations":  ["processed_time = currentTimestamp()"],
                "filter_condition": None,
                "num_workers":        rec["num_workers"],
                "shuffle_partitions": rec["shuffle_partitions"],
            })
    return stages


def build_default_config(
    schema: dict,
    user_prompt: str,
    num_containers: int = 3,
    custom_settings: dict = None,
    container_names: list = None,
) -> dict:
    rec = get_recommended_settings(schema.get("size_hint", "medium"))
    if custom_settings:
        rec.update(custom_settings)

    num_containers = max(2, min(MAX_CONTAINERS, num_containers))
    clist = _resolve_container_names(num_containers, container_names)

    containers = {f"stage{i}": clist[i] for i in range(num_containers)}
    datasets = _build_datasets(clist)
    stages = _build_stages(clist, rec)
    execution_order = [s["name"] for s in stages]

    return {
        "containers":           containers,
        "containers_to_create": clist,
        "datasets":             datasets,
        "stages":               stages,
        "execution_order":      execution_order,
        "num_containers":       num_containers,
        "recommended_settings": rec,
        "editable_settings":    DEFAULT_EDITABLE_SETTINGS,
        "reasoning": (
            f"Default {num_containers}-stage unified pipeline. "
            f"ADF Copy Activity ingests '{clist[0]}' → '{clist[1]}'. "
            f"Remaining stages run as Databricks notebooks invoked by ADF."
        ),
    }


_AGG_OPS = {"avg", "sum", "min", "max", "count"}
_NUMERIC_TYPES = {"integer", "double", "long", "float"}


def _validate_aggregation(agg: dict, schema: dict, stage_name: str):
    """Return a cleaned aggregation block, or None if it is unusable."""
    if not isinstance(agg, dict):
        return None
    columns = set(schema.get("columns", []))
    types   = schema.get("inferred_types", {})

    group_by = [g for g in (agg.get("group_by") or []) if g in columns]
    dropped_groups = [g for g in (agg.get("group_by") or []) if g not in columns]
    if dropped_groups:
        print(f"   [{stage_name}] dropping group_by cols not in schema: {dropped_groups}")

    clean_aggs = []
    for a in (agg.get("aggregations") or []):
        if not isinstance(a, dict):
            continue
        op    = str(a.get("op", "")).strip().lower()
        column = str(a.get("column", "")).strip()
        alias  = str(a.get("alias", "")).strip()
        if op not in _AGG_OPS or not alias:
            print(f"   [{stage_name}] dropping bad aggregation: {a}")
            continue
        if op == "count" and column == "*":
            clean_aggs.append({"op": op, "column": "*", "alias": alias})
            continue
        if column not in columns:
            print(f"   [{stage_name}] dropping aggregation on missing col '{column}'")
            continue
        if op in ("avg", "sum") and types.get(column) not in _NUMERIC_TYPES:
            print(f"   [{stage_name}] dropping {op} on non-numeric col '{column}' ({types.get(column)})")
            continue
        clean_aggs.append({"op": op, "column": column, "alias": alias})

    if not group_by or not clean_aggs:
        return None
    return {"group_by": group_by, "aggregations": clean_aggs}


def _sanitize_container_name(name: str) -> str:
    """Coerce a name into a valid Azure Blob container name:
    lowercase, 3-63 chars, [a-z0-9] and single hyphens, no leading/trailing hyphen."""
    s = re.sub(r"[^a-z0-9-]+", "-", str(name).strip().lower())
    s = re.sub(r"-+", "-", s).strip("-")
    if len(s) < 3:
        s = (s + "-data").strip("-") or "data"
    return s[:63].strip("-")


def _normalize_container_names(config: dict) -> dict:
    """Rewrite every container name in the config to an Azure-safe form, keeping
    all cross-references (containers, containers_to_create, datasets, stages)
    consistent. Collisions after sanitizing get a numeric suffix."""
    originals = config.get("containers_to_create") or list(config.get("containers", {}).values())
    if not originals:
        return config

    mapping, seen = {}, {}
    for orig in originals:
        clean = _sanitize_container_name(orig)
        if clean in seen.values():
            n = seen.get(clean, 1) + 1
            seen[clean] = n
            clean = _sanitize_container_name(f"{clean}-{n}")
        seen[clean] = seen.get(clean, 1)
        mapping[orig] = clean

    remap = lambda v: mapping.get(v, _sanitize_container_name(v)) if v else v

    config["containers_to_create"] = [mapping[o] for o in originals]
    if isinstance(config.get("containers"), dict):
        config["containers"] = {k: remap(v) for k, v in config["containers"].items()}
    for ds in config.get("datasets", []):
        if ds.get("container"):
            ds["container"] = remap(ds["container"])
    for s in config.get("stages", []):
        if s.get("source_container"):
            s["source_container"] = remap(s["source_container"])
        if s.get("sink_container"):
            s["sink_container"] = remap(s["sink_container"])
    return config


def _structural_validate(config: dict, schema: dict = None) -> dict:
    """Enforce first-stage=copy, later-stages=notebook, processed_time presence,
    validate any aggregation blocks, and normalize container names to Azure-safe."""
    schema = schema or {}
    config = _normalize_container_names(config)
    stages = config.get("stages", [])
    for i, s in enumerate(stages):
        if i == 0 and s.get("type") != "copy":
            print(f"   First stage must be 'copy' — coercing '{s['name']}'")
            s["type"] = "copy"
            s.pop("transformations", None)
            s.pop("filter_condition", None)
            s.pop("aggregation", None)
            s.setdefault("diu", 2)
        elif i > 0 and s.get("type") == "copy":
            print(f"   Stage '{s['name']}' coerced to 'notebook' (only stage0 may be copy)")
            s["type"] = "notebook"

        if s.get("type") == "notebook":
            transforms = [t for t in s.get("transformations", []) if t and t.strip()]
            if not any("processed_time" in t for t in transforms):
                transforms.append("processed_time = currentTimestamp()")
            s["transformations"] = transforms
            s.setdefault("filter_condition", None)
            s.setdefault("num_workers", 0)
            s.setdefault("shuffle_partitions", 8)

            if s.get("aggregation"):
                agg = _validate_aggregation(s["aggregation"], schema, s["name"])
                if agg:
                    s["aggregation"] = agg
                else:
                    s.pop("aggregation", None)

    config["stages"] = stages
    config["execution_order"] = [s["name"] for s in stages]
    return config


def _print_plan_summary(config: dict):
    print(f"   Containers  : {list(config['containers'].values())}")
    print(f"   Stages      : {[s['name'] for s in config['stages']]}")
    print(f"   Exec Order  : {config['execution_order']}")
    print(f"   Reasoning   : {config.get('reasoning', 'N/A')}")
    for s in config["stages"]:
        print(f"\n   Stage: {s['name']} (type={s['type']})")
        if s["type"] == "notebook":
            for t in s.get("transformations", []):
                print(f"      transform: {t}")
            if s.get("filter_condition"):
                print(f"      filter:    {s['filter_condition']}")
