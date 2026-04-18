"""
databricks_groq_brain.py
-------------------------
Groq LLaMA 3.3 70B pipeline planner.

CHANGES from previous version:
  - _validate_pipeline_config() NO LONGER drops transforms silently.
    All transforms are kept and passed to the pre-execution validator
    (databricks_validator.py) which applies proper healing or fails loudly.
  - Removed all _dropped_transforms logic from this file.
  - Groq output is cleaned (syntax, structure) but never dropped here.
"""

import requests
import json
import re
import base64
import time
import datetime
import os
from urllib3.exceptions import ProtocolError, HTTPError as HTTPErrorFromUrllib3
from config import GROQ_API_KEY

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

# ── Default DBFS stage naming ──────────────────────────────────────────────────
CONTAINER_NAMING_CONVENTIONS = [
    ["incoming", "bronze", "silver"],
    ["raw", "stage", "curated"],
    ["landing", "processing", "output"],
    ["raw", "staging", "curated"],
    ["input", "intermediate", "final"],
]

# ── Recommended cluster settings by file size ──────────────────────────────────
RECOMMENDED_SETTINGS = {
    "small":  {"num_workers": 0, "shuffle_partitions": 4,  "node_type": "Standard_D2s_v3"},
    "medium": {"num_workers": 0, "shuffle_partitions": 8,  "node_type": "Standard_D2s_v3"},
    "large":  {"num_workers": 0, "shuffle_partitions": 16, "node_type": "Standard_D2s_v3"},
    "xlarge": {"num_workers": 0, "shuffle_partitions": 32, "node_type": "Standard_D2s_v3"},
}

DEFAULT_EDITABLE_SETTINGS = {
    "num_workers":        [0, 2, 4, 8, 16, 32],
    "shuffle_partitions": [4, 8, 16, 32, 64],
    "node_type": [
        "Standard_D2s_v3", "Standard_D4s_v3", "Standard_D8s_v3",
        "i3.xlarge", "m5.xlarge",
    ],
}


def get_recommended_settings(size_hint: str) -> dict:
    s = size_hint.lower()
    if "small" in s or "< 5" in s:
        return dict(RECOMMENDED_SETTINGS["small"])
    if "medium" in s or "5–50" in s or "5-50" in s:
        return dict(RECOMMENDED_SETTINGS["medium"])
    if "large" in s and "200" not in s:
        return dict(RECOMMENDED_SETTINGS["large"])
    if "xlarge" in s or "> 50" in s or ">200" in s:
        return dict(RECOMMENDED_SETTINGS["xlarge"])
    return dict(RECOMMENDED_SETTINGS["medium"])


# ── Default config ─────────────────────────────────────────────────────────────
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

    num_containers = max(2, min(5, num_containers))

    if container_names and len(container_names) == num_containers:
        clist = container_names
    else:
        clist = ["incoming", "bronze", "silver"][:num_containers]
        while len(clist) < num_containers:
            clist.append(f"stage{len(clist)}")

    containers = {f"stage{i}": clist[i] for i in range(num_containers)}

    datasets = []
    for i, name in enumerate(clist):
        role = "source" if i == 0 else ("sink" if i == num_containers - 1 else "intermediate")
        datasets.append({
            "name":      f"DS_{name.title()}",
            "container": name,
            "filename":  "" if i == 0 else ("*.csv" if role == "intermediate" else "output.csv"),
            "role":      role,
        })

    pipelines = []
    for i in range(num_containers - 1):
        pl_type = "copy" if i == 0 else "transform"
        pipelines.append({
            "name":              f"Pipeline_{clist[i].title()}_to_{clist[i+1].title()}",
            "type":              pl_type,
            "source_dataset":    f"DS_{clist[i].title()}",
            "sink_dataset":      f"DS_{clist[i+1].title()}",
            "transformations":   ["processed_time = currentTimestamp()"] if pl_type == "transform" else [],
            "filter_condition":  None,
            "num_workers":       rec["num_workers"],
            "shuffle_partitions":rec["shuffle_partitions"],
        })

    return {
        "containers":         containers,
        "datasets":           datasets,
        "pipelines":          pipelines,
        "containers_to_create": clist,
        "execution_order":    [p["name"] for p in pipelines],
        "num_containers":     num_containers,
        "recommended_settings": rec,
        "editable_settings":  DEFAULT_EDITABLE_SETTINGS,
        "reasoning": (
            f"Default {num_containers}-stage Databricks pipeline: "
            f"CSV in '{clist[0]}', processed in '{clist[-1]}'. "
            "Copy job moves data, transform job applies PySpark transformations."
        ),
    }


# ── Main Groq decision function ────────────────────────────────────────────────
def decide_pipeline_config(
    schema: dict,
    user_prompt: str,
    num_containers: int = None,
    custom_settings: dict = None,
    container_names: list = None,
) -> tuple:
    """
    Ask Groq LLaMA 3.3 to design the Databricks pipeline config.
    Returns (config_dict, used_fallback_bool).
    """
    rec = get_recommended_settings(schema.get("size_hint", "medium"))
    if custom_settings:
        rec.update(custom_settings)

    if num_containers is None:
        num_containers = 3
    num_containers = max(2, min(5, num_containers))

    clist = container_names if (container_names and len(container_names) == num_containers) else []
    if len(clist) != num_containers:
        for conv in CONTAINER_NAMING_CONVENTIONS:
            if len(conv) >= num_containers:
                clist = conv[:num_containers]
                break
        if len(clist) != num_containers:
            clist = [f"stage{i}" for i in range(num_containers)]

    containers_json = json.dumps({f"stage{i}": clist[i] for i in range(num_containers)})
    datasets_json = json.dumps([
        {
            "name":      f"DS_{clist[i].title()}",
            "container": clist[i],
            "filename":  "" if i == 0 else ("*.csv" if i < num_containers - 1 else "output.csv"),
            "role":      "source" if i == 0 else ("sink" if i == num_containers - 1 else "intermediate"),
        }
        for i in range(num_containers)
    ])
    exec_order = json.dumps([
        f"Pipeline_{clist[i].title()}_to_{clist[i+1].title()}"
        for i in range(num_containers - 1)
    ])

    system_context = f"""
You are a Databricks pipeline architect. Design a complete pipeline configuration.

PIPELINE TYPES AVAILABLE:
- "copy"      : moves data as-is, no transformations
- "transform" : applies column derivations and/or row filters
- "aggregate" : groups rows and computes aggregations (SUM, AVG, COUNT, etc.)

NUMBER OF STAGES: {num_containers}  (pre-determined, do NOT change)
CONTAINER NAMES: {clist}

RECOMMENDED SETTINGS for {schema.get("size_hint", "medium")} data:
  num_workers: {rec["num_workers"]}, shuffle_partitions: {rec["shuffle_partitions"]}

=== TRANSFORMATION EXPRESSION SYNTAX ===

- Use simple expressions like:
  column1 = column2 + column3

- You may reference previously created columns.

- DO NOT attempt to correct column names or function names.
- Use exactly what the user provides.

CRITICAL RULES:
- You MAY use column names from the schema, but DO NOT try to correct or validate user input.
- If the user uses a column name that does not exist, KEEP IT AS IS.
- DO NOT attempt to map or fix column names (e.g., do NOT change qty → quantity).
- DO NOT try to correct function names (e.g., do NOT change toTimestamp → to_timestamp).
- Generate expressions EXACTLY as implied by the user prompt, even if they may be incorrect.
- ALWAYS include: processed_time = currentTimestamp()
- Do NOT use Python variable syntax, pandas, or SQL. Use the DSL above only.

IMPORTANT:
- The system has a downstream self-healing engine that will fix errors.
- Your job is ONLY to generate the intended transformations, NOT to fix them.

STRICT RULE:
- If the user writes a function name (e.g., toTimestamp), you MUST use it exactly as written.
- DO NOT convert it to valid PySpark (e.g., do NOT change toTimestamp → to_timestamp).
- Even if the function is invalid, KEEP IT unchanged.
- DO NOT use your prior knowledge of PySpark to correct function names.
- Preserve user-provided function names exactly.

=== JSON OUTPUT FORMAT ===

Return ONLY valid JSON matching this exact structure:
{{
  "containers": {containers_json},
  "datasets": {datasets_json},
  "pipelines": [
    {{
      "name": "Pipeline_{clist[0].title()}_to_{clist[1].title()}",
      "type": "copy",
      "source_dataset": "DS_{clist[0].title()}",
      "sink_dataset": "DS_{clist[1].title()}",
      "transformations": [],
      "filter_condition": null,
      "num_workers": {rec["num_workers"]},
      "shuffle_partitions": {rec["shuffle_partitions"]}
    }},
    {{
      "name": "Pipeline_{clist[1].title()}_to_{clist[2].title() if len(clist) > 2 else 'Silver'}",
      "type": "transform",
      "source_dataset": "DS_{clist[1].title()}",
      "sink_dataset": "DS_{clist[2].title() if len(clist) > 2 else 'Silver'}",
      "transformations": [
        "derived_column = columnA + columnB",
        "processed_time = currentTimestamp()"
      ],
      "filter_condition": null,
      "num_workers": {rec["num_workers"]},
      "shuffle_partitions": {rec["shuffle_partitions"]}
    }}
  ],
  "containers_to_create": {json.dumps(clist)},
  "execution_order": {exec_order},
  "num_containers": {num_containers},
  "recommended_settings": {{
    "num_workers": {rec["num_workers"]},
    "shuffle_partitions": {rec["shuffle_partitions"]},
    "node_type": "{rec["node_type"]}"
  }},
  "editable_settings": {{
    "num_workers": [0, 2, 4, 8, 16, 32],
    "shuffle_partitions": [4, 8, 16, 32, 64],
    "node_type": ["Standard_D2s_v3", "Standard_D4s_v3", "Standard_D8s_v3"]
  }},
  "reasoning": "Brief explanation"
}}
"""

    user_message = f"""
CSV Columns: columnA, columnB, columnC
Column Types: string, int, double

Row Count: {schema['row_count']}
File Size: {schema['size_hint']}

Sample Data:
{json.dumps(schema['samples'][:3], indent=2)}

User Prompt: "{user_prompt}"

Number of stages: {num_containers}
Container names: {clist}

Design the complete Databricks pipeline configuration JSON:
"""

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_context},
            {"role": "user",   "content": user_message},
        ],
        "temperature": 0.1,
        "max_completion_tokens": 2048,
        "top_p": 0.8,
    }

    print("Groq LLaMA 3.3 70B is designing your Databricks pipeline...")

    try:
        response = requests.post(
            GROQ_URL,
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {GROQ_API_KEY}",
            },
            json=payload,
            timeout=30,
        )

        if response.status_code != 200:
            raise requests.exceptions.HTTPError(f"HTTP {response.status_code}: {response.text}")

        raw = response.json()["choices"][0]["message"]["content"].strip()

        # Strip markdown fences
        if "```" in raw:
            for part in raw.split("```"):
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{"):
                    raw = part
                    break

        config = json.loads(raw.strip())

        # Enforce field defaults
        config.setdefault("recommended_settings", rec)
        config.setdefault("editable_settings", DEFAULT_EDITABLE_SETTINGS)
        config["num_containers"] = num_containers

        # Trim extra pipelines if LLM hallucinated more than expected
        expected = num_containers - 1
        if len(config.get("pipelines", [])) > expected:
            extra_names = [p["name"] for p in config["pipelines"][expected:]]
            print(f"   LLM generated extra pipelines — removing: {extra_names}")
            config["pipelines"] = config["pipelines"][:expected]
            config["execution_order"] = [
                n for n in config.get("execution_order", []) if n not in extra_names
            ]

        # Post-process: structural validation only (no dropping of transforms)
        config = _structural_validate(config, schema)

        print(f"   Containers : {list(config['containers'].values())}")
        print(f"   Pipelines  : {[p['name'] for p in config['pipelines']]}")
        print(f"   Exec Order : {config['execution_order']}")
        print(f"   Reasoning  : {config.get('reasoning', 'N/A')}")

        for p in config["pipelines"]:
            print(f"\n   Pipeline: {p['name']} (type={p['type']})")
            if p.get("transformations"):
                for t in p["transformations"]:
                    print(f"      transform: {t}")
            if p.get("filter_condition"):
                print(f"      filter:    {p['filter_condition']}")
            if p.get("group_by_columns"):
                print(f"      groupBy:   {p['group_by_columns']}")
            if p.get("aggregations"):
                for a in p["aggregations"]:
                    print(f"      agg:       {a}")

        return config, False

    except json.JSONDecodeError as e:
        print(f"   Groq returned invalid JSON: {e}")
        print("   Falling back to default config...")
    except Exception as e:
        err = str(e)
        is_network = isinstance(e, (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            ProtocolError,
            HTTPErrorFromUrllib3,
        )) or any(x in err for x in ["Connection", "RemoteDisconnected", "aborted"])

        if is_network:
            print(f"   Groq network error: {e}")
        else:
            print(f"   Groq error: {e}")
        print("   Falling back to default config...")

    return build_default_config(
        schema, user_prompt,
        num_containers=num_containers,
        custom_settings=custom_settings,
        container_names=container_names,
    ), True


# ============================================================
# STRUCTURAL VALIDATION ONLY — no transform dropping
# ============================================================
def _structural_validate(config: dict, schema: dict) -> dict:
    """
    Only fix structural issues:
    - Ensure processed_time exists in transform pipelines
    - Ensure aggregate pipelines have required keys
    - Skip malformed transforms that have no '=' (log, but do NOT drop transforms
      that have '=' — those go to the pre-execution validator)
    
    NOTE: This function intentionally does NOT drop or modify transform expressions.
    Expression-level validation is done by validate_and_heal_config() in
    databricks_validator.py AFTER this function returns.
    """
    for p in config.get("pipelines", []):
        pl_type = p.get("type", "copy")

        if pl_type == "transform":
            transforms = p.get("transformations", [])
            # Only skip truly empty entries (whitespace-only)
            non_empty = [t for t in transforms if t.strip()]

            # Ensure processed_time exists
            if not any("processed_time" in t for t in non_empty):
                non_empty.append("processed_time = currentTimestamp()")

            p["transformations"] = non_empty

        elif pl_type == "aggregate":
            if "group_by_columns" not in p:
                print(f"   ⚠  Aggregate pipeline '{p['name']}' missing group_by_columns")
                p["group_by_columns"] = []
            if "aggregations" not in p:
                print(f"   ⚠  Aggregate pipeline '{p['name']}' missing aggregations")
                p["aggregations"] = []

            valid_funcs = {"sum", "avg", "mean", "min", "max", "count", "countdistinct", "first", "last"}
            cleaned_aggs = []
            for agg in p.get("aggregations", []):
                func = agg.get("function", "sum").lower()
                if func not in valid_funcs:
                    print(f"   ⚠  Unknown aggregation function '{func}' — defaulting to sum")
                    agg["function"] = "sum"
                cleaned_aggs.append(agg)
            p["aggregations"] = cleaned_aggs

    return config