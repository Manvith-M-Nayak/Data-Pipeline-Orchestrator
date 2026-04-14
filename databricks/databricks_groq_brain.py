import requests
import json
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


# ── Default config (used when Groq unavailable) ────────────────────────────────
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
            "name": f"DS_{name.title()}",
            "container": name,
            "filename": "" if i == 0 else ("*.csv" if role == "intermediate" else "output.csv"),
            "role": role,
        })

    pipelines = []
    for i in range(num_containers - 1):
        pl_type = "copy" if i == 0 else "transform"
        pipelines.append({
            "name": f"Pipeline_{clist[i].title()}_to_{clist[i+1].title()}",
            "type": pl_type,
            "source_dataset": f"DS_{clist[i].title()}",
            "sink_dataset": f"DS_{clist[i+1].title()}",
            "transformations": ["processed_time = currentTimestamp()"] if pl_type == "transform" else [],
            "filter_condition": None,
            "num_workers": rec["num_workers"],
            "shuffle_partitions": rec["shuffle_partitions"],
        })

    return {
        "containers": containers,
        "datasets": datasets,
        "pipelines": pipelines,
        "containers_to_create": clist,
        "execution_order": [p["name"] for p in pipelines],
        "num_containers": num_containers,
        "recommended_settings": rec,
        "editable_settings": DEFAULT_EDITABLE_SETTINGS,
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

    # Resolve container list
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
            "name": f"DS_{clist[i].title()}",
            "container": clist[i],
            "filename": "" if i == 0 else ("*.csv" if i < num_containers - 1 else "output.csv"),
            "role": "source" if i == 0 else ("sink" if i == num_containers - 1 else "intermediate"),
        }
        for i in range(num_containers)
    ])
    pipelines_template = json.dumps([
        {
            "name": f"Pipeline_{clist[i].title()}_to_{clist[i+1].title()}",
            "type": "copy" if i == 0 else "transform",
            "source_dataset": f"DS_{clist[i].title()}",
            "sink_dataset": f"DS_{clist[i+1].title()}",
            "transformations": ["processed_time = currentTimestamp()"] if i > 0 else [],
            "filter_condition": None,
            "num_workers": rec["num_workers"],
            "shuffle_partitions": rec["shuffle_partitions"],
        }
        for i in range(num_containers - 1)
    ])
    exec_order = json.dumps([
        f"Pipeline_{clist[i].title()}_to_{clist[i+1].title()}"
        for i in range(num_containers - 1)
    ])

    system_context = f"""
You are a Databricks pipeline architect.

Given a CSV schema and a user's natural language prompt, design the COMPLETE Databricks pipeline configuration.
You control container names, dataset names, pipeline names, PySpark transformations, compute settings, and execution order.

Rules:
1. Number of stages (pre-determined): {num_containers}
2. Container/DBFS path names: {clist}
3. Recommended settings for {schema.get('size_hint', 'medium')} data:
   - num_workers: {rec['num_workers']}
   - shuffle_partitions: {rec['shuffle_partitions']}
   - node_type: {rec['node_type']}
4. Pipeline types:
   - "copy": PySpark spark.read.csv → spark.write.csv (no transformations, just stage data)
   - "transform": PySpark with filter and/or derived column logic
5. For "transformations" list, use ADF Data Flow syntax (it will be auto-converted):
   - upper(col), lower(col), trim(col)
   - toInteger(col), toDouble(col), toString(col)
   - currentTimestamp()
   - year(col), month(col), dayOfMonth(col)
   - concat(col1, col2)
   - iifNull(col, 'default')
   - Always include: processed_time = currentTimestamp()
6. For FILTERS, use the "filter_condition" field (NOT in transformations):
   - equals(toInteger(eggs), 1)      — filter where eggs = 1
   - notEquals(toInteger(eggs), 0)   — filter where eggs != 0
   - greater(toInteger(legs), 4)     — filter where legs > 4
   - isNull(column_name)             — filter where column is null
7. If the user prompt contains a filter condition, put it in filter_condition, not transformations.
8. Always include a "reasoning" field.
9. Include "recommended_settings" and "editable_settings".

Return ONLY valid JSON. No markdown, no explanation, no backticks.

JSON structure:
{{
  "containers": {containers_json},
  "datasets": {datasets_json},
  "pipelines": {pipelines_template},
  "containers_to_create": {json.dumps(clist)},
  "execution_order": {exec_order},
  "num_containers": {num_containers},
  "recommended_settings": {{
    "num_workers": {rec['num_workers']},
    "shuffle_partitions": {rec['shuffle_partitions']},
    "node_type": "{rec['node_type']}"
  }},
  "editable_settings": {{
    "num_workers": [2, 4, 8, 16, 32],
    "shuffle_partitions": [4, 8, 16, 32, 64],
    "node_type": ["Standard_DS3_v2", "Standard_DS4_v2", "Standard_D8s_v3"]
  }},
  "reasoning": "Brief explanation"
}}

IMPORTANT: filter_condition must use ADF-style expressions. Examples:
  "equals(toInteger(eggs), 1)"  to filter rows where eggs = 1
  "greater(toInteger(legs), 4)" to filter rows where legs > 4
  "isNull(name)"                to filter rows where name is null
"""

    user_message = f"""
CSV Columns: {schema['columns']}
Column Types: {schema['inferred_types']}
Row Count: {schema['row_count']}
File Size: {schema['size_hint']}

Sample Data:
{json.dumps(schema['samples'], indent=2)}

User Prompt: "{user_prompt}"

Number of stages: {num_containers}
Container names: {clist}

Return the complete Databricks pipeline configuration JSON:
"""

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_context},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.2,
        "max_completion_tokens": 2048,
        "top_p": 0.8,
    }

    print("Groq LLaMA 3.3 70B is designing your Databricks pipeline...")

    try:
        response = requests.post(
            GROQ_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {GROQ_API_KEY}",
            },
            json=payload,
            timeout=30,
        )

        if response.status_code != 200:
            raise requests.exceptions.HTTPError(f"HTTP {response.status_code}: {response.text}")

        raw = response.json()["choices"][0]["message"]["content"].strip()

        # Strip markdown fences if present
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

        print(f"   Containers : {list(config['containers'].values())}")
        print(f"   Pipelines  : {[p['name'] for p in config['pipelines']]}")
        print(f"   Exec Order : {config['execution_order']}")
        print(f"   Reasoning  : {config.get('reasoning', 'N/A')}")
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
