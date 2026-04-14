import requests
import json
from urllib3.exceptions import ProtocolError, HTTPError as HTTPErrorFromUrllib3
from config import GROQ_API_KEY

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

DEFAULT_CONTAINERS = {
    "stage0": "incoming",
    "stage1": "bronze", 
    "stage2": "silver"
}

DEFAULT_DATASETS = [
    {"name": "DS_Incoming", "container": "incoming", "filename": "", "role": "source"},
    {"name": "DS_Bronze", "container": "bronze", "filename": "*.csv", "role": "intermediate"},
    {"name": "DS_Silver", "container": "silver", "filename": "output.csv", "role": "sink"}
]

DEFAULT_PIPELINES = [
    {
        "name": "Pipeline_Incoming_to_Bronze",
        "type": "copy",
        "source_dataset": "DS_Incoming",
        "sink_dataset": "DS_Bronze",
        "diu": 2,
        "transformations": [],
        "partition_count": 2,
        "compute_type": "General",
        "core_count": 4
    },
    {
        "name": "Pipeline_Bronze_to_Silver",
        "type": "dataflow",
        "source_dataset": "DS_Bronze",
        "sink_dataset": "DS_Silver",
        "diu": 2,
        "transformations": ["processed_time = currentTimestamp()"],
        "partition_count": 4,
        "compute_type": "General",
        "core_count": 4
    }
]

DEFAULT_EXECUTION_ORDER = [
    "Pipeline_Incoming_to_Bronze",
    "Pipeline_Bronze_to_Silver"
]

DEFAULT_EDITABLE_SETTINGS = {
    "compute_type": ["General"],
    "core_count": [2, 4, 8, 16, 32, 64],
    "partition_count": [2, 4, 8, 16, 32, 64],
    "diu": [1, 2, 4, 8, 16, 32]
}


RECOMMENDED_SETTINGS = {
    "small": {
        "compute_type": "General",
        "core_count": 4,
        "partition_count": 2,
        "diu": 2
    },
    "medium": {
        "compute_type": "General",
        "core_count": 8,
        "partition_count": 4,
        "diu": 4
    },
    "large": {
        "compute_type": "General",
        "core_count": 16,
        "partition_count": 8,
        "diu": 8
    },
    "xlarge": {
        "compute_type": "General",
        "core_count": 32,
        "partition_count": 16,
        "diu": 16
    }
}

CONTAINER_NAMING_CONVENTIONS = [
    ["incoming", "bronze", "silver"],
    ["raw", "stage", "curated"],
    ["landing", "processing", "output"],
    ["raw", "staging", " curated"],
    ["input", "intermediate", "final"],
]


def get_recommended_settings(size_hint: str) -> dict:
    """Return recommended settings based on file size."""
    size_key = size_hint.lower().replace(" ", "").replace("<", "").replace(">", "").replace("mb", "").replace("gb", "")
    
    if "small" in size_key or "<5mb" in size_key or "5mb" in size_key:
        return RECOMMENDED_SETTINGS["small"]
    elif "medium" in size_key or "5mb50mb" in size_key or "50mb" in size_key:
        return RECOMMENDED_SETTINGS["medium"]
    elif "large" in size_key and "50mb" not in size_key:
        return RECOMMENDED_SETTINGS["large"]
    elif ">50mb" in size_key or "xlarge" in size_key:
        return RECOMMENDED_SETTINGS["xlarge"]
    
    return RECOMMENDED_SETTINGS["medium"]


def build_default_config(schema: dict, user_prompt: str, num_containers: int = 3, custom_settings: dict = None, container_names: list = None) -> dict:
    """
    Build a default 3-stage pipeline configuration without using Groq API.
    Uses sensible defaults based on file size.
    """
    rec = get_recommended_settings(schema.get("size_hint", "medium"))
    
    if custom_settings:
        rec.update(custom_settings)
    
    num_containers = max(2, min(5, num_containers))
    
    if container_names and len(container_names) == num_containers:
        container_list = container_names
    else:
        container_list = ["incoming", "bronze", "silver"][:num_containers]
        if len(container_list) < num_containers:
            container_list.extend([f"stage{i}" for i in range(len(container_list), num_containers)])
    
    containers = {f"stage{i}": container_list[i] for i in range(num_containers)}
    
    datasets = []
    for i in range(num_containers):
        role = "source" if i == 0 else ("intermediate" if i < num_containers - 1 else "sink")
        datasets.append({
            "name": f"DS_{container_list[i].title()}",
            "container": container_list[i],
            "filename": "" if i == 0 else ("*.csv" if i < num_containers - 1 else "output.csv"),
            "role": role
        })
    
    pipelines = []
    for i in range(num_containers - 1):
        pipeline = {
            "name": f"Pipeline_{container_list[i].title()}_to_{container_list[i+1].title()}",
            "type": "copy" if i == 0 else "dataflow",
            "source_dataset": f"DS_{container_list[i].title()}",
            "sink_dataset": f"DS_{container_list[i+1].title()}",
            "diu": rec.get("diu", 2),
            "transformations": ["processed_time = currentTimestamp()"] if i > 0 else [],
            "partition_count": rec.get("partition_count", 4),
            "compute_type": rec.get("compute_type", "General"),
            "core_count": rec.get("core_count", 4)
        }
        pipelines.append(pipeline)
    
    execution_order = [p["name"] for p in pipelines]
    
    reasoning = f"Default 3-stage pipeline: raw data in '{container_list[0]}', processed in '{container_list[1]}', output in '{container_list[2]}'. Copy pipeline moves data, Data Flow applies transformations."
    
    return {
        "containers": containers,
        "datasets": datasets,
        "pipelines": pipelines,
        "containers_to_create": container_list,
        "execution_order": execution_order,
        "num_containers": num_containers,
        "recommended_settings": rec,
        "editable_settings": DEFAULT_EDITABLE_SETTINGS,
        "reasoning": reasoning
    }


def decide_pipeline_config(
    schema: dict,
    user_prompt: str,
    num_containers: int = None,
    custom_settings: dict = None,
    container_names: list = None,
) -> tuple[dict, bool]:
    """
    Sends CSV schema + user prompt to Groq LLaMA 3.3 70B.
    Groq returns a complete pipeline configuration as JSON.
    Falls back to default config if API is unavailable.
    
    Returns:
        tuple: (config_dict, used_fallback_bool)
    
    Args:
        schema: CSV schema with columns, samples, inferred_types, row_count, size_hint
        user_prompt: Natural language prompt describing desired transformations
        num_containers: Number of containers/stages (default: 3, min: 2, max: 5)
        custom_settings: Override recommended settings with custom values
        custom_settings can include:
            - compute_type, core_count, partition_count, diu
        container_names: Custom container names (list of strings)
    """
    
    rec = get_recommended_settings(schema.get("size_hint", "medium"))
    
    if custom_settings:
        rec.update(custom_settings)
    
    if num_containers is None:
        num_containers = 3
    num_containers = max(2, min(5, num_containers))
    
    container_list = container_names if container_names else []
    if len(container_list) != num_containers:
        for conv in CONTAINER_NAMING_CONVENTIONS:
            if len(conv) >= num_containers:
                container_list = conv[:num_containers]
                break
        if not container_list:
            container_list = [f"stage{i}" for i in range(num_containers)]
    
    containers_json = json.dumps({f"stage{i}": container_list[i] for i in range(num_containers)})
    datasets_json = json.dumps([
        {
            "name": f"DS_{container_list[0].title()}",
            "container": container_list[0],
            "filename": "",
            "role": "source"
        }
    ] + [
        {
            "name": f"DS_{container_list[i].title()}",
            "container": container_list[i],
            "filename": "*.csv" if i < num_containers - 1 else "output.csv",
            "role": "intermediate" if i < num_containers - 1 else "sink"
        }
        for i in range(1, num_containers)
    ])
    
    pipelines_json = json.dumps([
        {
            "name": f"Pipeline_{container_list[i].title()}_to_{container_list[i+1].title()}",
            "type": "copy" if i == 0 else "dataflow",
            "source_dataset": f"DS_{container_list[i].title()}",
            "sink_dataset": f"DS_{container_list[i+1].title()}",
            "diu": rec.get("diu", 2),
            "transformations": ["processed_time = currentTimestamp()"] if i > 0 else [],
            "partition_count": rec.get("partition_count", 4),
            "compute_type": rec.get("compute_type", "General"),
            "core_count": rec.get("core_count", 4)
        }
        for i in range(num_containers - 1)
    ])
    
    execution_order_json = json.dumps([
        f"Pipeline_{container_list[i].title()}_to_{container_list[i+1].title()}"
        for i in range(num_containers - 1)
    ])

    system_context = f"""
You are an Azure Data Factory (ADF) pipeline architect.

Given a CSV schema and a user's natural language prompt, you must decide the COMPLETE pipeline configuration.
You control everything: container names, dataset names, pipeline names, transformations, compute settings, partitioning, DIU, and execution order.

Rules:
1. The number of containers/stages has been PRE-DETERMINED: {num_containers} stages
2. Container names: {container_list}
3. Recommended settings for this data size ({schema.get('size_hint', 'medium')}):
   - compute_type: {rec.get('compute_type', 'General')}
   - core_count: {rec.get('core_count', 4)}
   - partition_count: {rec.get('partition_count', 4)}
   - diu: {rec.get('diu', 2)}
   These can be adjusted based on the user's prompt or data characteristics.
4. For "copy" type pipelines: use Copy Activity to move data between containers
5. For "dataflow" type pipelines: use Data Flow Activity with Derived Column transformations
   6. For transformations, use ONLY valid ADF Data Flow expression syntax:
    - upper(col), lower(col), trim(col)
    - toInteger(col), toDouble(col), toString(col)
    - iifNull(col, 'default')
    - currentTimestamp()
    - year(col), month(col), dayOfMonth(col)
    - concat(col1, ' ', col2)
    - substring(col, 1, 5)
    - regexReplace(col, '[^a-zA-Z0-9]', '')
    - Always include: processed_time = currentTimestamp()
 6b. For FILTERS, use the "filter_condition" field in the pipeline config. 
    The filter_condition should be a valid ADF expression like:
    - equals(toInteger(eggs), 1)  -- to filter rows where eggs = 1
    - notEquals(toInteger(eggs), 0)  -- to filter where eggs is not 0
    - isNull(legs)  -- to filter rows where legs is null
 6c. If the user prompt describes a filter (e.g. "filter animals that lay eggs"), 
    you MUST set filter_condition in the pipeline config, NOT in transformations.
 7. Always include a "reasoning" field explaining your decisions
8. Include a "recommended_settings" object showing optimal values
9. Include an "editable_settings" object with all configurable options

Return ONLY a valid JSON object. No markdown, no explanation, no backticks.

The JSON must follow this exact structure:
{{
  "containers": {containers_json},
  "datasets": {datasets_json},
  "pipelines": {pipelines_json},
  "containers_to_create": {json.dumps(container_list)},
  "execution_order": {execution_order_json},
  "recommended_settings": {{
    "compute_type": "{rec.get('compute_type', 'General')}",
    "core_count": {rec.get('core_count', 4)},
    "partition_count": {rec.get('partition_count', 4)},
    "diu": {rec.get('diu', 2)}
  }},
  "editable_settings": {{
    "compute_type": ["General"],
    "core_count": [2, 4, 8, 16, 32, 64],
    "partition_count": [2, 4, 8, 16, 32, 64],
    "diu": [1, 2, 4, 8, 16, 32]
  }},
  "reasoning": "Brief explanation of decisions made"
}}

IMPORTANT: If the user prompt contains a filter (like "filter rows where X", "only include where Y", "where eggs = 1"),
you MUST add a "filter_condition" field to each pipeline that needs filtering.
Example filter_condition values:
- "equals(toInteger(eggs), 1)" to filter eggs = 1
- "notEquals(toInteger(eggs), 0)" to filter eggs != 0
- "greater(toInteger(legs), 4)" to filter legs > 4
"""

    user_message = f"""
CSV Columns: {schema['columns']}
Column Types (inferred): {schema['inferred_types']}
Row Count (approx): {schema['row_count']}
File Size Hint: {schema['size_hint']}

Sample Data:
{json.dumps(schema['samples'], indent=2)}

User Prompt: "{user_prompt}"

Number of stages: {num_containers}
Container names: {container_list}

Return the complete ADF pipeline configuration JSON:
"""

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_context},
            {"role": "user", "content": user_message}
        ],
        "temperature": 0.2,
        "max_completion_tokens": 2048,
        "top_p": 0.8
    }

    print("🤖 Groq LLaMA 3.3 70B is analyzing your data and prompt...")

    try:
        response = requests.post(
            GROQ_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {GROQ_API_KEY}"
            },
            json=payload,
            timeout=30
        )

        if response.status_code != 200:
            raise requests.exceptions.HTTPError(f"HTTP {response.status_code}: {response.text}")

        raw = response.json()["choices"][0]["message"]["content"].strip()

        if "```" in raw:
            parts = raw.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{"):
                    raw = part
                    break

        raw = raw.strip()

        try:
            config = json.loads(raw)
            
            if "recommended_settings" not in config:
                config["recommended_settings"] = rec
            if "editable_settings" not in config:
                config["editable_settings"] = {
                    "compute_type": ["General", "MemoryOptimized"],
                    "core_count": [2, 4, 8, 16, 32, 64],
                    "partition_count": [2, 4, 8, 16, 32, 64],
                    "diu": [1, 2, 4, 8, 16, 32]
                }
            
            config["num_containers"] = num_containers
            
            expected_pipelines = num_containers - 1
            if len(config.get("pipelines", [])) > expected_pipelines:
                removed = config["pipelines"][expected_pipelines:]
                removed_names = [p["name"] for p in removed]
                print(f"⚠️  LLM generated {len(config['pipelines'])} pipelines (expected {expected_pipelines}). Removing extra: {removed_names}")
                config["pipelines"] = config["pipelines"][:expected_pipelines]
                config["execution_order"] = [name for name in config.get("execution_order", []) if name not in removed_names]
            
            print("\n✅ Groq decided the following pipeline config:")
            print(f"   Containers  : {list(config['containers'].values())}")
            print(f"   Datasets    : {[d['name'] for d in config['datasets']]}")
            print(f"   Pipelines   : {[p['name'] for p in config['pipelines']]}")
            print(f"   Exec Order  : {config['execution_order']}")
            print(f"\n📋 Recommended Settings:")
            for k, v in config.get("recommended_settings", rec).items():
                print(f"      {k}: {v}")
            print(f"\n💡 Reasoning  : {config.get('reasoning', 'N/A')}\n")
            return config, False
        except json.JSONDecodeError as e:
            raise Exception(f"Groq returned invalid JSON: {e}")
            
    except Exception as e:
        error_str = str(e)
        is_network_error = (
            isinstance(e, (requests.exceptions.ConnectionError,
                          requests.exceptions.Timeout,
                          ProtocolError,
                          HTTPErrorFromUrllib3)) or
            "Connection" in error_str or
            "RemoteDisconnected" in error_str or
            "Connection aborted" in error_str
        )
        
        if is_network_error:
            print(f"⚠️  Groq API network error: {e}")
            print("🔄 Falling back to default 3-stage pipeline configuration...")
        else:
            print(f"⚠️  Groq API unavailable or error: {e}")
            print("🔄 Falling back to default 3-stage pipeline configuration...")
        
        return build_default_config(
            schema,
            user_prompt,
            num_containers=num_containers,
            custom_settings=custom_settings,
            container_names=container_names
        ), True
