import requests
import json
from config import GROQ_API_KEY

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


RECOMMENDED_SETTINGS = {
    "small": {
        "compute_type": "General",
        "core_count": 4,
        "partition_count": 2,
        "parallel_copies": 2,
        "diu": 2
    },
    "medium": {
        "compute_type": "General",
        "core_count": 8,
        "partition_count": 4,
        "parallel_copies": 4,
        "diu": 4
    },
    "large": {
        "compute_type": "MemoryOptimized",
        "core_count": 16,
        "partition_count": 8,
        "parallel_copies": 8,
        "diu": 8
    },
    "xlarge": {
        "compute_type": "MemoryOptimized",
        "core_count": 32,
        "partition_count": 16,
        "parallel_copies": 16,
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


def decide_pipeline_config(
    schema: dict,
    user_prompt: str,
    num_containers: int = None,
    custom_settings: dict = None,
    container_names: list = None,
) -> dict:
    """
    Sends CSV schema + user prompt to Groq LLaMA 3.3 70B.
    Groq returns a complete pipeline configuration as JSON.
    
    Args:
        schema: CSV schema with columns, samples, inferred_types, row_count, size_hint
        user_prompt: Natural language prompt describing desired transformations
        num_containers: Number of containers/stages (default: 3, min: 2, max: 5)
        custom_settings: Override recommended settings with custom values
        custom_settings can include:
            - compute_type, core_count, partition_count, parallel_copies, diu
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
            "parallel_copies": rec.get("parallel_copies", 2),
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
   - parallel_copies: {rec.get('parallel_copies', 2)}
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
    "parallel_copies": {rec.get('parallel_copies', 2)},
    "diu": {rec.get('diu', 2)}
  }},
  "editable_settings": {{
    "compute_type": ["General", "MemoryOptimized"],
    "core_count": [4, 8, 16, 32],
    "partition_count": [2, 4, 8, 16, 32, 64],
    "parallel_copies": [1, 2, 4, 8, 16],
    "diu": [1, 2, 4, 8, 16, 32]
  }},
  "reasoning": "Brief explanation of decisions made"
}}
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

    response = requests.post(
        GROQ_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {GROQ_API_KEY}"
        },
        json=payload
    )

    if response.status_code != 200:
        print(f"❌ Groq API error: {response.status_code} — {response.text}")
        response.raise_for_status()

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
                "core_count": [4, 8, 16, 32],
                "partition_count": [2, 4, 8, 16, 32, 64],
                "parallel_copies": [1, 2, 4, 8, 16],
                "diu": [1, 2, 4, 8, 16, 32]
            }
        
        config["num_containers"] = num_containers
        
        print("\n✅ Groq decided the following pipeline config:")
        print(f"   Containers  : {list(config['containers'].values())}")
        print(f"   Datasets    : {[d['name'] for d in config['datasets']]}")
        print(f"   Pipelines   : {[p['name'] for p in config['pipelines']]}")
        print(f"   Exec Order  : {config['execution_order']}")
        print(f"\n📋 Recommended Settings:")
        for k, v in config.get("recommended_settings", rec).items():
            print(f"      {k}: {v}")
        print(f"\n💡 Reasoning  : {config.get('reasoning', 'N/A')}\n")
        return config
    except json.JSONDecodeError as e:
        print(f"❌ Groq returned invalid JSON: {e}")
        print(f"Raw output:\n{raw}")
        raise
