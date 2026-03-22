import requests
import json
from config import GROQ_API_KEY

# Groq API endpoint — completely free, no billing needed
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


# ============================================================
# THE BRAIN: Groq reads the CSV schema + user prompt and
# decides EVERYTHING about the pipeline dynamically
# ============================================================
def decide_pipeline_config(schema: dict, user_prompt: str) -> dict:
    """
    Sends CSV schema + user prompt to Groq LLaMA 3.3 70B.
    Groq returns a complete pipeline configuration as JSON.
    """

    system_context = """
You are an Azure Data Factory (ADF) pipeline architect.

Given a CSV schema and a user's natural language prompt, you must decide the COMPLETE pipeline configuration.
You control everything: container names, dataset names, pipeline names, transformations, compute settings, partitioning, DIU, and execution order.

Rules:
1. Decide container names based on the prompt context (e.g. raw/stage/curated OR incoming/bronze/silver OR landing/processing/output)
2. Decide how many pipelines are needed (usually 2: copy + dataflow, but decide based on the prompt)
3. For "copy" type pipelines: use Copy Activity to move data between containers
4. For "dataflow" type pipelines: use Data Flow Activity with Derived Column transformations
5. Choose compute_type as "MemoryOptimized" for large data or "General" for small data
6. Choose partition_count based on data size hints in the prompt (use 4 for small, 10 for large)
7. Choose core_count: 4 for small datasets, 8 for large datasets
8. For transformations, use ONLY valid ADF Data Flow expression syntax:
   - upper(col), lower(col), trim(col)
   - toInteger(col), toDouble(col), toString(col)
   - iifNull(col, 'default')
   - currentTimestamp()
   - year(col), month(col), dayOfMonth(col)
   - concat(col1, ' ', col2)
   - substring(col, 1, 5)
   - regexReplace(col, '[^a-zA-Z0-9]', '')
   - Always include: processed_time = currentTimestamp()
9. Always include a "reasoning" field explaining your decisions

Return ONLY a valid JSON object. No markdown, no explanation, no backticks.

The JSON must follow this exact structure:
{
  "containers": {
      "raw":    "incoming",
      "stage1": "bronze",
      "stage2": "silver"
  },
  "datasets": [
      { "name": "DS_Raw",    "container": "incoming", "filename": "*.csv",      "role": "source" },
      { "name": "DS_Bronze", "container": "bronze",   "filename": "*.csv",      "role": "intermediate" },
      { "name": "DS_Silver", "container": "silver",   "filename": "output.csv", "role": "sink" }
  ],
  "pipelines": [
      {
          "name": "Pipeline_Raw_to_Bronze",
          "type": "copy",
          "source_dataset":  "DS_Raw",
          "sink_dataset":    "DS_Bronze",
          "merge_files":     true,
          "parallel_copies": 4,
          "diu":             4
      },
      {
          "name": "Pipeline_Bronze_to_Silver",
          "type": "dataflow",
          "source_dataset":  "DS_Bronze",
          "sink_dataset":    "DS_Silver",
          "transformations": [
              "processed_time = currentTimestamp()",
              "name = upper(name)"
          ],
          "partition_count": 10,
          "compute_type":    "MemoryOptimized",
          "core_count":      8
      }
  ],
  "containers_to_create": ["incoming", "bronze", "silver"],
  "execution_order":      ["Pipeline_Raw_to_Bronze", "Pipeline_Bronze_to_Silver"],
  "reasoning":            "Brief explanation of decisions made"
}
"""

    user_message = f"""
CSV Columns: {schema['columns']}
Column Types (inferred): {schema['inferred_types']}
Row Count (approx): {schema['row_count']}
File Size Hint: {schema['size_hint']}

Sample Data:
{json.dumps(schema['samples'], indent=2)}

User Prompt: "{user_prompt}"

Return the complete ADF pipeline configuration JSON:
"""

    # Combine system context + user message for Groq
    full_prompt = system_context + "\n\n" + user_message

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

    # Strip any accidental markdown code fences Groq might add
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
        print("\n✅ Groq decided the following pipeline config:")
        print(f"   Containers  : {list(config['containers'].values())}")
        print(f"   Datasets    : {[d['name'] for d in config['datasets']]}")
        print(f"   Pipelines   : {[p['name'] for p in config['pipelines']]}")
        print(f"   Exec Order  : {config['execution_order']}")
        print(f"\n💡 Reasoning  : {config.get('reasoning', 'N/A')}\n")
        return config
    except json.JSONDecodeError as e:
        print(f"❌ Groq returned invalid JSON: {e}")
        print(f"Raw output:\n{raw}")
        raise