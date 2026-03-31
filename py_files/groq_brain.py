import requests
import json
import re
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
    ["raw", "staging", "curated"],
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

You MUST return JSON in EXACTLY this structure — no extra keys, no markdown.

=== PIPELINE STRUCTURE RULES ===
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
6. Always include a "reasoning" field explaining your decisions
7. Include a "recommended_settings" object showing optimal values
8. Include an "editable_settings" object with all configurable options

=== ADF DATA FLOW EXPRESSION SYNTAX — USE EXACTLY THESE FORMS ===

ONLY use these function names. No Python. No SQL. No pandas.

| Goal                                  | ADF Expression                                      |
|---------------------------------------|-----------------------------------------------------|
| Uppercase a string column             | upper(column_name)                                  |
| Lowercase a string column             | lower(column_name)                                  |
| Conditional / ternary                 | iif(condition, true_value, false_value)             |
| Check integer equality                | equals(toInteger(column_name), 1)                   |
| Check integer inequality              | notEquals(toInteger(column_name), 0)                |
| New binary flag from int column       | iif(equals(toInteger(milk), 1), 1, 0)               |
| Null check                            | iif(isNull(column_name), 'unknown', column_name)    |
| Current timestamp                     | currentTimestamp()                                  |
| Cast to integer                       | toInteger(column_name)                              |
| Cast to string                        | toString(column_name)                               |
| Concatenate two columns               | concat(col1, ' ', col2)                             |
| String length                         | length(column_name)                                 |
| Replace substring                     | replace(column_name, 'old', 'new')                  |

=== CONCRETE EXAMPLES ===

User: "convert animal_name to uppercase"
  -> "animal_name = upper(animal_name)"

User: "create is_mammal column, 1 if milk==1 else 0"
  -> "is_mammal = iif(equals(toInteger(milk), 1), 1, 0)"

User: "add a column that flags rows where backbone is present"
  -> "has_backbone = iif(equals(toInteger(backbone), 1), 1, 0)"

User: "uppercase animal_name AND create is_mammal from milk"
  transformations list:
    "animal_name = upper(animal_name)",
    "is_mammal = iif(equals(toInteger(milk), 1), 1, 0)",
    "processed_time = currentTimestamp()"

=== STRICT RULES ===
1. containers MUST be a dict with keys matching the stage names: {container_list}
2. execution_order MUST contain ONLY pipeline names that exist in the pipelines list
3. Generate transformations using the EXACT column names the user specifies in their
   prompt. Do NOT substitute or map to other columns. Do NOT validate against any
   schema — use the user's words literally.
   CRITICAL: If the user says "aggression_level", you must write aggression_level in
   the expression — even if that column does not exist in any real dataset. Do NOT
   replace it with a similar-sounding column. The validation layer downstream will
   detect unknown column names and trigger the Self-Healing Agent to resolve them.
   Substituting silently bypasses that mechanism entirely.
4. ALWAYS include "processed_time = currentTimestamp()" in transformations
5. Use ONLY the ADF functions listed above — not Python, not SQL
6. Return ONLY the JSON object — no markdown fences, no explanation text
7. Every transformation string MUST follow the pattern:  new_col_name = expression
8. When casting a column to integer, ALWAYS use bare toInteger() with NO null safety wrapper.
   CORRECT:   legs = toInteger(legs)
   INCORRECT: legs = iifNull(toInteger(legs), 0)
   INCORRECT: legs = iif(isNull(legs), 0, toInteger(legs))
   The pipeline validation layer handles null safety separately — do NOT add it here.

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

    columns_list = schema['columns']
    # NOTE: We deliberately do NOT send CSV column names or sample data to Groq.
    # Groq must generate the user's literal intent without knowing what columns
    # exist. The validator catches unknown columns and routes them to the
    # SelfHealingAgent. Sending sample data lets Groq cheat by substituting
    # nearby real column names, which bypasses self-healing entirely.
    user_message = f"""
Row Count   : {schema['row_count']}
Size        : {schema['size_hint']}

User Prompt: "{user_prompt}"

Number of stages: {num_containers}
Container names: {container_list}

Return the complete ADF pipeline configuration JSON:
"""

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_context},
            {"role": "user",   "content": user_message}
        ],
        "temperature": 0.1,
        "max_tokens": 1500,
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
            timeout=30,
        )
    except Exception as e:
        print(f"❌ Groq request failed: {e} → fallback")
        return get_safe_fallback()

    if response.status_code != 200:
        print(f"❌ Groq API error: {response.status_code} — {response.text} → fallback")
        return get_safe_fallback()

    data = response.json()
    raw = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()

    if not raw:
        print("❌ Empty Groq response → fallback")
        return get_safe_fallback()

    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
    raw = raw.strip()

    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        print("❌ Could not find JSON in Groq response → fallback")
        return get_safe_fallback()

    try:
        config = json.loads(match.group(0))

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

        # Validate and fix transformations using known column list
        known_columns = {c.lower() for c in columns_list}
        config = _validate_and_fix_transformations(config, known_columns, user_prompt)

        print("\n✅ Groq decided the following pipeline config:")
        print(f"   Containers  : {list(config['containers'].values())}")
        print(f"   Datasets    : {[d['name'] for d in config['datasets']]}")
        for p in config["pipelines"]:
            print(f"   {p['name']} ({p['type']})")
            if p.get("transformations"):
                for t in p["transformations"]:
                    print(f"      → {t}")
            if p.get("_dropped_transforms"):
                print(f"   ⚠️  {len(p['_dropped_transforms'])} transform(s) could not be validated by planner:")
                for d in p["_dropped_transforms"]:
                    print(f"      NEEDS HEALING: {d}")
        print(f"   Exec Order  : {config['execution_order']}")
        print(f"\n📋 Recommended Settings:")
        for k, v in config.get("recommended_settings", rec).items():
            print(f"      {k}: {v}")
        print(f"\n💡 Reasoning  : {config.get('reasoning', 'N/A')}\n")
        return config
    except json.JSONDecodeError as e:
        print(f"❌ Groq returned invalid JSON: {e} → fallback")
        return get_safe_fallback()


# ============================================================
# POST-PROCESS: validate transformations Groq generated
# ============================================================
def _validate_and_fix_transformations(
    config: dict,
    known_columns: set,
    user_prompt: str,
) -> dict:
    ADF_FUNCTIONS = {
        'currentTimestamp', 'currentDate', 'currentUTC',
        'toDate', 'toTimestamp', 'toString', 'toInteger', 'toLong',
        'toDouble', 'toFloat', 'toBoolean', 'toDecimal',
        'trim', 'ltrim', 'rtrim', 'upper', 'lower', 'initCap',
        'concat', 'substring', 'length', 'replace', 'regexReplace',
        'split', 'startsWith', 'endsWith', 'contains', 'instr',
        'iifNull', 'iif', 'isNull', 'isNaN', 'isInteger', 'isString',
        'coalesce', 'decode',
        'round', 'floor', 'ceil', 'abs', 'sqrt', 'mod', 'power',
        'year', 'month', 'dayOfMonth', 'hour', 'minute', 'second',
        'addDays', 'addMonths', 'dateDiff', 'dayOfWeek', 'dayOfYear',
        'md5', 'sha1', 'sha2', 'uuid',
        'equals', 'notEquals', 'greater', 'less', 'greaterOrEqual',
        'lessOrEqual', 'and', 'or', 'not', 'in',
        'true', 'false', 'null',
        'sum', 'avg', 'min', 'max', 'count', 'countDistinct',
        'first', 'last',
    }

    for p in config.get("pipelines", []):
        if p.get("type") != "dataflow":
            continue

        raw_transforms = p.get("transformations", [])
        fixed   = []
        dropped = []

        for t in raw_transforms:
            if "=" not in t:
                print(f"   ⚠️  Skipping malformed transformation (no '='): {t}")
                dropped.append({"transform": t, "reason": "malformed — no '=' found"})
                continue

            col, expr = t.split("=", 1)
            col  = col.strip()
            expr = expr.strip()

            expr = _fix_common_groq_mistakes(expr, known_columns)
            expr = _strip_null_safety_from_cast(expr)

            tokens = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expr)
            invalid = [
                tk for tk in tokens
                if tk.lower() not in known_columns
                and tk not in ADF_FUNCTIONS
            ]

            if invalid:
                print(f"   ⚠️  Planner cannot validate '{col} = {expr}'")
                print(f"        Unknown tokens: {invalid} — will route to Self-Healing Agent")
                dropped.append({
                    "transform": t,
                    "column":    col,
                    "expr":      expr,
                    "invalid_tokens": invalid,
                    "reason":    f"unknown tokens: {invalid}",
                })
            else:
                fixed.append(f"{col} = {expr}")

        if not any("processed_time" in f for f in fixed):
            fixed.append("processed_time = currentTimestamp()")

        p["transformations"]     = fixed
        p["_dropped_transforms"] = dropped

        if dropped:
            print(f"\n   ❗ {len(dropped)} transform(s) beyond planner's ability — Self-Healing Agent will handle:")
            for d in dropped:
                print(f"      → {d['transform']}")

    return config


def _strip_null_safety_from_cast(expr: str) -> str:
    expr = re.sub(
        r'iifNull\(\s*(toInteger\(\w+\))\s*,\s*[^)]+\)',
        r'\1',
        expr
    )
    expr = re.sub(
        r'iif\(\s*isNull\(\w+\)\s*,\s*[^,]+,\s*(toInteger\(\w+\))\s*\)',
        r'\1',
        expr
    )
    return expr


def _fix_common_groq_mistakes(expr: str, known_columns: set) -> str:
    expr = re.sub(
        r'\b(\w+)\.upper\(\)',
        lambda m: f"upper({m.group(1)})" if m.group(1).lower() in known_columns else m.group(0),
        expr
    )
    expr = re.sub(
        r'\b(\w+)\.lower\(\)',
        lambda m: f"lower({m.group(1)})" if m.group(1).lower() in known_columns else m.group(0),
        expr
    )
    py_ternary = re.compile(
        r'\(\s*1\s+if\s+(\w+)\s*==\s*1\s+else\s+0\s*\)', re.IGNORECASE
    )
    expr = py_ternary.sub(
        lambda m: f"iif(equals(toInteger({m.group(1)}), 1), 1, 0)", expr
    )
    sql_case = re.compile(
        r'CASE\s+WHEN\s+(\w+)\s*=\s*1\s+THEN\s+1\s+ELSE\s+0\s+END', re.IGNORECASE
    )
    expr = sql_case.sub(
        lambda m: f"iif(equals(toInteger({m.group(1)}), 1), 1, 0)", expr
    )
    expr = re.sub(r'\bUPPER\(', 'upper(', expr)
    expr = re.sub(r'\bLOWER\(', 'lower(', expr)
    return expr


# ============================================================
# SAFE FALLBACK
# ============================================================
def get_safe_fallback():
    return {
        "containers": {"raw": "incoming", "stage1": "bronze", "stage2": "silver"},
        "datasets": [
            {"name": "DS_Raw",    "container": "incoming", "filename": "*.csv",      "role": "source"},
            {"name": "DS_Bronze", "container": "bronze",   "filename": "*.csv",      "role": "intermediate"},
            {"name": "DS_Silver", "container": "silver",   "filename": "output.csv", "role": "sink"},
        ],
        "pipelines": [
            {
                "name":           "Pipeline_Raw_to_Bronze",
                "type":           "copy",
                "source_dataset": "DS_Raw",
                "sink_dataset":   "DS_Bronze",
            },
            {
                "name":                "Pipeline_Bronze_to_Silver",
                "type":                "dataflow",
                "source_dataset":      "DS_Bronze",
                "sink_dataset":        "DS_Silver",
                "transformations":     ["processed_time = currentTimestamp()"],
                "_dropped_transforms": [],
            },
        ],
        "execution_order": ["Pipeline_Raw_to_Bronze", "Pipeline_Bronze_to_Silver"],
        "reasoning": "Safe fallback — Groq response was invalid",
    }