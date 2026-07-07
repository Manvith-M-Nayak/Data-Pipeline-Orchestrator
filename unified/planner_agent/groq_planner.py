"""
Groq planner backend.

Asks Groq's LLaMA 3.3 70B to design the unified ADF + Databricks pipeline
config. Selected when PLANNER_BACKEND="groq". All backend-neutral logic
(container naming, validation, deterministic fallback, settings) lives in
planner_common; this module only owns the Groq API call and its prompt.
"""

import json
import requests
from urllib3.exceptions import ProtocolError, HTTPError as HTTPErrorFromUrllib3
from config import GROQ_API_KEY

from .planner_common import (
    DEFAULT_EDITABLE_SETTINGS,
    MAX_CONTAINERS,
    _build_datasets,
    _build_stages,
    _print_plan_summary,
    _resolve_container_names,
    _structural_validate,
    apply_custom_settings,
    apply_prompt_stage_names,
    build_default_config,
    enforce_container_count,
    get_recommended_settings,
    redistribute_operations,
    required_containers_for_prompt,
)


GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


def decide_pipeline_config(
    schema: dict,
    user_prompt: str,
    num_containers: int = None,
    custom_settings: dict = None,
    container_names: list = None,
) -> tuple:
    """
    Ask Groq LLaMA 3.3 70B to design the unified pipeline config.
    Returns (config_dict, used_fallback_bool).

    Falls back to build_default_config() on any API / JSON error.
    """
    rec = get_recommended_settings(schema.get("size_hint", "medium"))
    if custom_settings:
        rec.update(custom_settings)

    # K numbered stages in the prompt are transformation stages — the copy
    # stage must not consume one of them, so K+2 containers are required.
    needed = required_containers_for_prompt(user_prompt)
    if needed and (num_containers or 0) < needed:
        if num_containers:
            print(f"   Prompt numbers {needed - 2} stage(s) — raising containers {num_containers} → {needed}")
        num_containers = needed

    if num_containers is None:
        num_containers = 3
    num_containers = max(2, min(MAX_CONTAINERS, num_containers))
    clist = _resolve_container_names(num_containers, container_names)

    containers_json = json.dumps({f"stage{i}": clist[i] for i in range(num_containers)})
    datasets_json   = json.dumps(_build_datasets(clist))
    default_stages  = _build_stages(clist, rec)
    stages_json     = json.dumps(default_stages, indent=2)
    exec_order_json = json.dumps([s["name"] for s in default_stages])

    system_context = f"""
You are a hybrid Azure Data Factory + Databricks pipeline architect.

ORCHESTRATION = ADF (control plane).
COMPUTE       = Databricks (execution plane).

ADF drives the pipeline. Each stage is one activity inside a single ADF pipeline:
  - "copy"     : ADF Copy Activity copies blob-to-blob with NO transformation.
                 Use this ONLY for ingestion (stage0 → stage1).
  - "notebook" : ADF DatabricksNotebook Activity runs a PySpark notebook on
                 Databricks. Use this for transformations, filters, aggregations.

NUMBER OF STAGES: {num_containers} (pre-determined, do NOT change).
CONTAINER NAMES : {clist}

RECOMMENDED SETTINGS for {schema.get('size_hint', 'medium')} data:
  diu={rec['diu']}, num_workers={rec['num_workers']}, shuffle_partitions={rec['shuffle_partitions']}

=== TRANSFORMATION EXPRESSION SYNTAX (ADF-DSL) ===
Write transformations as 'output_column = expression' strings. Supported:
  - upper(col), lower(col), trim(col), initCap(col), length(col)
  - toInteger(col), toDouble(col), toString(col), toTimestamp(col)
  - concat(col1, ' ', col2), substring(col, 1, 5)
  - regexReplace(col, 'pattern', 'replacement')
  - iifNull(col, default), isNull(col), coalesce(col, default)
  - year(col), month(col), dayOfMonth(col), currentTimestamp()
  - Arithmetic: col1 + col2, col1 * col2 / 100
  - ALWAYS include: processed_time = currentTimestamp()

=== FILTER SYNTAX ===
For row filters, set "filter_condition" on the notebook stage (ONE condition
per stage — use separate sequential stages for multiple filters). Examples:
  - equals(toInteger(eggs), 1)
  - notEquals(toInteger(status), 0)
  - greater(toInteger(amount), 100)
  - isNull(email)
  - startsWith(animal_name, 'a')
  - endsWith(animal_name, 's')
  - contains(product, 'pro')

=== AGGREGATION SYNTAX (optional, notebook stages only) ===
To group + aggregate, add an "aggregation" object to a notebook stage:
  "aggregation": {{
    "group_by": ["workspace"],
    "aggregations": [
      {{"op": "avg", "column": "duration", "alias": "avg_duration"}},
      {{"op": "count", "column": "*", "alias": "run_count"}}
    ]
  }}
Rules for aggregation:
  - op is one of: avg, sum, min, max, count.
  - avg/sum require a NUMERIC column (integer/double). min/max work on any column.
  - count may use "*" for a row count, or a column name.
  - group_by columns AND aggregated columns MUST exist in the CSV columns above.
  - After aggregation the ONLY surviving columns are the group_by columns plus the
    aliases — downstream stages may reference only those (and processed_time).
  - Aggregation runs AFTER transformations and filter within the same stage.
  - Omit "aggregation" entirely (or set null) for stages that do not group.

=== EXECUTION GROUPS (optional parallelism) ===
"execution_groups" is a list of lists of stage names: groups run in order,
stages inside one group run CONCURRENTLY. Two notebook stages may run in the
same group ONLY if they are independent: they read from the same (or an
already-produced) source container and write to DIFFERENT sink containers
(fan-out). Rules:
  - The copy stage is always alone in the first group.
  - A stage must appear in a group AFTER the stage that produces its source.
  - Max 3 stages per group.
  - For a purely sequential pipeline use one stage per group.

=== RULES ===
1. First stage (stage0 → stage1) MUST be type "copy". No transformations.
2. Subsequent stages MUST be type "notebook".
3. Preserve user-provided column names and function names EXACTLY. Do not fix typos.
4. Always add processed_time = currentTimestamp() to every notebook stage.
5. Output ONLY a valid JSON object. No markdown, no backticks, no commentary.

=== JSON OUTPUT FORMAT ===
{{
  "containers": {containers_json},
  "containers_to_create": {json.dumps(clist)},
  "datasets": {datasets_json},
  "stages": {stages_json},
  "execution_order": {exec_order_json},
  "execution_groups": {json.dumps([[s["name"]] for s in default_stages])},
  "num_containers": {num_containers},
  "recommended_settings": {{
    "diu": {rec['diu']},
    "num_workers": {rec['num_workers']},
    "shuffle_partitions": {rec['shuffle_partitions']},
    "node_type": "{rec['node_type']}"
  }},
  "editable_settings": {json.dumps(DEFAULT_EDITABLE_SETTINGS)},
  "reasoning": "Brief explanation of design decisions"
}}
"""

    user_message = f"""
CSV Columns: {schema['columns']}
Column Types: {schema['inferred_types']}
Row Count: {schema['row_count']}
File Size: {schema['size_hint']}

Sample Data:
{json.dumps(schema['samples'][:3], indent=2)}

User Prompt: "{user_prompt}"

Number of stages: {num_containers}
Container names : {clist}

Design the complete unified ADF+Databricks pipeline configuration JSON:
"""

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_context},
            {"role": "user",   "content": user_message},
        ],
        "temperature": 0.2,
        "max_completion_tokens": 2048,
        "top_p": 0.8,
        "response_format": {"type": "json_object"},
    }

    print("Groq LLaMA 3.3 70B is designing your unified ADF+Databricks pipeline...")

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
            raise requests.exceptions.HTTPError(
                f"HTTP {response.status_code}: {response.text[:300]}"
            )

        raw = response.json()["choices"][0]["message"]["content"].strip()

        if "```" in raw:
            for part in raw.split("```"):
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{"):
                    raw = part
                    break

        config = json.loads(raw.strip())

        config.setdefault("recommended_settings", rec)
        config.setdefault("editable_settings", DEFAULT_EDITABLE_SETTINGS)
        config["num_containers"] = num_containers

        # Enforce the requested container/stage count: trims extra stages the
        # LLM invented, pads with pass-through stages if it produced too few.
        config = enforce_container_count(config, num_containers, container_names, rec)
        # Spread stacked operations into any do-nothing stages — only when
        # the prompt shows distribution intent (numbered stages, "each stage",
        # "distribute", ...); otherwise the model's grouping is respected.
        config = redistribute_operations(config, user_prompt)
        # Explicit user resource settings override whatever the model echoed.
        config = apply_custom_settings(config, custom_settings)
        # Prompt-referenced stage numbers become the notebook stage names.
        config = apply_prompt_stage_names(config, user_prompt)

        config = _structural_validate(config, schema, custom_settings=custom_settings)

        _print_plan_summary(config)
        return config, False

    except json.JSONDecodeError as e:
        print(f"   Groq returned invalid JSON: {e}")
    except Exception as e:
        err = str(e)
        is_network = isinstance(e, (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            ProtocolError,
            HTTPErrorFromUrllib3,
        )) or any(x in err for x in ["Connection", "RemoteDisconnected", "aborted"])
        print(f"   Groq {'network error' if is_network else 'error'}: {e}")

    print("   Falling back to default config...")
    return build_default_config(
        schema, user_prompt,
        num_containers=num_containers,
        custom_settings=custom_settings,
        container_names=container_names,
    ), True
