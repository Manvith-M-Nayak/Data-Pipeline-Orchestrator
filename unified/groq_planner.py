"""
Unified Groq planner.

Generates a single pipeline config that uses:
  - Azure Data Factory (ADF) as the ORCHESTRATION layer
  - Azure Databricks as the COMPUTE layer

Output config describes multi-stage medallion flow (raw → bronze → silver → ...),
where each stage is either:
  - "copy"     : ADF Copy Activity moves blob-to-blob as-is (ingestion)
  - "notebook" : ADF DatabricksNotebook Activity runs a generated PySpark notebook
                 on Databricks compute (transformation + filter + aggregation)

The Groq LLM picks transformation expressions in ADF-DSL syntax; the downstream
notebook builder converts them to PySpark. If the LLM is unreachable, a deterministic
default config is returned instead.
"""

import json
import requests
from urllib3.exceptions import ProtocolError, HTTPError as HTTPErrorFromUrllib3
from config import GROQ_API_KEY


GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

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
For row filters, set "filter_condition" on the notebook stage. Examples:
  - equals(toInteger(eggs), 1)
  - notEquals(toInteger(status), 0)
  - greater(toInteger(amount), 100)
  - isNull(email)

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

        expected = num_containers - 1
        stages = config.get("stages", [])
        if len(stages) > expected:
            extras = [s["name"] for s in stages[expected:]]
            print(f"   LLM produced extra stages — trimming: {extras}")
            config["stages"] = stages[:expected]
            config["execution_order"] = [
                n for n in config.get("execution_order", []) if n not in extras
            ]

        config = _structural_validate(config, schema)

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


def _structural_validate(config: dict, schema: dict = None) -> dict:
    """Enforce first-stage=copy, later-stages=notebook, processed_time presence,
    and validate any aggregation blocks against the schema."""
    schema = schema or {}
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
