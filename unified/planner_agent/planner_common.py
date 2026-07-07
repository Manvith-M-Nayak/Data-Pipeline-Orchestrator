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
    if "xlarge" in s or "> 200" in s or ">200" in s:
        return dict(RECOMMENDED_SETTINGS["xlarge"])
    if "small" in s or "< 5" in s or "<5" in s:
        return dict(RECOMMENDED_SETTINGS["small"])
    if "large" in s or "50–200" in s or "50-200" in s:
        return dict(RECOMMENDED_SETTINGS["large"])
    if "medium" in s or "5–50" in s or "5-50" in s:
        return dict(RECOMMENDED_SETTINGS["medium"])
    return dict(RECOMMENDED_SETTINGS["medium"])


def _resolve_container_names(num_containers: int, container_names: list) -> list:
    if container_names and len(container_names) == num_containers:
        return container_names
    for conv in CONTAINER_NAMING_CONVENTIONS:
        if len(conv) >= num_containers:
            return conv[:num_containers]
    return [f"stage{i}" for i in range(num_containers)]


def _dataset_name(container: str) -> str:
    """ADF dataset name for a container (ADF names disallow hyphens)."""
    return f"DS_{str(container).title().replace('_', '').replace('-', '')}"


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
            "name":      _dataset_name(name),
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
                "source_dataset": _dataset_name(src_container),
                "sink_dataset":   _dataset_name(sink_container),
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


def apply_prompt_stage_names(config: dict, user_prompt: str) -> dict:
    """If the user's prompt references numbered stages ("Stage 1: ...",
    "step 2 ..."), rename the notebook stages to match that vocabulary
    (Stage_1, Stage_2, ...) so the plan mirrors the user's own naming.
    The first (copy/ingest) stage always keeps its generated name.
    execution_order / execution_groups references are renamed in step."""
    if not user_prompt or not isinstance(user_prompt, str):
        return config
    nums = []
    for m in re.finditer(r"\b(?:stage|step)\s*[-#]?\s*(\d+)", user_prompt, re.IGNORECASE):
        if m.group(1) not in nums:
            nums.append(m.group(1))
    if not nums:
        return config

    stages = config.get("stages", [])
    notebooks = [s for s in stages if s.get("type") != "copy"]
    used = {s.get("name") for s in stages}
    mapping = {}
    for s, n in zip(notebooks, nums):
        new_name = f"Stage_{n}"
        if new_name == s.get("name") or new_name in used:
            continue
        mapping[s.get("name")] = new_name
        used.add(new_name)
        s["name"] = new_name
    if not mapping:
        return config

    config["execution_order"] = [mapping.get(n, n) for n in config.get("execution_order", [])]
    if isinstance(config.get("execution_groups"), list):
        config["execution_groups"] = [
            [mapping.get(n, n) for n in g] if isinstance(g, list) else g
            for g in config["execution_groups"]
        ]
    return config


def _stage_op_load(s: dict) -> int:
    """How many meaningful operations a notebook stage performs."""
    if s.get("type") != "notebook":
        return 0
    transforms = [t for t in (s.get("transformations") or [])
                  if t and "processed_time" not in t]
    load = len(transforms)
    if s.get("filter_condition"):
        load += 1
    if (s.get("aggregation") or {}).get("aggregations"):
        load += 1
    return load


# Signals that the user wants the work spread over the stages: numbered
# stages ("Stage 1: ...", "step 2 ..."), per-stage phrasing, or explicit
# distribute/split language.
_DISTRIBUTE_HINTS = re.compile(
    r"\b(?:stage|step)\s*[-#]?\s*\d+"
    r"|\b(?:each|every|separate|individual)\s+(?:stage|step)s?\b"
    r"|\bone\s+(?:operation|transformation|filter|step)\s+per\s+(?:stage|step)\b"
    r"|\bdistribute\b|\bsplit\s+(?:across|into|over)\b",
    re.IGNORECASE,
)


def prompt_requests_distribution(user_prompt) -> bool:
    return bool(user_prompt) and bool(_DISTRIBUTE_HINTS.search(str(user_prompt)))


def required_containers_for_prompt(user_prompt) -> int:
    """Containers needed to honor the stages the user numbered in the prompt.

    K numbered stages ("Stage 1..K") are transformation stages — the ADF copy
    stage must NOT consume one of them: copy + K notebooks = K+1 stages =
    K+2 containers. Returns 0 when the prompt numbers no stages."""
    if not user_prompt:
        return 0
    nums = {m.group(1) for m in re.finditer(
        r"\b(?:stage|step)\s*[-#]?\s*(\d+)", str(user_prompt), re.IGNORECASE)}
    return min(MAX_CONTAINERS, len(nums) + 2) if nums else 0


def redistribute_operations(config: dict, user_prompt: str = None) -> dict:
    """Spread stacked operations into pass-through stages.

    The LLM often piles every transformation/filter into one notebook stage
    and leaves later stages doing nothing (especially after padding to a
    user-requested stage count). A stage's output is a full CSV, so a later
    operation can always move one stage forward: repeatedly shift the LAST
    operation (aggregation, then filter, then the tail half of transforms —
    matching the transforms → filter → aggregation execution order) from any
    stage doing ≥2 things into an adjacent do-nothing stage.

    Only applied when the user asked for it: user_prompt (when given) must
    show distribution intent — numbered stages, per-stage phrasing, or
    distribute/split language. Pass user_prompt=None to force it."""
    if user_prompt is not None and not prompt_requests_distribution(user_prompt):
        return config
    stages = config.get("stages", [])
    moved_any = True
    while moved_any:
        moved_any = False
        # Fill empty stages right-to-left so the last operation of a loaded
        # stage lands in the last empty stage (keeps transforms → filter →
        # aggregation execution order intact across the chain).
        for i in range(len(stages) - 1, 0, -1):
            tgt = stages[i]
            if tgt.get("type") != "notebook" or _stage_op_load(tgt) != 0:
                continue
            # Nearest loaded stage to the left, crossing only empty stages —
            # crossing a stage that does work would reorder operations.
            j = i - 1
            while j > 0 and stages[j].get("type") == "notebook" and _stage_op_load(stages[j]) == 0:
                j -= 1
            src = stages[j]
            if src.get("type") != "notebook" or _stage_op_load(src) < 2:
                continue

            agg = src.get("aggregation")
            if agg and agg.get("aggregations"):
                tgt["aggregation"] = agg
                src.pop("aggregation", None)
            elif src.get("filter_condition"):
                tgt["filter_condition"] = src["filter_condition"]
                src["filter_condition"] = None
            else:
                transforms = [t for t in (src.get("transformations") or [])
                              if t and "processed_time" not in t]
                if len(transforms) < 2:
                    continue
                half = len(transforms) // 2
                src["transformations"] = transforms[:half] + ["processed_time = currentTimestamp()"]
                tgt["transformations"] = transforms[half:] + ["processed_time = currentTimestamp()"]
            print(f"   Redistributing work: '{src.get('name')}' → '{tgt.get('name')}'")
            moved_any = True
    return config


def apply_custom_settings(config: dict, custom_settings: dict) -> dict:
    """Explicit user resource settings override whatever the LLM produced —
    both the top-level recommendation and every stage's own values. Without
    this the model's echoed values silently win over the user's choices."""
    if not custom_settings:
        return config
    rec = dict(config.get("recommended_settings") or {})
    rec.update(custom_settings)
    config["recommended_settings"] = rec
    for s in config.get("stages", []):
        if s.get("type") == "copy" and "diu" in custom_settings:
            s["diu"] = custom_settings["diu"]
        elif s.get("type") == "notebook":
            if "num_workers" in custom_settings:
                s["num_workers"] = custom_settings["num_workers"]
            if "shuffle_partitions" in custom_settings:
                s["shuffle_partitions"] = custom_settings["shuffle_partitions"]
    return config


def enforce_container_count(
    config: dict,
    num_containers: int,
    container_names: list = None,
    rec: dict = None,
) -> dict:
    """Coerce an LLM-produced config to the user-requested container count.

    The fine-tuned (Ollama) model has a fixed contract and picks its own stage
    count, so a user request for N containers must be enforced afterwards:
      - model produced fewer  → extend the chain with pass-through notebook
        stages (processed_time only) until the count matches
      - model produced more   → trim trailing stages and rewire the last kept
        stage to the final container
      - container_names given → positional rename across the whole config
    Containers/datasets/execution_order are rebuilt; execution_groups is
    dropped so _structural_validate re-derives it for the new stage set.
    """
    rec = rec or get_recommended_settings("medium")
    target = max(2, min(MAX_CONTAINERS, int(num_containers)))
    clist = list(config.get("containers_to_create")
                 or config.get("containers", {}).values())
    if not clist:
        return config
    stages = config.get("stages", [])

    if len(clist) > target:
        print(f"   Planner produced {len(clist)} containers — trimming to {target}")
        clist = clist[:target]
        stages = stages[: max(1, target - 1)]
        last = stages[-1]
        if last.get("type") == "notebook":
            last["sink_container"] = clist[-1]
        else:
            last["sink_dataset"] = _dataset_name(clist[-1])

    if len(clist) < target:
        print(f"   Planner produced {len(clist)} containers — extending to {target}")
        used = set(clist)
        pool = []
        for conv in CONTAINER_NAMING_CONVENTIONS:
            pool.extend(n for n in conv if n not in used and n not in pool)
        while len(clist) < target:
            new_name = pool.pop(0) if pool else f"stage{len(clist)}"
            src = clist[-1]
            clist.append(new_name)
            stages.append({
                "name": f"Transform_{src.title()}_To_{new_name.title()}".replace("-", ""),
                "type": "notebook",
                "source_container": src,
                "sink_container":   new_name,
                "transformations":  ["processed_time = currentTimestamp()"],
                "filter_condition": None,
                "num_workers":        rec["num_workers"],
                "shuffle_partitions": rec["shuffle_partitions"],
            })

    if container_names and len(container_names) == target and list(container_names) != clist:
        mapping = dict(zip(clist, container_names))
        for s in stages:
            if s.get("source_container"):
                s["source_container"] = mapping.get(s["source_container"], s["source_container"])
            if s.get("sink_container"):
                s["sink_container"] = mapping.get(s["sink_container"], s["sink_container"])
        clist = list(container_names)

    # The chain starts with the copy stage; its dataset refs follow positions
    # 0 → 1 regardless of any rename above.
    if stages and stages[0].get("type") == "copy" and len(clist) >= 2:
        stages[0]["source_dataset"] = _dataset_name(clist[0])
        stages[0]["sink_dataset"]   = _dataset_name(clist[1])

    config["containers_to_create"] = clist
    config["containers"]     = {f"stage{i}": c for i, c in enumerate(clist)}
    config["datasets"]       = _build_datasets(clist)
    config["num_containers"] = len(clist)
    config["stages"]          = stages
    config["execution_order"] = [s.get("name") for s in stages]
    config.pop("execution_groups", None)
    return config


def _stage_dep_graph(stages: list) -> dict:
    """stage name -> set of upstream stage names (whose sink feeds its source).

    Copy stages reference datasets (DS_RawData), notebook stages reference
    containers (raw-data); normalize both to a bare comparable token.
    """
    def _norm(token) -> str:
        t = str(token or "").lower().strip()
        if t.startswith("ds_"):
            t = t[3:]
        return t.replace("-", "").replace("_", "")

    sinks = {}
    for s in stages:
        tok = _norm(s.get("sink_container") or s.get("sink_dataset"))
        if tok:
            sinks[tok] = s.get("name")

    deps = {}
    for s in stages:
        name = s.get("name")
        src = _norm(s.get("source_container") or s.get("source_dataset"))
        up = sinks.get(src)
        deps[name] = {up} if up and up != name else set()
    return deps


def sanitize_execution_groups(config: dict, groups: list = None) -> dict:
    """Validate/repair execution_groups (user- or LLM-provided) on a config.

    execution_groups is a list of lists of stage names: groups run in order,
    stages inside one group run concurrently. Guarantees after this call:
      - every stage appears exactly once
      - copy stages run first, each in its own group (single ADF pipeline)
      - no stage runs in the same group as (or before) a stage it depends on
      - stages and execution_order are reordered to the flattened group order
    Absent or unusable input degrades to fully sequential groups.
    """
    stages = config.get("stages", [])
    names = [s.get("name") for s in stages]
    by_name = {s.get("name"): s for s in stages}

    raw = groups if groups is not None else config.get("execution_groups")
    if not isinstance(raw, list) or not raw:
        raw = [[n] for n in names]

    # Keep only known stage names, first occurrence wins; missing stages are
    # appended as their own trailing groups.
    seen, cleaned = set(), []
    for g in raw:
        if not isinstance(g, list):
            g = [g]
        keep = [n for n in g if n in by_name and n not in seen]
        seen.update(keep)
        if keep:
            cleaned.append(keep)
    for n in names:
        if n not in seen:
            cleaned.append([n])

    # Copy stages always lead, one group each (they run as one ADF pipeline
    # before any Databricks work).
    copy_names = [n for n in names if by_name[n].get("type") == "copy"]
    cleaned = [[n] for n in copy_names] + [
        [m for m in g if m not in copy_names] for g in cleaned
    ]
    cleaned = [g for g in cleaned if g]

    # Dependency-aware repair: honor the requested grouping wherever the data
    # flow allows it, split it where it does not.
    group_index = {}
    for gi, g in enumerate(cleaned):
        for n in g:
            group_index[n] = gi

    deps = _stage_dep_graph(stages)
    remaining = [n for g in cleaned for n in g]
    resolved, final = set(), []
    while remaining:
        ready = [n for n in remaining if deps.get(n, set()) <= resolved]
        if not ready:
            ready = [remaining[0]]     # dependency cycle — force progress
        gi_min = min(group_index[n] for n in ready)
        batch = [n for n in ready if group_index[n] == gi_min]
        if len(batch) < len([n for n in cleaned[gi_min] if n in remaining]):
            dropped = [n for n in cleaned[gi_min] if n in remaining and n not in batch]
            print(f"   execution_groups: deferring {dropped} (depend on stages in the same group)")
        final.append(batch)
        resolved.update(batch)
        remaining = [n for n in remaining if n not in batch]

    flat = [n for g in final for n in g]
    config["stages"] = [by_name[n] for n in flat]
    config["execution_order"] = flat
    config["execution_groups"] = final
    return config


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

    config = {
        "containers":           containers,
        "containers_to_create": clist,
        "datasets":             datasets,
        "stages":               stages,
        "execution_order":      execution_order,
        "execution_groups":     [[name] for name in execution_order],
        "num_containers":       num_containers,
        "recommended_settings": rec,
        "editable_settings":    DEFAULT_EDITABLE_SETTINGS,
        "reasoning": (
            f"Default {num_containers}-stage unified pipeline. "
            f"ADF Copy Activity ingests '{clist[0]}' → '{clist[1]}'. "
            f"Remaining stages run as Databricks notebooks invoked by ADF."
        ),
    }
    return apply_prompt_stage_names(config, user_prompt)


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

    mapping, used = {}, set()
    cleaned = []
    for orig in originals:
        base = _sanitize_container_name(orig)
        clean, n = base, 1
        while clean in used:
            n += 1
            clean = _sanitize_container_name(f"{base}-{n}")
        used.add(clean)
        cleaned.append(clean)
        mapping[orig] = clean

    remap = lambda v: mapping.get(v, _sanitize_container_name(v)) if v else v

    config["containers_to_create"] = cleaned
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


def _structural_validate(config: dict, schema: dict = None, custom_settings: dict = None) -> dict:
    """Enforce first-stage=copy, later-stages=notebook, processed_time presence,
    validate any aggregation blocks, and normalize container names to Azure-safe.

    custom_settings: explicit user choices — they raise the size-based
    over-provisioning caps (an explicit request wins over the safety clamp,
    which only guards against LLM-invented values)."""
    schema = schema or {}
    config = _normalize_container_names(config)
    stages = config.get("stages", [])

    # Rebuild any top-level keys the LLM omitted so downstream consumers
    # (executor, assurance, dashboard) never hit a missing key.
    clist = config.get("containers_to_create") or list(config.get("containers", {}).values())
    if clist:
        config["containers_to_create"] = clist
        if not isinstance(config.get("containers"), dict) or not config["containers"]:
            config["containers"] = {f"stage{i}": name for i, name in enumerate(clist)}
        if not config.get("datasets"):
            config["datasets"] = _build_datasets(clist)
        config.setdefault("num_containers", len(clist))

    # Size-aware ceilings: the LLM can over-provision workers (e.g. 4 workers on
    # a tiny CSV → 68 GB cluster, infeasible). Clamp every notebook stage to the
    # worker/shuffle counts recommended for the detected data size so small data
    # never demands a big cluster. This is a deterministic guardrail, not a model
    # change — it applies to every planner backend.
    _size_rec      = get_recommended_settings(schema.get("size_hint", "medium"))
    _max_workers   = _size_rec["num_workers"]
    _max_shuffle   = _size_rec["shuffle_partitions"]
    if custom_settings:
        if custom_settings.get("num_workers") is not None:
            _max_workers = max(_max_workers, int(custom_settings["num_workers"]))
        if custom_settings.get("shuffle_partitions") is not None:
            _max_shuffle = max(_max_shuffle, int(custom_settings["shuffle_partitions"]))

    # The model often maps the user's "Stage 1" onto the literal first stage.
    # Coercing that stage to copy would DELETE its operations (off-by-one:
    # every filter shifts up a stage and the first one is lost). Instead,
    # insert a dedicated landing container + copy stage in front, keeping all
    # transformation stages intact. If padding previously appended a trailing
    # do-nothing stage, drop it to reclaim the slot.
    if stages and stages[0].get("type") != "copy" and _stage_op_load(stages[0]) > 0 and clist:
        landing, n = _sanitize_container_name(f"{clist[0]}-landing"), 1
        while landing in clist:
            n += 1
            landing = _sanitize_container_name(f"{clist[0]}-landing-{n}")
        clist.insert(0, landing)
        stages.insert(0, {
            "name": f"Ingest_{landing.title().replace('-', '')}",
            "type": "copy",
            "source_dataset": _dataset_name(landing),
            "sink_dataset":   _dataset_name(clist[1]),
            "diu": 2,
        })
        print(f"   First stage transforms data — inserted copy stage '{stages[0]['name']}' "
              f"(landing container '{landing}') so ingestion does not consume a transformation stage")
        if (len(stages) > 2 and stages[-1].get("type") == "notebook"
                and _stage_op_load(stages[-1]) == 0 and len(clist) > 2):
            dropped = stages.pop()
            clist.pop()
            if stages[-1].get("type") == "notebook":
                stages[-1]["sink_container"] = clist[-1]
            print(f"   Dropped trailing pass-through stage '{dropped.get('name')}' to keep the stage count")
        config["containers_to_create"] = clist
        config["containers"] = {f"stage{i}": c for i, c in enumerate(clist)}
        config["datasets"] = _build_datasets(clist)
        config["num_containers"] = len(clist)

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

        # Guarantee the references each stage type needs at execution time.
        # Coerced or LLM-shaped stages may carry the wrong kind of reference
        # (containers on a copy stage, datasets on a notebook stage); derive
        # the missing ones from the container sequence.
        src = s.get("source_container") or (clist[i] if i < len(clist) else None)
        snk = s.get("sink_container") or (clist[i + 1] if i + 1 < len(clist) else None)
        if s.get("type") == "copy":
            if not s.get("source_dataset") and src:
                s["source_dataset"] = _dataset_name(src)
            if not s.get("sink_dataset") and snk:
                s["sink_dataset"] = _dataset_name(snk)
            s.setdefault("diu", 2)
        else:
            if not s.get("source_container") and src:
                s["source_container"] = src
            if not s.get("sink_container") and snk:
                s["sink_container"] = snk

        if s.get("type") == "notebook":
            transforms = [t for t in s.get("transformations", []) if t and t.strip()]
            if not any("processed_time" in t for t in transforms):
                transforms.append("processed_time = currentTimestamp()")
            s["transformations"] = transforms
            s.setdefault("filter_condition", None)
            s.setdefault("num_workers", 0)
            s.setdefault("shuffle_partitions", 8)

            # Clamp to size-appropriate ceilings (prevents over-provisioning).
            req_w = int(s.get("num_workers", 0) or 0)
            if req_w > _max_workers:
                print(f"   Stage '{s['name']}' num_workers {req_w} → {_max_workers} (size cap)")
                s["num_workers"] = _max_workers
            req_sh = int(s.get("shuffle_partitions", 8) or 8)
            if req_sh > _max_shuffle:
                s["shuffle_partitions"] = _max_shuffle

            if s.get("aggregation"):
                agg = _validate_aggregation(s["aggregation"], schema, s["name"])
                if agg:
                    s["aggregation"] = agg
                else:
                    s.pop("aggregation", None)

    # Every dataset a copy stage references must exist in config["datasets"],
    # otherwise the executor skips its creation and the ADF run fails.
    datasets = config.setdefault("datasets", [])
    known_ds = {d.get("name") for d in datasets}
    for i, s in enumerate(stages):
        if s.get("type") != "copy":
            continue
        for key, role_idx in (("source_dataset", i), ("sink_dataset", i + 1)):
            ds_name = s.get(key)
            if not ds_name or ds_name in known_ds:
                continue
            container = clist[role_idx] if role_idx < len(clist) else None
            if not container:
                continue
            role = "source" if role_idx == 0 else ("sink" if role_idx == len(clist) - 1 else "intermediate")
            datasets.append({"name": ds_name, "container": container, "role": role})
            known_ds.add(ds_name)

    config["stages"] = stages
    config["execution_order"] = [s["name"] for s in stages]
    # Validate/derive the concurrency plan (also reorders stages if needed).
    config = sanitize_execution_groups(config)
    return config


def _print_plan_summary(config: dict):
    print(f"   Containers  : {list(config.get('containers', {}).values())}")
    print(f"   Stages      : {[s.get('name') for s in config.get('stages', [])]}")
    print(f"   Exec Order  : {config.get('execution_order', [])}")
    print(f"   Reasoning   : {config.get('reasoning', 'N/A')}")
    for s in config.get("stages", []):
        print(f"\n   Stage: {s.get('name')} (type={s.get('type')})")
        if s.get("type") == "notebook":
            for t in s.get("transformations", []):
                print(f"      transform: {t}")
            if s.get("filter_condition"):
                print(f"      filter:    {s['filter_condition']}")
