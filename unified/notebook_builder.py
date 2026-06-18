"""
Notebook builder for the unified orchestrator.

For each "notebook" stage in the pipeline config, generate a PySpark notebook
(in Databricks "source" format) that reads a CSV from an Azure Blob container,
applies transformations and optional row filter, writes the result to a sink
blob container, and returns a short JSON exit value back to ADF.

Transformations authored in ADF-DSL (e.g. `total = qty * price`) are converted
to safe PySpark expressions so columns resolve at runtime via col("...").

The generated notebook takes four widget-based parameters supplied by the
ADF DatabricksNotebook activity at runtime:
    storage_account, storage_key, run_id, stage_name
Source / sink / transforms / filter are baked into the notebook at build time
because they are fixed per stage.
"""

import re


# ────────────────────────────────────────────────────────────────────────────
# ADF-DSL → PySpark expression converter (compact)
# ────────────────────────────────────────────────────────────────────────────
_ADF_SUBS = [
    (r"toInteger\(\s*(\w+)\s*\)",   r'col("\1").cast("int")'),
    (r"toLong\(\s*(\w+)\s*\)",      r'col("\1").cast("long")'),
    (r"toDouble\(\s*(\w+)\s*\)",    r'col("\1").cast("double")'),
    (r"toString\(\s*(\w+)\s*\)",    r'col("\1").cast("string")'),
    (r"toTimestamp\(\s*(\w+)\s*\)", r'to_timestamp(col("\1"))'),
    (r"toDate\(\s*(\w+)\s*\)",      r'to_date(col("\1"))'),

    (r"upper\(\s*(\w+)\s*\)",       r'upper(col("\1"))'),
    (r"lower\(\s*(\w+)\s*\)",       r'lower(col("\1"))'),
    (r"trim\(\s*(\w+)\s*\)",        r'trim(col("\1"))'),
    (r"ltrim\(\s*(\w+)\s*\)",       r'ltrim(col("\1"))'),
    (r"rtrim\(\s*(\w+)\s*\)",       r'rtrim(col("\1"))'),
    (r"initCap\(\s*(\w+)\s*\)",     r'initcap(col("\1"))'),
    (r"length\(\s*(\w+)\s*\)",      r'length(col("\1"))'),

    (r"year\(\s*(\w+)\s*\)",        r'year(col("\1"))'),
    (r"month\(\s*(\w+)\s*\)",       r'month(col("\1"))'),
    (r"dayOfMonth\(\s*(\w+)\s*\)",  r'dayofmonth(col("\1"))'),
    (r"hour\(\s*(\w+)\s*\)",        r'hour(col("\1"))'),
    (r"minute\(\s*(\w+)\s*\)",      r'minute(col("\1"))'),
    (r"second\(\s*(\w+)\s*\)",      r'second(col("\1"))'),

    (r"round\(\s*(\w+)\s*\)",       r'round(col("\1"))'),
    (r"floor\(\s*(\w+)\s*\)",       r'floor(col("\1"))'),
    (r"ceil\(\s*(\w+)\s*\)",        r'ceil(col("\1"))'),
    (r"abs\(\s*(\w+)\s*\)",         r'abs(col("\1"))'),
    (r"sqrt\(\s*(\w+)\s*\)",        r'sqrt(col("\1"))'),

    (r"isNull\(\s*(\w+)\s*\)",      r'col("\1").isNull()'),
    (r"iifNull\(\s*(\w+)\s*,\s*(.+?)\s*\)",   r'coalesce(col("\1"), lit(\2))'),
    (r"coalesce\(\s*(\w+)\s*,\s*(.+?)\s*\)",  r'coalesce(col("\1"), lit(\2))'),

    (r"currentTimestamp\(\s*\)",    "current_timestamp()"),
    (r"currentDate\(\s*\)",         "current_date()"),
]


_PYSPARK_KEEP_BARE = {
    "col", "lit", "when", "otherwise", "coalesce", "expr",
    "upper", "lower", "trim", "ltrim", "rtrim", "initcap", "length",
    "concat", "concat_ws", "substring", "regexp_replace",
    "current_timestamp", "current_date",
    "year", "month", "dayofmonth", "hour", "minute", "second",
    "to_date", "to_timestamp", "date_format",
    "round", "floor", "ceil", "abs", "sqrt", "pow",
    "sum", "avg", "mean", "min", "max", "count",
    "cast", "true", "false", "none", "null",
    "and", "or", "not", "in", "is",
}


def _convert_expr(expr: str) -> str:
    """Convert an ADF-DSL RHS expression to PySpark. Bare column names → col('...')."""
    expr = expr.strip()
    for pattern, replacement in _ADF_SUBS:
        expr = re.sub(pattern, replacement, expr)

    # Wrap bare identifiers in col(), skipping known PySpark functions and literals.
    def _wrap(match):
        token = match.group(0)
        if token in _PYSPARK_KEEP_BARE:
            return token
        if re.match(r'^\d', token):
            return token
        # Already inside col("...") → marker-based skip: if followed by " (or preceded by col(") leave it.
        return f'col("{token}")'

    placeholder_re = re.compile(r'col\("[^"]+"\)|\.cast\("\w+"\)|"[^"]*"|\'[^\']*\'')
    placeholders = []

    def _stash(m):
        placeholders.append(m.group(0))
        return f"\x00{len(placeholders) - 1}\x00"

    protected = placeholder_re.sub(_stash, expr)

    protected = re.sub(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', _wrap, protected)

    def _unstash(m):
        idx = int(m.group(1))
        return placeholders[idx]

    return re.sub(r'\x00(\d+)\x00', _unstash, protected)


def _convert_filter(expr: str) -> str:
    """Convert an ADF-DSL filter_condition into a PySpark boolean expression string."""
    e = expr.strip()

    patterns = [
        (r'^equals\(toInteger\((\w+)\),\s*(-?\d+)\)$',    r'col("\1").cast("int") == \2'),
        (r'^notEquals\(toInteger\((\w+)\),\s*(-?\d+)\)$', r'col("\1").cast("int") != \2'),
        (r'^greater\(toInteger\((\w+)\),\s*(-?\d+)\)$',   r'col("\1").cast("int") > \2'),
        (r'^less\(toInteger\((\w+)\),\s*(-?\d+)\)$',      r'col("\1").cast("int") < \2'),
        (r'^greaterOrEqual\(toInteger\((\w+)\),\s*(-?\d+)\)$', r'col("\1").cast("int") >= \2'),
        (r'^lessOrEqual\(toInteger\((\w+)\),\s*(-?\d+)\)$',    r'col("\1").cast("int") <= \2'),
        (r'^equals\((\w+),\s*\'([^\']+)\'\)$',  r'col("\1") == "\2"'),
        (r'^equals\((\w+),\s*(-?\d+)\)$',       r'col("\1") == \2'),
        (r'^notEquals\((\w+),\s*\'([^\']+)\'\)$', r'col("\1") != "\2"'),
        (r'^isNull\((\w+)\)$',                  r'col("\1").isNull()'),
        (r'^(\w+)\s*(==|!=|>=|<=|>|<)\s*(-?\d+)$', r'col("\1") \2 \3'),
        (r'^(\w+)\s*(==|!=)\s*\'([^\']+)\'$',      r'col("\1") \2 "\3"'),
    ]
    for pattern, replacement in patterns:
        if re.match(pattern, e, re.IGNORECASE):
            return re.sub(pattern, replacement, e, flags=re.IGNORECASE)
    return _convert_expr(e)


def _parse_transform(entry: str) -> tuple:
    """Split 'col = expr' into (col, expr). Returns None if malformed."""
    if "=" not in entry:
        return None
    lhs, rhs = entry.split("=", 1)
    lhs = lhs.strip()
    rhs = rhs.strip()
    if not lhs or not rhs or not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', lhs):
        return None
    return lhs, rhs


# ────────────────────────────────────────────────────────────────────────────
# Aggregation codegen
# ────────────────────────────────────────────────────────────────────────────
_AGG_FUNCS = {"avg", "sum", "min", "max", "count", "mean"}


def _build_agg_expr(agg: dict) -> str:
    """One {'op','column','alias'} → PySpark agg expression string."""
    op    = str(agg.get("op", "")).strip().lower()
    column = str(agg.get("column", "")).strip()
    alias  = str(agg.get("alias", "")).strip()
    if op not in _AGG_FUNCS:
        return ""
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', alias):
        return ""
    if op == "count" and column in ("*", ""):
        inner = "lit(1)"
    elif re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column):
        inner = f'col("{column}")'
    else:
        return ""
    return f'{op}({inner}).alias("{alias}")'


def _build_agg_block(aggregation: dict) -> tuple:
    """
    Convert an aggregation block into (group_by_args, agg_exprs).
    Returns ([], []) if the block is malformed / empty.
    """
    if not aggregation:
        return [], []
    group_by = [g for g in (aggregation.get("group_by") or [])
                if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', str(g).strip())]
    exprs = [e for e in (_build_agg_expr(a) for a in (aggregation.get("aggregations") or [])) if e]
    if not group_by or not exprs:
        return [], []
    return group_by, exprs


# ────────────────────────────────────────────────────────────────────────────
# Notebook source generator
# ────────────────────────────────────────────────────────────────────────────
NOTEBOOK_HEADER = "# Databricks notebook source\n"
CELL_SEP = "\n# COMMAND ----------\n\n"


def build_notebook_source(stage: dict, storage_account: str) -> str:
    """
    Generate a Databricks notebook (.py source format) for a notebook stage.

    The notebook:
      1. Pulls widget params: storage_key, run_id, stage_name
      2. Configures Spark to read/write wasbs:// with the supplied account key
      3. Reads all CSVs from stage['source_container']
      4. Applies transformations (ADF-DSL → PySpark via _convert_expr)
      5. Applies optional filter_condition
      6. Writes the result to stage['sink_container'] as a single CSV
      7. Calls dbutils.notebook.exit() with a short JSON status string
    """
    source_container = stage["source_container"]
    sink_container   = stage["sink_container"]
    transforms       = stage.get("transformations", []) or []
    filter_condition = stage.get("filter_condition")
    aggregation      = stage.get("aggregation")
    shuffle_parts    = int(stage.get("shuffle_partitions", 8))

    agg_group_by, agg_exprs = _build_agg_block(aggregation)
    has_agg = bool(agg_group_by and agg_exprs)

    pyspark_transforms = []
    for raw in transforms:
        parsed = _parse_transform(raw)
        if not parsed:
            pyspark_transforms.append(("_skipped", f"# skipped malformed transform: {raw!r}"))
            continue
        col_name, rhs = parsed
        # When aggregating, a row-level processed_time is dropped by groupBy;
        # it is re-added after the aggregation instead.
        if has_agg and col_name == "processed_time":
            continue
        rhs_pyspark = _convert_expr(rhs)
        pyspark_transforms.append((col_name, rhs_pyspark))

    pyspark_filter = _convert_filter(filter_condition) if filter_condition else None

    # ── Cell 1: imports + widgets ─────────────────────────────────────────
    cell_imports = (
        "from pyspark.sql import SparkSession\n"
        "from pyspark.sql.functions import (\n"
        "    col, lit, when, coalesce, expr,\n"
        "    upper, lower, trim, ltrim, rtrim, initcap, length,\n"
        "    concat, concat_ws, substring, regexp_replace,\n"
        "    current_timestamp, current_date,\n"
        "    year, month, dayofmonth, hour, minute, second,\n"
        "    to_date, to_timestamp, date_format,\n"
        "    round, floor, ceil, abs, sqrt, pow,\n"
        "    sum, avg, mean, min, max, count,\n"
        ")\n"
        "import json\n"
        "\n"
        'dbutils.widgets.text("storage_key", "", "Azure Storage Account Key")\n'
        'dbutils.widgets.text("run_id", "", "ADF Run ID")\n'
        'dbutils.widgets.text("stage_name", "", "Stage Name")\n'
        "\n"
        'storage_key = dbutils.widgets.get("storage_key")\n'
        'run_id      = dbutils.widgets.get("run_id")\n'
        'stage_name  = dbutils.widgets.get("stage_name")\n'
        "\n"
        f'STORAGE_ACCOUNT  = "{storage_account}"\n'
        f'SOURCE_CONTAINER = "{source_container}"\n'
        f'SINK_CONTAINER   = "{sink_container}"\n'
        f'SHUFFLE_PARTITIONS = {shuffle_parts}\n'
    )

    # ── Cell 2: Spark + storage auth ──────────────────────────────────────
    cell_spark = (
        "spark.conf.set(\n"
        "    f\"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net\",\n"
        "    storage_key,\n"
        ")\n"
        "spark.conf.set(\"spark.sql.shuffle.partitions\", str(SHUFFLE_PARTITIONS))\n"
        "print(f\"[{stage_name}] run_id={run_id} source={SOURCE_CONTAINER} sink={SINK_CONTAINER}\")\n"
    )

    # ── Cell 3: read source blob ──────────────────────────────────────────
    cell_read = (
        "source_path = (\n"
        "    f\"wasbs://{SOURCE_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/\"\n"
        ")\n"
        "df = (\n"
        "    spark.read\n"
        "         .option(\"header\", True)\n"
        "         .option(\"inferSchema\", True)\n"
        "         .csv(source_path)\n"
        ")\n"
        "print(f\"[{stage_name}] read {df.count()} rows, {len(df.columns)} columns from {SOURCE_CONTAINER}\")\n"
    )

    # ── Cell 4: transformations ───────────────────────────────────────────
    if pyspark_transforms:
        lines = []
        for col_name, rhs in pyspark_transforms:
            if col_name == "_skipped":
                lines.append(rhs)
            else:
                lines.append(f'df = df.withColumn("{col_name}", {rhs})')
        cell_transforms = "\n".join(lines)
    else:
        cell_transforms = "# no transformations configured for this stage\n"

    # ── Cell 5: filter ────────────────────────────────────────────────────
    if pyspark_filter:
        cell_filter = (
            f'df = df.filter({pyspark_filter})\n'
            'print(f"[{stage_name}] after filter: {df.count()} rows")\n'
        )
    else:
        cell_filter = "# no filter configured for this stage\n"

    # ── Cell 5b: aggregation (groupBy) ────────────────────────────────────
    if has_agg:
        group_args = ", ".join(f'"{g}"' for g in agg_group_by)
        agg_args   = ",\n    ".join(agg_exprs)
        cell_agg = (
            f"df = df.groupBy({group_args}).agg(\n"
            f"    {agg_args},\n"
            ")\n"
            'df = df.withColumn("processed_time", current_timestamp())\n'
            'print(f"[{stage_name}] after aggregation: {df.count()} groups")\n'
        )
    else:
        cell_agg = "# no aggregation configured for this stage\n"

    # ── Cell 6: write + exit ──────────────────────────────────────────────
    cell_write = (
        "sink_path = (\n"
        "    f\"wasbs://{SINK_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/output\"\n"
        ")\n"
        "(\n"
        "    df.coalesce(1)\n"
        "      .write\n"
        "      .mode(\"overwrite\")\n"
        "      .option(\"header\", True)\n"
        "      .csv(sink_path)\n"
        ")\n"
        "written = df.count()\n"
        "print(f\"[{stage_name}] wrote {written} rows to {SINK_CONTAINER}\")\n"
        "dbutils.notebook.exit(json.dumps({\n"
        "    \"status\":      \"succeeded\",\n"
        "    \"stage\":       stage_name,\n"
        "    \"run_id\":      run_id,\n"
        "    \"rows_written\": written,\n"
        "    \"sink\":        SINK_CONTAINER,\n"
        "}))\n"
    )

    cells = [cell_imports, cell_spark, cell_read, cell_transforms, cell_filter, cell_agg, cell_write]
    return NOTEBOOK_HEADER + CELL_SEP.join(cells)
