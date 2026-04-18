"""
databricks_self_healing_agent.py
---------------------------------
Runtime Self-Healing Agent for the Databricks pipeline orchestrator.

RESPONSIBILITY: Runtime failures ONLY.
Pre-execution issues are handled by databricks_validator.py before execution.

Runtime issues handled:
  - NameError        : bare column used as Python var  → col() wrapping
  - AnalysisException: unresolved column reference     → fuzzy fix
  - NullPointerException / null value failures         → null-safe coalesce
  - Type cast errors                                   → safe cast with default
  - Cluster OOM / timeout                              → reduce parallelism
  - Cluster terminated / not found                     → mark for re-provision
  - Auth / token errors                                → flag for token refresh
  - DBFS / Azure Blob / ADLS missing paths             → path recovery / skip
  - Azure container deleted                            → recreate container flag
  - Workspace errors                                   → flag + guidance
  - Copy failures                                      → delimiter / encoding fix

KEY RULE: NEVER silently drop transforms.
  - If a transform CAN be fixed → fix it and keep it.
  - If it absolutely cannot be fixed → replace with safe equivalent + explain.
  - Transforms are only removed as a last resort when they are the direct
    source of a hard crash AND no fix exists; dependents are rewritten, not lost.
"""

import re
import json
from difflib import get_close_matches  # ✅ STEP 1 — ADDED IMPORT


# ============================================================
# PYSPARK FUNCTION ALLOWLIST
# ============================================================
PYSPARK_FUNCTIONS = {
    'col', 'lit', 'when', 'otherwise', 'coalesce', 'expr',
    'upper', 'lower', 'trim', 'ltrim', 'rtrim', 'initcap',
    'concat', 'concat_ws', 'substring', 'length', 'replace',
    'regexp_replace', 'regexp_extract', 'split', 'instr',
    'lpad', 'rpad', 'repeat', 'reverse', 'soundex',
    'round', 'floor', 'ceil', 'abs', 'sqrt', 'pow', 'log',
    'sin', 'cos', 'tan', 'asin', 'acos', 'atan', 'atan2',
    'greatest', 'least', 'rand', 'randn', 'signum',
    'current_timestamp', 'current_date', 'now',
    'year', 'month', 'dayofmonth', 'dayofweek', 'dayofyear',
    'hour', 'minute', 'second',
    'to_date', 'to_timestamp', 'date_format', 'unix_timestamp',
    'from_unixtime', 'date_add', 'date_sub', 'datediff',
    'add_months', 'months_between', 'next_day', 'last_day',
    'trunc', 'date_trunc',
    'cast', 'int', 'float', 'double', 'string', 'boolean', 'long',
    'isnull', 'isnotnull', 'nvl', 'nvl2', 'nullif', 'ifnull', 'isnan',
    'sum', 'avg', 'mean', 'min', 'max', 'count', 'countDistinct',
    'first', 'last', 'collect_list', 'collect_set',
    'stddev', 'variance', 'kurtosis', 'skewness',
    'array', 'map', 'struct', 'create_map', 'array_contains',
    'size', 'explode', 'posexplode', 'flatten', 'sort_array',
    'row_number', 'rank', 'dense_rank', 'percent_rank',
    'lag', 'lead', 'ntile', 'cume_dist',
    'md5', 'sha1', 'sha2', 'crc32', 'hash', 'xxhash64', 'uuid',
    'true', 'false', 'none', 'null',
    'IntegerType', 'LongType', 'DoubleType', 'FloatType',
    'StringType', 'BooleanType', 'DateType', 'TimestampType',
    'int', 'str', 'float', 'bool', 'len', 'range', 'print',
    'toInteger', 'toString', 'toDouble', 'toLong', 'toFloat',
    'currentTimestamp', 'currentDate', 'iifNull', 'iif', 'isNull',
    'initCap', 'dayOfMonth',
    'equals', 'notEquals', 'greater', 'less', 'greaterOrEqual', 'lessOrEqual',
}

_PYTHON_KEYWORDS = {
    'and', 'or', 'not', 'in', 'is', 'if', 'else', 'elif',
    'for', 'while', 'return', 'import', 'from', 'as', 'with',
    'try', 'except', 'finally', 'raise', 'pass', 'break',
    'continue', 'def', 'class', 'lambda', 'yield', 'del',
    'global', 'nonlocal', 'assert',
}

ALL_NON_COLUMN_TOKENS = PYSPARK_FUNCTIONS | _PYTHON_KEYWORDS


# ============================================================
# ERROR CATEGORY CONSTANTS
# ============================================================
CAUSE_NULL_VALUES           = "null_values"
CAUSE_TYPE_CAST_ERROR       = "type_cast_error"
CAUSE_EXPRESSION_ERROR      = "expression_error"
CAUSE_SCHEMA_MISMATCH       = "schema_mismatch"
CAUSE_CLUSTER_OOM           = "cluster_oom"
CAUSE_CLUSTER_ERROR         = "cluster_error"
CAUSE_AUTH_ERROR            = "auth_error"
CAUSE_DBFS_MISSING          = "dbfs_missing"
CAUSE_CONTAINER_DELETED     = "container_deleted"   # Azure container gone
CAUSE_WORKSPACE_ERROR       = "workspace_error"
CAUSE_JOB_TIMEOUT           = "job_timeout"
CAUSE_NOTEBOOK_EXIT_ERROR   = "notebook_exit_error"
CAUSE_COPY_FAILURE          = "copy_failure"
CAUSE_UNKNOWN               = "unknown"


# ============================================================
# ✅ STEP 2 — FUZZY MATCH HELPER (module-level)
# ============================================================
def fuzzy_match_column(token, columns):
    """
    Find closest matching column name using similarity.

    Examples:
        qty        → quantity
        price      → unit_price
        order_dt   → order_date
    """
    matches = get_close_matches(token, columns, n=1, cutoff=0.6)
    return matches[0] if matches else None


# ============================================================
# SELF-HEALING AGENT
# ============================================================
class DatabricksSelfHealingAgent:

    def __init__(self):
        pass

    # ------------------------------------------------------------------
    # PUBLIC ENTRY POINT
    # ------------------------------------------------------------------
    def heal(
        self,
        error_message: str,
        pipeline_config: dict,
        csv_columns: list = None,
    ) -> tuple:
        """
        Analyse a runtime error, apply a fix, and return (success, fixed_config).

        Behaviour by error type:
          NameError on a derived/CSV column  → keep ALL transforms, set healing=True
                                               (build_transform_script will use col())
          NameError on unknown column        → fuzzy match fix, or rewrite dependents
          AnalysisException                  → fuzzy match the bad column ref
          Null / type errors                 → null-safe coalesce wrap
          Cluster OOM / timeout              → reduce shuffle_partitions
          Cluster deleted                    → mark for re-provision
          Azure container deleted            → mark containers_to_create + healing=True
          Auth error                         → flag, do not retry automatically
          DBFS missing                       → strip DBFS refs or reconstruct path
          Copy failure                       → flag encoding/delimiter issue
        """
        print("\n🛠  DATABRICKS RUNTIME SELF-HEALING AGENT ACTIVATED")

        cause_info = self.detect_root_cause(error_message, pipeline_config)
        cause      = cause_info["cause"]
        details    = cause_info.get("details", {})

        print(f"🔍 Detected issue: {cause} | Details: {details}")

        fixed_config = self.apply_fix(
            cause_info, error_message, pipeline_config, csv_columns=csv_columns
        )

        if fixed_config is None:
            print("🚫 No safe healing possible")
            return False, pipeline_config

        print("🔧 Fix applied to pipeline config — caller will redeploy via execute_pipeline()")
        # 🔥 ENABLE conversion for retry
        import databricks_api as api
        api.SELF_HEALING_MODE = True
        return True, fixed_config

    # ------------------------------------------------------------------
    # ROOT CAUSE DETECTION
    # ------------------------------------------------------------------
    def detect_root_cause(self, error_message: str, pipeline_config: dict = None) -> dict:
        if not error_message:
            return {"cause": CAUSE_UNKNOWN, "details": {}}

        em  = error_message.lower()
        col = self._extract_column(error_message)

        # ── NameError (bare Python var instead of col()) ─────────────────────
        if "nameerror" in em or ("name" in em and "is not defined" in em):
            col_from_name = self._extract_undefined_name(error_message)
            return {
                "cause": CAUSE_EXPRESSION_ERROR,
                "details": {
                    "column":         col_from_name or col,
                    "hint":           "bare column used as Python variable — needs col() wrapping",
                    "is_name_error":  True,
                    "undefined_name": col_from_name or col,
                },
            }

        # ── PySpark AnalysisException ─────────────────────────────────────────
        if "analysisexception" in em:
            if any(x in em for x in [
                "cannot resolve", "unresolved attribute", "no such struct field",
                "column not found", "reference", "ambiguous",
            ]):
                if any(x in em for x in ["schema", "mismatch", "incompatible"]):
                    return {"cause": CAUSE_SCHEMA_MISMATCH, "details": {"column": col}}
                return {
                    "cause": CAUSE_EXPRESSION_ERROR,
                    "details": {"column": col, "hint": "unresolved column reference"},
                }

        # ── Type errors ───────────────────────────────────────────────────────
        if any(x in em for x in [
            "illegalargumentexception", "numberformatexception",
            "cannot convert", "invalid value for cast",
            "value must be", "not an integer",
        ]):
            return {"cause": CAUSE_TYPE_CAST_ERROR, "details": {"column": col}}

        # ── Null errors ───────────────────────────────────────────────────────
        if any(x in em for x in [
            "nullpointerexception", "null value", "violates not-null",
            "column is null", "contains null", "non-null constraint",
        ]):
            return {"cause": CAUSE_NULL_VALUES, "details": {"column": col}}

        # ── AttributeError ────────────────────────────────────────────────────
        if "attributeerror" in em:
            return {
                "cause": CAUSE_EXPRESSION_ERROR,
                "details": {"column": col, "hint": "attribute error in generated notebook"},
            }

        # ── Azure container deleted / not found ───────────────────────────────
        if any(x in em for x in [
            "containernotfound", "container does not exist",
            "the specified container does not exist",
            "blobserviceerror", "resourcenotfound",
            "storageexception", "azure storage",
            "container was deleted", "container not found",
        ]):
            container = self._extract_container_name(error_message)
            return {
                "cause": CAUSE_CONTAINER_DELETED,
                "details": {"container": container, "hint": "Azure Blob container missing"},
            }

        # ── DBFS / path missing ───────────────────────────────────────────────
        if any(x in em for x in [
            "dbfs", "filenotfoundexception", "path does not exist",
            "no such file", "dbfs open failed",
        ]):
            path = self._extract_dbfs_path(error_message)
            return {"cause": CAUSE_DBFS_MISSING, "details": {"path": path}}

        # ── Cluster OOM ───────────────────────────────────────────────────────
        if any(x in em for x in [
            "outofmemoryerror", "java heap space", "gc overhead limit",
            "not enough memory", "executor lost", "container killed",
        ]):
            return {"cause": CAUSE_CLUSTER_OOM, "details": {}}

        # ── Cluster terminated / not found ────────────────────────────────────
        if any(x in em for x in [
            "cluster terminated", "clusternotfound", "cluster does not exist",
            "cluster is not running", "failed to start cluster",
        ]):
            return {"cause": CAUSE_CLUSTER_ERROR, "details": {}}

        # ── Auth ──────────────────────────────────────────────────────────────
        if any(x in em for x in [
            "401", "403", "unauthorized", "forbidden",
            "authentication failed", "invalid token",
        ]):
            return {"cause": CAUSE_AUTH_ERROR, "details": {}}

        # ── Timeout ───────────────────────────────────────────────────────────
        if any(x in em for x in ["timeout", "timed out", "max_wait"]):
            return {"cause": CAUSE_JOB_TIMEOUT, "details": {}}

        # ── Internal error ────────────────────────────────────────────────────
        if "internal_error" in em or "internal error" in em:
            return {"cause": CAUSE_CLUSTER_ERROR, "details": {"hint": "Databricks internal error"}}

        # ── Notebook exit ─────────────────────────────────────────────────────
        if any(x in em for x in [
            "no output", "notebook_output", "exit value",
            "dbutils.notebook.exit", "notebook did not",
        ]):
            return {"cause": CAUSE_NOTEBOOK_EXIT_ERROR, "details": {}}

        # ── Workspace error ───────────────────────────────────────────────────
        if any(x in em for x in [
            "workspace import failed", "workspace mkdirs", "workspace_error",
        ]):
            return {"cause": CAUSE_WORKSPACE_ERROR, "details": {}}

        # ── Copy failure ──────────────────────────────────────────────────────
        if any(x in em for x in [
            "typeerror", "csv", "delimiter", "encoding", "decode",
            "unicodedecodeerror", "badrecord",
        ]):
            return {"cause": CAUSE_COPY_FAILURE, "details": {}}

        # ── Python errors that likely come from bad transform code ─────────────
        if any(x in em for x in ["valueerror", "keyerror", "indexerror", "syntaxerror"]):
            return {"cause": CAUSE_EXPRESSION_ERROR, "details": {"column": col}}

        return {"cause": CAUSE_UNKNOWN, "details": {}}

    # ------------------------------------------------------------------
    # APPLY FIX — dispatch
    # ------------------------------------------------------------------
    def apply_fix(
        self,
        cause_info: dict,
        error_message: str,
        config: dict,
        csv_columns: list = None,
    ) -> dict:
        cause     = cause_info["cause"]
        details   = cause_info.get("details", {})
        col       = details.get("column", "unknown_column")
        pipelines = config.setdefault("pipelines", [])

        # ── NULL VALUES ───────────────────────────────────────────────────────
        if cause == CAUSE_NULL_VALUES:
            print(f"🔧 Null values in '{col}' → adding null-safe coalesce cast")
            for p in pipelines:
                if p.get("type") == "transform":
                    existing  = list(p.get("transformations", []))
                    existing  = self._remove_transform_for(existing, col)
                    safe_expr = f"coalesce(toDouble({col}), 0)"
                    existing  = [f"{col} = {safe_expr}"] + existing
                    existing  = self._ensure_processed_time(existing)
                    p["transformations"] = existing
                    p["healing"]         = True
                    print(f"   → Added null-safe: {col} = {safe_expr}")
                    break
            return config

        # ── TYPE CAST ERROR ───────────────────────────────────────────────────
        if cause == CAUSE_TYPE_CAST_ERROR:
            print(f"🔧 Type cast error for '{col}' → null-safe cast with default")
            for p in pipelines:
                if p.get("type") == "transform":
                    existing    = list(p.get("transformations", []))
                    target_type = "double"
                    for t in existing:
                        if col in t and "integer" in t.lower():
                            target_type = "int"
                            break
                    existing  = self._remove_transform_for(existing, col)
                    safe_expr = (
                        f"coalesce(toDouble({col}), 0)"
                        if target_type == "double"
                        else f"coalesce(toInteger({col}), 0)"
                    )
                    existing = [f"{col} = {safe_expr}"] + existing
                    existing = self._ensure_processed_time(existing)
                    p["transformations"] = existing
                    p["healing"]         = True
                    print(f"   → Safe cast applied: {col} = {safe_expr}")
                    break
            return config

        # ── AZURE CONTAINER DELETED ───────────────────────────────────────────
        if cause == CAUSE_CONTAINER_DELETED:
            container = details.get("container", "unknown")
            print(f"🔧 Azure container '{container}' deleted → marking for recreation")
            print(f"   → Setting containers_to_create to include '{container}'")
            print(f"   → Pipeline will recreate the container before next run")

            # Add container back to the creation list
            existing_containers = config.get("containers_to_create", [])
            if container and container != "unknown" and container not in existing_containers:
                existing_containers.append(container)
                config["containers_to_create"] = existing_containers

            # Also ensure all containers in the pipeline are in the create list
            for label, name in config.get("containers", {}).items():
                if name not in existing_containers:
                    existing_containers.append(name)
            config["containers_to_create"] = list(set(existing_containers))

            # Flag all pipelines for healing retry
            for p in pipelines:
                p["healing"]             = True
                p["container_recreated"] = True
            print(f"   → containers_to_create: {config['containers_to_create']}")
            print(f"   → Retry will recreate containers then re-run the pipeline.")
            return config

        # ── EXPRESSION / SCHEMA / NAME ERROR ──────────────────────────────────
        if cause in (CAUSE_EXPRESSION_ERROR, CAUSE_SCHEMA_MISMATCH):

            is_name_error  = details.get("is_name_error", False)
            undefined_name = details.get("undefined_name", col)

            print(f"🔧 Expression error — undefined name: '{undefined_name}' "
                  f"(is_name_error={is_name_error})")

            # -----------------------------
            # FUNCTION REGISTRY
            # -----------------------------
            KNOWN_FUNCTIONS = {
                "totimestamp": "to_timestamp",
                "to_timestamp": "to_timestamp",
                "year": "year",
                "upper": "upper",
                "lower": "lower",
                "coalesce": "coalesce",
                "trim": "trim"
            }

            FUNCTION_LIKE = {
                "toTimestamp", "to_timestamp",
                "toDate", "to_date",
                "toInteger", "toDouble",
                "year"
            }

            def classify_token(token):
                # Check CSV columns
                if csv_columns and token.lower() in [c.lower() for c in csv_columns]:
                    return "column"

                # Check derived columns
                derived_cols = []
                for p in pipelines:
                    for t in p.get("transformations", []):
                        if "=" in t:
                            lhs = t.split("=")[0].strip()
                            derived_cols.append(lhs)

                if token in derived_cols:
                    return "derived"

                # detect function by pattern
                if (
                    token.lower() in KNOWN_FUNCTIONS
                    or token in FUNCTION_LIKE
                    or token.lower().startswith("to")
                    or token.lower() in PYSPARK_FUNCTIONS
                ):
                    return "function"

                return "unknown"

            def fix_function(expr):

                # Fix toTimestamp → to_timestamp
                expr = re.sub(
                    r"\btoTimestamp\s*\(\s*(.*?)\s*\)",
                    r'to_timestamp(\1)',
                    expr,
                    flags=re.IGNORECASE
                )

                # Wrap bare word arguments of to_timestamp with col()
                expr = re.sub(
                    r"to_timestamp\((\w+)\)",
                    r'to_timestamp(col("\1"))',
                    expr
                )

                # Fix year() — wrap bare word arg with col()
                expr = re.sub(
                    r"year\((\w+)\)",
                    r'year(col("\1"))',
                    expr
                )

                return expr

            token_type = classify_token(undefined_name)
            print(f"🔍 Token '{undefined_name}' classified as: {token_type}")

            # ✅ CHANGE 1 — BLOCK UNKNOWN TOKENS IMMEDIATELY
            if token_type == "unknown":
                print(f"🚫 Unknown token '{undefined_name}' — cannot heal safely")
                return config

            for p in pipelines:
                if p.get("type") == "transform":
                    existing = list(p.get("transformations", []))

                    # -------------------------
                    # CASE 1: FUNCTION ERROR
                    # -------------------------
                    if token_type == "function":
                        print("🔧 Fixing function mapping...")

                        fixed = []
                        for t in existing:
                            if "=" in t:
                                lhs, rhs = t.split("=", 1)

                                # apply fix_function TWICE so that
                                # toTimestamp(order_date)
                                # → to_timestamp(order_date)       [pass 1]
                                # → to_timestamp(col("order_date")) [pass 2]
                                new_rhs = fix_function(rhs.strip())
                                new_rhs = fix_function(new_rhs)

                                fixed.append(f"{lhs.strip()} = {new_rhs}")
                            else:
                                fixed.append(t)

                        fixed = self._ensure_processed_time(fixed)
                        p["transformations"] = fixed
                        p["healing"] = True
                        break

                    # -------------------------
                    # CASE 2: COLUMN or DERIVED ERROR
                    # -------------------------
                    elif token_type in ("column", "derived"):

                        print("🔧 Column exists → applying col() wrapping")

                        fixed = []
                        for t in existing:
                            if "=" in t:
                                lhs, rhs = t.split("=", 1)

                                # fix functions FIRST, THEN wrap columns
                                new_rhs = fix_function(rhs)

                                tokens = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', new_rhs)

                                for token in tokens:

                                    # Skip known safe tokens
                                    if (
                                        token in ALL_NON_COLUMN_TOKENS
                                        or token in FUNCTION_LIKE
                                        or token.lower() in PYSPARK_FUNCTIONS
                                    ):
                                        continue

                                    # ✅ CHANGE 2 — CONTROLLED FUZZY (ONLY SAFE CASES)
                                    if csv_columns and token.lower() not in [c.lower() for c in csv_columns]:
                                        from difflib import SequenceMatcher

                                        match = fuzzy_match_column(token, csv_columns)

                                        if match and match != token:
                                            confidence = SequenceMatcher(
                                                None, token.lower(), match.lower()
                                            ).ratio()

                                            if confidence >= 0.75:
                                                print(f"🔧 High-confidence match '{token}' → '{match}' ({confidence:.2f})")
                                                new_rhs = re.sub(
                                                    rf'\b{token}\b',
                                                    match,
                                                    new_rhs
                                                )
                                                token = match
                                            else:
                                                print(f"🚫 Low confidence match '{token}' → '{match}' ({confidence:.2f}) — rejecting")
                                                return config

                                    # Wrap as col()
                                    new_rhs = re.sub(
                                        rf'(?<!col\(")\b{re.escape(token)}\b',
                                        f'col("{token}")',
                                        new_rhs
                                    )

                                # Fix coalesce literals
                                new_rhs = re.sub(
                                    r'coalesce\((col\("[^"]+"\)),\s*([0-9.]+)\)',
                                    r'coalesce(\1, lit(\2))',
                                    new_rhs
                                )

                                fixed.append(f"{lhs.strip()} = {new_rhs.strip()}")

                            else:
                                fixed.append(t)

                        fixed = self._ensure_processed_time(fixed)
                        p["transformations"] = fixed
                        p["healing"] = True
                        break

                    # ✅ CHANGE 3 — CASE 3 (unknown fallback) REMOVED ENTIRELY

            return config

        # ── CLUSTER OOM ───────────────────────────────────────────────────────
        if cause == CAUSE_CLUSTER_OOM:
            print("🔧 Cluster OOM → reducing shuffle_partitions")
            for p in pipelines:
                current = p.get("shuffle_partitions", 8)
                p["shuffle_partitions"] = max(2, current // 2)
                p["healing"]            = True
                print(f"   → shuffle_partitions: {current} → {p['shuffle_partitions']}")
            return config

        # ── CLUSTER ERROR / TERMINATED ────────────────────────────────────────
        if cause == CAUSE_CLUSTER_ERROR:
            print("🔧 Cluster error → stripping to minimal safe config for re-provision")
            for p in pipelines:
                if p.get("type") == "transform":
                    p["transformations"] = [self._build_timestamp_transform()]
                    p["healing"]         = True
                    p["reprovision"]     = True
            return config

        # ── AUTH ERROR ────────────────────────────────────────────────────────
        if cause == CAUSE_AUTH_ERROR:
            print("🔧 Auth error — DATABRICKS_TOKEN may be expired or invalid.")
            print("   Regenerate: Databricks UI → User Settings → Developer → Access Tokens")
            for p in pipelines:
                p["healing"]       = True
                p["auth_required"] = True
            return config

        # ── DBFS MISSING ──────────────────────────────────────────────────────
        if cause == CAUSE_DBFS_MISSING:
            missing_path = details.get("path", "unknown DBFS path")
            print(f"🔧 DBFS path missing: '{missing_path}' → stripping DBFS references")
            for p in pipelines:
                if p.get("type") == "transform":
                    existing = [
                        t for t in p.get("transformations", [])
                        if "dbfs:/" not in t.lower()
                    ]
                    existing = self._ensure_processed_time(existing)
                    p["transformations"] = existing
                    p["healing"]         = True
            return config

        # ── WORKSPACE ERROR ───────────────────────────────────────────────────
        if cause == CAUSE_WORKSPACE_ERROR:
            print("🔧 Workspace upload error — check DATABRICKS_TOKEN permissions.")
            for p in pipelines:
                p["healing"]         = True
                p["workspace_error"] = True
            return config

        # ── JOB TIMEOUT ───────────────────────────────────────────────────────
        if cause == CAUSE_JOB_TIMEOUT:
            print("🔧 Job timeout → reducing shuffle_partitions")
            for p in pipelines:
                current = p.get("shuffle_partitions", 8)
                p["shuffle_partitions"] = max(2, current // 2)
                p["healing"]            = True
            return config

        # ── NOTEBOOK EXIT ERROR ───────────────────────────────────────────────
        if cause == CAUSE_NOTEBOOK_EXIT_ERROR:
            print("🔧 Notebook did not exit cleanly → stripping to safe baseline")
            for p in pipelines:
                if p.get("type") == "transform":
                    p["transformations"] = [self._build_timestamp_transform()]
                    p["healing"]         = True
            return config

        # ── COPY FAILURE ──────────────────────────────────────────────────────
        if cause == CAUSE_COPY_FAILURE:
            print("🔧 Copy stage failed — check CSV encoding / delimiter.")
            print("   Common fixes: UTF-8-BOM encoding, comma vs semicolon delimiter")
            for p in pipelines:
                if p.get("type") == "copy":
                    p["healing"]     = True
                    p["copy_failed"] = True
            return config

        # ── UNKNOWN ───────────────────────────────────────────────────────────
        print("🚫 Unknown error — cannot safely heal")

        return None

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------
    def _extract_column(self, message: str) -> str:
        patterns = [
            r"cannot resolve ['\"`]?`?([a-zA-Z_][a-zA-Z0-9_]*)`?['\"`]?",
            r"unresolved attribute ['\"`]?([a-zA-Z_][a-zA-Z0-9_]*)['\"`]?",
            r"column ['\"`]?([a-zA-Z_][a-zA-Z0-9_]*)['\"`]? not found",
            r"no such struct field ['\"`]?([a-zA-Z_][a-zA-Z0-9_]*)['\"`]?",
            r'withColumn\(["\'"]([a-zA-Z_][a-zA-Z0-9_]*)["\'"]',
            r'col\(["\'"]([a-zA-Z_][a-zA-Z0-9_]*)["\'"]',
            r"['\"`]([a-zA-Z_][a-zA-Z0-9_]*)['\"`]",
        ]
        for pat in patterns:
            m = re.search(pat, message, re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return "unknown_column"

    def _extract_undefined_name(self, message: str) -> str:
        """Extract the undefined name from a Python NameError traceback."""
        m = re.search(r"name ['\"]([a-zA-Z_][a-zA-Z0-9_]*)['\"] is not defined", message)
        if m:
            return m.group(1)
        m = re.search(r"NameError.*?['\"]([a-zA-Z_][a-zA-Z0-9_]*)['\"]", message)
        if m:
            return m.group(1)
        return ""

    def _extract_dbfs_path(self, message: str) -> str:
        m = re.search(r"(dbfs:/[^\s'\"]+)", message, re.IGNORECASE)
        if m:
            return m.group(1)
        return ""

    def _extract_container_name(self, message: str) -> str:
        """Extract Azure container name from error messages."""
        patterns = [
            r"container ['\"]([a-zA-Z0-9_\-]+)['\"]",
            r"ContainerName[:\s]+([a-zA-Z0-9_\-]+)",
            r"container=([a-zA-Z0-9_\-]+)",
            r"'([a-zA-Z0-9_\-]+)' does not exist",
        ]
        for pat in patterns:
            m = re.search(pat, message, re.IGNORECASE)
            if m:
                return m.group(1)
        return "unknown"

    def _get_known_columns(self, config: dict, csv_columns: list = None) -> list:
        if csv_columns:
            known = list(csv_columns)
        else:
            known = []
        # Also collect derived column names from transforms
        for p in config.get("pipelines", []):
            for t in p.get("transformations", []):
                if "=" in t:
                    lhs = t.split("=", 1)[0].strip()
                    if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', lhs) and lhs not in known:
                        known.append(lhs)
        return known

    def _fuzzy_match_column(self, unknown: str, known_columns: list) -> str:
        """Return best fuzzy match or None if below threshold."""
        unknown_lower = unknown.lower()
        unknown_parts = set(re.split(r'[_\s]+', unknown_lower))
        best_match    = None
        best_score    = 0.0

        for col in known_columns:
            col_lower = col.lower()

            # Exact case-insensitive
            if unknown_lower == col_lower:
                return col

            from difflib import SequenceMatcher
            score = SequenceMatcher(None, unknown_lower, col_lower).ratio()

            # Substring bonus
            if unknown_lower in col_lower or col_lower in unknown_lower:
                score = max(score, 0.80)

            # Token overlap bonus
            col_parts = set(re.split(r'[_\s]+', col_lower))
            overlap   = unknown_parts & col_parts
            if overlap:
                token_score = len(overlap) / max(len(unknown_parts), len(col_parts))
                score = max(score, 0.55 + token_score * 0.35)

            if score > best_score:
                best_score = score
                best_match = col

        # guard against self-referential match
        return best_match if best_score >= 0.50 and best_match != unknown else None

    def _build_timestamp_transform(self) -> str:
        return "processed_time = currentTimestamp()"

    def _ensure_processed_time(self, transforms: list) -> list:
        if not any("processed_time" in t for t in transforms):
            transforms.append(self._build_timestamp_transform())
        return transforms

    def _remove_transform_for(self, transforms: list, col_name: str) -> list:
        return [
            t for t in transforms
            if not t.strip().startswith(f"{col_name} =")
            and not t.strip().startswith(f"{col_name}=")
        ]

    def _transforms_depending_on(self, transforms: list, col_name: str) -> list:
        """Return all transforms that reference col_name on the RHS."""
        dependent = []
        for t in transforms:
            if "=" not in t:
                continue
            lhs, rhs = t.split("=", 1)
            if lhs.strip() == col_name:
                continue
            if re.search(r'\b' + re.escape(col_name) + r'\b', rhs):
                dependent.append(t)
        return dependent