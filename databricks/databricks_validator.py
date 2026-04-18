"""
databricks_validator.py
-----------------------
Pre-execution validation and healing layer.

Sits between Groq planner output and Databricks execution.

Flow:
    Groq Planner
        ↓
    validate_and_heal_config()   ← THIS FILE
        ↓
    Databricks Execution
        ↓
    (if failure) → DatabricksSelfHealingAgent (runtime)

Healing philosophy:
    ✅  Exact case-insensitive match → auto-fix (case_fix)
    ✅  Fuzzy match → NOT auto-fixed (delegated to self-healing)
    ✅  Derived columns from earlier transforms → always valid
    ✅  Known PySpark/ADF functions, Python keywords → always skip (not columns)
    ❌  HARD STOP only when a token has NO fuzzy match in schema AT ALL
        i.e. the column is genuinely invented / totally wrong



Examples of what causes a HARD STOP:
    revenue_ytd  (does not exist, no close match → unresolvable)
    xyz_col      (completely invented)
"""

import re
from difflib import SequenceMatcher


# ============================================================
# HEALING LOG ENTRY
# ============================================================
def _healing_entry(fix_type: str, original: str, fixed: str,
                   confidence: float, note: str = "") -> dict:
    return {
        "type":       fix_type,
        "original":   original,
        "fixed":      fixed,
        "confidence": round(confidence, 2),
        "note":       note,
    }


# ============================================================
# FUZZY COLUMN MATCHER  (aggressive + smart)
# ============================================================
def _fuzzy_match(unknown: str, known_columns: list) -> tuple:
    """
    Return (best_match, confidence) for `unknown` against `known_columns`.

    Scoring layers (highest wins):
      1. Exact case-insensitive  → 1.0
      2. Substring containment   → 0.80
      3. Token overlap           → 0.60 + token_score * 0.35
      4. Shared prefix (≥4 chars)→ 0.65 + prefix_ratio * 0.25
      5. SequenceMatcher ratio   → base

    Auto-fix threshold: 0.55  (below this → unresolvable → hard stop)
    """
    if not known_columns:
        return None, 0.0

    unknown_lower = unknown.lower()
    unknown_parts = set(re.split(r'[_\s]+', unknown_lower))

    best_match = None
    best_score = 0.0

    for col in known_columns:
        col_lower = col.lower()

        # 1. Exact case-insensitive
        if unknown_lower == col_lower:
            return col, 1.0

        # 2. SequenceMatcher base
        score = SequenceMatcher(None, unknown_lower, col_lower).ratio()

        # 3. Substring containment bonus
        if unknown_lower in col_lower or col_lower in unknown_lower:
            score = max(score, 0.80)

        # 4. Token overlap bonus (split on underscore)
        col_parts = set(re.split(r'[_\s]+', col_lower))
        overlap   = unknown_parts & col_parts
        if overlap:
            token_score = len(overlap) / max(len(unknown_parts), len(col_parts))
            score = max(score, 0.60 + token_score * 0.35)

        # 5. Shared prefix bonus (≥4 chars)
        min_len = min(len(unknown_lower), len(col_lower))
        if min_len >= 4:
            prefix_len = 0
            for a, b in zip(unknown_lower, col_lower):
                if a == b:
                    prefix_len += 1
                else:
                    break
            if prefix_len >= 4:
                score = max(score, 0.65 + (prefix_len / min_len) * 0.25)

        if score > best_score:
            best_score = score
            best_match = col

    if best_score >= 0.55:
        return best_match, best_score
    return None, 0.0


# ============================================================
# PYSPARK / ADF FUNCTION ALLOWLIST
# ============================================================
_PYSPARK_FUNCTIONS = {
    # Core Spark
    'col', 'lit', 'when', 'otherwise', 'coalesce', 'expr',
    # String
    'upper', 'lower', 'trim', 'ltrim', 'rtrim', 'initcap',
    'concat', 'concat_ws', 'substring', 'length', 'replace',
    'regexp_replace', 'regexp_extract', 'split', 'lpad', 'rpad',
    'instr', 'repeat', 'reverse', 'soundex',
    # Math
    'round', 'floor', 'ceil', 'abs', 'sqrt', 'pow', 'log',
    'sin', 'cos', 'tan', 'greatest', 'least', 'rand', 'randn', 'signum',
    # Date/time
    'current_timestamp', 'current_date', 'now',
    'year', 'month', 'dayofmonth', 'dayofweek', 'hour', 'minute', 'second',
    'to_date', 'to_timestamp', 'date_format', 'unix_timestamp',
    'from_unixtime', 'date_add', 'date_sub', 'datediff',
    'add_months', 'months_between', 'next_day', 'last_day', 'trunc',
    # Null / cast
    'isnull', 'isnotnull', 'isnan', 'nvl', 'nvl2', 'ifnull', 'nullif',
    'cast', 'int', 'float', 'double', 'string', 'boolean', 'long',
    # Aggregation
    'sum', 'avg', 'mean', 'min', 'max', 'count', 'countDistinct',
    'first', 'last', 'collect_list', 'collect_set',
    'stddev', 'variance', 'kurtosis', 'skewness',
    # Array/map
    'array', 'map', 'struct', 'create_map', 'array_contains',
    'size', 'explode', 'posexplode', 'flatten', 'sort_array',
    # Window
    'row_number', 'rank', 'dense_rank', 'percent_rank',
    'lag', 'lead', 'ntile', 'cume_dist',
    # Hash
    'md5', 'sha1', 'sha2', 'crc32', 'hash', 'uuid',
    # Literals / types
    'true', 'false', 'none', 'null',
    'IntegerType', 'LongType', 'DoubleType', 'FloatType',
    'StringType', 'BooleanType', 'DateType', 'TimestampType',
    # ADF / Data Factory style
    'toInteger', 'toString', 'toDouble', 'toLong', 'toFloat',
    'currentTimestamp', 'currentDate', 'iifNull', 'iif', 'isNull',
    'initCap', 'dayOfMonth',
    'equals', 'notEquals', 'greater', 'less', 'greaterOrEqual', 'lessOrEqual',
    # Python builtins
    'str', 'len', 'range', 'print', 'list', 'dict', 'set', 'tuple',
    'isinstance', 'type', 'sorted', 'enumerate', 'zip',
}

_PYTHON_KEYWORDS = {
    'and', 'or', 'not', 'in', 'is', 'if', 'else', 'elif',
    'for', 'while', 'return', 'import', 'from', 'as', 'with',
    'try', 'except', 'finally', 'raise', 'pass', 'break',
    'continue', 'def', 'class', 'lambda', 'yield', 'del',
    'global', 'nonlocal', 'assert',
}

_NON_COLUMN_TOKENS = _PYSPARK_FUNCTIONS | _PYTHON_KEYWORDS
_NON_COLUMN_LOWER  = {k.lower() for k in _NON_COLUMN_TOKENS}


# ============================================================
# IDENTIFIER EXTRACTOR
# ============================================================
def _extract_identifiers(expr: str) -> list:
    """
    Extract bare identifiers from an expression that COULD be column refs.
    Skips: known functions, Python keywords, numeric literals.
    """
    tokens     = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expr)
    candidates = []
    seen       = set()
    for t in tokens:
        if t in seen:
            continue
        seen.add(t)
        if t in _NON_COLUMN_TOKENS:
            continue
        if t.lower() in _NON_COLUMN_LOWER:
            continue
        try:
            float(t)
            continue
        except ValueError:
            pass
        candidates.append(t)
    return candidates


# ============================================================
# SYNTAX VALIDATOR
# ============================================================
def _validate_expression_syntax(expr: str) -> tuple:
    """Balanced parentheses + non-empty check. Returns (ok, error_msg)."""
    if not expr.strip():
        return False, "Expression is empty"
    depth = 0
    for ch in expr:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if depth < 0:
            return False, f"Unbalanced parentheses in: {expr}"
    if depth != 0:
        return False, f"Unbalanced parentheses (depth={depth}) in: {expr}"
    return True, ""


# ============================================================
# MAIN VALIDATION + HEALING
# ============================================================
def validate_and_heal_config(pipeline_config: dict, schema: dict) -> tuple:
    """
    Pre-execution validation and healing of the pipeline config.

    Parameters
    ----------
    pipeline_config : dict   Config dict from Groq planner.
    schema          : dict   CSV schema (columns, inferred_types, samples…)

    Returns
    -------
    (healed_config, healing_log, is_valid)

    Raises
    ------
    ValueError — only when a column reference is completely unresolvable.
    """
    csv_columns   = list(schema.get("columns", []))
    healing_log   = []

    for pipeline in pipeline_config.get("pipelines", []):
        pl_name = pipeline.get("name", "unknown")
        pl_type = pipeline.get("type", "copy")

        if pl_type != "transform":
            continue

        transforms      = pipeline.get("transformations", [])
        derived_columns = []          # cols produced by earlier transforms
        validated       = []

        for raw_transform in transforms:
            transform_str = raw_transform.strip()
            if not transform_str:
                continue

            # ── Must have '=' ──────────────────────────────────────────────
            if "=" not in transform_str:
                raise ValueError(
                    f"\n❌  Pre-execution validation FAILED in pipeline '{pl_name}':\n"
                    f"    Transform has no '=' sign: '{transform_str}'\n"
                    f"    Format required: output_col = expression\n"
                    f"    ⛔  Pipeline execution STOPPED."
                )

            lhs, rhs   = transform_str.split("=", 1)
            output_col = lhs.strip()
            raw_expr   = rhs.strip()

            # ── Syntax check ───────────────────────────────────────────────
            ok, err = _validate_expression_syntax(raw_expr)
            if not ok:
                raise ValueError(
                    f"\n❌  Pre-execution validation FAILED in pipeline '{pl_name}':\n"
                    f"    Transform '{output_col}': {err}\n"
                    f"    Expression: {raw_expr}\n"
                    f"    ⛔  Pipeline execution STOPPED."
                )

            # ── Full valid column universe at this point ────────────────────
            all_valid        = csv_columns + derived_columns
            all_valid_lower  = {c.lower(): c for c in all_valid}

            # ── Identify candidate tokens ──────────────────────────────────
            candidates = _extract_identifiers(raw_expr)

            healed_expr = raw_expr

            for token in candidates:
                tl = token.lower()

                # a) Exact match → fine
                if token in all_valid:
                    continue

                # b) Case mismatch only → silent fix
                if tl in all_valid_lower:
                    correct = all_valid_lower[tl]
                    healed_expr = re.sub(r'\b' + re.escape(token) + r'\b',
                                         correct, healed_expr)
                    healing_log.append(_healing_entry(
                        "case_fix", token, correct, 1.0,
                        f"in transform '{output_col}'",
                    ))
                    continue

                # c) Fuzzy match — only against actual CSV schema columns
                #    (not derived columns computed in this pipeline).
                #    Derived cols are correctly tracked above as exact/case matches.
                #    Fuzzy-matching against derived cols causes false positives like
                #    'revenue_ytd' → 'total_revenue' which is wrong and dangerous.
                # ❌ DISABLE fuzzy auto-fix — let self-healing handle it
                best, conf = _fuzzy_match(token, csv_columns)
                if best is not None:
                    raise ValueError(
                        f"\n❌ Column '{token}' not found.\n"
                        f"Did you mean '{best}'? (confidence {conf:.2f})\n"
                        f"Letting self-healing handle this.\n"
                    )

                # d) No match → HARD STOP
                available = ", ".join(f"'{c}'" for c in all_valid)
                raise ValueError(
                    f"\n❌  Pre-execution validation FAILED in pipeline '{pl_name}':\n"
                    f"    Transform '{output_col}' references unknown column '{token}'.\n"
                    f"    Expression : {raw_expr}\n"
                    f"    Available  : [{available}]\n"
                    f"    '{token}' does not resemble any schema column (threshold 55%).\n"
                    f"    Fix the transform or check your CSV schema.\n"
                    f"    ⛔  Pipeline execution STOPPED."
                )

            # ── Store healed/original transform ────────────────────────────
            if healed_expr != raw_expr:
                validated.append(f"{output_col} = {healed_expr}")
            else:
                validated.append(transform_str)

            # ── Register output column as derived ──────────────────────────
            derived_columns.append(output_col)

        pipeline["transformations"] = validated

    return pipeline_config, healing_log, True


# ============================================================
# HEALING LOG PRINTER
# ============================================================
def print_healing_log(healing_log: list):
    """Pretty-print the pre-execution healing log."""
    if not healing_log:
        print("✅  No pre-execution fixes needed — all transformations valid.")
        return

    print("\n🔧 Pre-execution healing applied:")
    for entry in healing_log:
        ft   = entry["type"]
        orig = entry["original"]
        fix  = entry["fixed"]
        conf = entry["confidence"]
        note = entry.get("note", "")
        if ft == "case_fix":
            print(f"   📝 Case fix  : '{orig}' → '{fix}' (confidence: {conf:.2f})  [{note}]")
        elif ft == "auto_fix":
            print(f"   🔄 Fuzzy fix : '{orig}' → '{fix}' (confidence: {conf:.2f})  [{note}]")
        else:
            print(f"   ✏️  Fix       : '{orig}' → '{fix}' (confidence: {conf:.2f})  [{note}]")
    print("✅  All transformations validated and healed.\n")