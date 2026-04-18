import requests
import json
import re
import base64
import time
import datetime
import os
from config import (
    DATABRICKS_HOST, DATABRICKS_TOKEN,
    DATABRICKS_CLUSTER_ID, DATABRICKS_SPARK_VERSION, DATABRICKS_NODE_TYPE,
)

SELF_HEALING_MODE = False
# ============================================================
# AUTH HEADERS
# ============================================================
def _headers() -> dict:
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }


def _url(path: str) -> str:
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(DATABRICKS_HOST)
    base = urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
    return base.rstrip("/") + path


# ============================================================
# CONNECTION CHECK
# ============================================================
def check_connection() -> tuple:
    try:
        r = requests.get(
            _url("/api/2.1/jobs/list"),
            headers=_headers(),
            params={"limit": 1},
            timeout=15,
        )
        if r.status_code == 200:
            return True, "Connected"
        if r.status_code == 401:
            return False, (
                "Authentication failed (401). "
                "Check DATABRICKS_TOKEN in config.py — it may be expired or invalid."
            )
        if r.status_code == 403:
            return False, (
                "Permission denied (403). "
                "Your token may lack Jobs API permissions. "
                "Regenerate your Personal Access Token in Databricks UI → User Settings → Developer."
            )
        if r.status_code == 404:
            return False, (
                f"Workspace not found (404). "
                f"DATABRICKS_HOST '{DATABRICKS_HOST}' is likely wrong. "
                f"Expected format: https://adb-XXXXXXXX.X.azuredatabricks.net"
            )
        return False, f"Unexpected response {r.status_code}: {r.text[:200]}"
    except requests.exceptions.ConnectionError:
        return False, (
            f"Cannot connect to '{DATABRICKS_HOST}'. "
            "Check the URL is correct and the workspace is reachable."
        )
    except requests.exceptions.Timeout:
        return False, f"Connection timed out reaching '{DATABRICKS_HOST}'."
    except Exception as e:
        return False, f"Connection error: {e}"


# ============================================================
# DBFS HELPERS (kept for API compat)
# ============================================================
def dbfs_mkdirs(path: str):
    r = requests.post(_url("/api/2.0/dbfs/mkdirs"), headers=_headers(), json={"path": path})
    if r.status_code == 200:
        print(f"   DBFS dir ready: {path}")
    elif r.status_code in (401, 403):
        raise Exception(f"DBFS auth error {r.status_code} on mkdirs '{path}'.")
    elif r.status_code == 404:
        raise Exception(f"DBFS 404 on mkdirs '{path}'.")
    else:
        raise Exception(f"DBFS mkdirs failed '{path}' -> {r.status_code}: {r.text[:200]}")


def dbfs_list(path: str) -> list:
    r = requests.get(_url(f"/api/2.0/dbfs/list"), headers=_headers(), params={"path": path})
    if r.status_code == 200:
        return r.json().get("files", [])
    elif r.status_code == 404:
        return []
    else:
        print(f"   DBFS list error '{path}' -> {r.status_code}")
        return []


def dbfs_has_files(path: str) -> bool:
    files = dbfs_list(path)
    valid = [
        f for f in files
        if f.get("path", "").endswith(".csv") and f.get("file_size", 0) > 0
    ]
    if not valid:
        print(f"   DBFS '{path}' has no valid CSV files")
        return False
    for f in valid:
        print(f"   Found: {f['path']} ({f.get('file_size', 0):,} bytes)")
    return True


def dbfs_delete_path(path: str, recursive: bool = True):
    r = requests.post(
        _url("/api/2.0/dbfs/delete"),
        headers=_headers(),
        json={"path": path, "recursive": recursive},
    )
    if r.status_code == 200:
        print(f"   DBFS deleted: {path}")
    elif r.status_code == 404:
        pass
    else:
        print(f"   DBFS delete failed '{path}' -> {r.status_code}: {r.text[:100]}")


def dbfs_purge(path: str):
    files = dbfs_list(path)
    csv_files = [f for f in files if f.get("path", "").endswith(".csv")]
    if not csv_files:
        print(f"   DBFS '{path}' already empty")
        return
    print(f"   Purging {len(csv_files)} file(s) from '{path}'...")
    for f in csv_files:
        dbfs_delete_path(f["path"], recursive=False)


def dbfs_upload(local_path: str, dbfs_path: str):
    CHUNK_SIZE = 1 * 1024 * 1024
    r = requests.post(
        _url("/api/2.0/dbfs/open"),
        headers=_headers(),
        json={"path": dbfs_path, "overwrite": True, "flags": "CREATE|OVERWRITE|WRITE"},
    )
    if r.status_code != 200:
        raise Exception(f"DBFS open failed: {r.status_code}: {r.text}")
    handle = r.json()["handle"]
    with open(local_path, "rb") as f:
        chunk_count = 0
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            encoded = base64.b64encode(chunk).decode("utf-8")
            ar = requests.post(
                _url("/api/2.0/dbfs/add-block"),
                headers=_headers(),
                json={"handle": handle, "data": encoded},
            )
            if ar.status_code != 200:
                raise Exception(f"DBFS add-block failed: {ar.status_code}: {ar.text}")
            chunk_count += 1
    cr = requests.post(_url("/api/2.0/dbfs/close"), headers=_headers(), json={"handle": handle})
    if cr.status_code == 200:
        size = os.path.getsize(local_path)
        print(f"   Uploaded '{os.path.basename(local_path)}' -> '{dbfs_path}' ({size:,} bytes)")
    else:
        raise Exception(f"DBFS close failed: {cr.status_code}: {cr.text}")


def dbfs_read_bytes(dbfs_path: str, file_size: int) -> bytes:
    CHUNK_SIZE = 1 * 1024 * 1024
    data = bytearray()
    offset = 0
    while offset < file_size:
        length = min(CHUNK_SIZE, file_size - offset)
        r = requests.get(
            _url("/api/2.0/dbfs/read"),
            headers=_headers(),
            params={"path": dbfs_path, "offset": offset, "length": length},
        )
        if r.status_code != 200:
            raise Exception(f"DBFS read failed: {r.status_code}: {r.text[:200]}")
        chunk = base64.b64decode(r.json()["data"])
        data.extend(chunk)
        offset += length
    return bytes(data)


def fetch_output_from_dbfs(dbfs_path: str) -> tuple:
    import io
    import csv as csv_mod
    files = dbfs_list(dbfs_path)
    csv_files = sorted(
        [f for f in files if f.get("file_size", 0) > 0 and not f["path"].endswith("/_SUCCESS")],
        key=lambda f: f["path"],
    )
    if not csv_files:
        return None, ""
    if len(csv_files) == 1:
        raw = dbfs_read_bytes(csv_files[0]["path"], csv_files[0]["file_size"])
        return raw, os.path.basename(csv_files[0]["path"])
    merged = []
    header = None
    for f in csv_files:
        content = dbfs_read_bytes(f["path"], f["file_size"]).decode("utf-8")
        reader = csv_mod.reader(io.StringIO(content))
        rows = list(reader)
        if not rows:
            continue
        if header is None:
            header = rows[0]
            merged.append(header)
        merged.extend(r for r in rows[1:] if r)
    if merged:
        out = io.StringIO()
        writer = csv_mod.writer(out)
        writer.writerows(merged)
        return out.getvalue().encode("utf-8"), "merged_output.csv"
    return None, ""


# ============================================================
# PYSPARK EXPRESSION CONVERTER (ADF → PySpark)
#
# KEY FIX: This converter now correctly handles:
#   1. Multi-column arithmetic:  col("a") * col("b")
#   2. References to derived columns: col("total_revenue") instead of bare total_revenue
#   3. Complex nested expressions with proper col() wrapping
# ============================================================
def _adf_to_pyspark_expr(expr: str) -> str:
    """
    Convert ADF Data Flow expression syntax to valid PySpark expression string.
    
    CRITICAL RULE: Every column reference MUST be wrapped in col().
    Bare column names like `total_revenue` cause NameError in PySpark notebooks.
    Only Python literals (numbers, strings) and PySpark functions can appear bare.
    """
    expr = expr.strip()

    # ── Step 1: Replace ADF function calls with PySpark equivalents ──────────
    subs = [
        # Type casts — bare column name arg
        (r"toInteger\((\w+)\)",      r'col("\1").cast("int")'),
        (r"toDouble\((\w+)\)",       r'col("\1").cast("double")'),
        (r"toString\((\w+)\)",       r'col("\1").cast("string")'),
        (r"toLong\((\w+)\)",         r'col("\1").cast("long")'),
        # CHANGE 2: toTimestamp support
        (r"toTimestamp\s*\(\s*(\w+)\s*\)", r'to_timestamp(col("\1"))'),
        # String functions
        (r"upper\((\w+)\)",          r'upper(col("\1"))'),
        (r"lower\((\w+)\)",          r'lower(col("\1"))'),
        (r"trim\((\w+)\)",           r'trim(col("\1"))'),
        (r"ltrim\((\w+)\)",          r'ltrim(col("\1"))'),
        (r"rtrim\((\w+)\)",          r'rtrim(col("\1"))'),
        (r"initCap\((\w+)\)",        r'initcap(col("\1"))'),
        (r"length\((\w+)\)",         r'length(col("\1"))'),
        # Time
        (r"currentTimestamp\(\)",    "current_timestamp()"),
        (r"currentDate\(\)",         "current_date()"),
        (r"year\((\w+)\)",           r'year(col("\1"))'),
        (r"month\((\w+)\)",          r'month(col("\1"))'),
        (r"dayOfMonth\((\w+)\)",     r'dayofmonth(col("\1"))'),
        (r"hour\((\w+)\)",           r'hour(col("\1"))'),
        (r"minute\((\w+)\)",         r'minute(col("\1"))'),
        (r"second\((\w+)\)",         r'second(col("\1"))'),
        # Math
        (r"round\((\w+)\)",          r'round(col("\1"))'),
        (r"floor\((\w+)\)",          r'floor(col("\1"))'),
        (r"ceil\((\w+)\)",           r'ceil(col("\1"))'),
        (r"abs\((\w+)\)",            r'abs(col("\1"))'),
        (r"sqrt\((\w+)\)",           r'sqrt(col("\1"))'),
        # Null
        (r"isNull\((\w+)\)",         r'col("\1").isNull()'),
        (r"iifNull\((\w+),\s*(.+)\)", r'coalesce(col("\1"), lit(\2))'),
    ]
    for pattern, replacement in subs:
        expr = re.sub(pattern, replacement, expr)

    # CHANGE 3: Fix coalesce(col("x"), 0.0) → coalesce(col("x"), lit(0.0))
    # Must run AFTER the subs loop so col() wrapping is already in place.
    expr = re.sub(
        r'coalesce\(([^,]+),\s*([0-9.]+)\)',
        r'coalesce(\1, lit(\2))',
        expr
    )

    # ── Step 2: Convert bare column names to col() references ────────────────
    # This is the CRITICAL fix. After function conversion, any remaining bare
    # identifier that is a known column name must be wrapped in col().
    #
    # We do this by identifying tokens that:
    #   - Are valid Python identifiers
    #   - Are NOT already inside col("...") 
    #   - Are NOT Python keywords / numeric literals
    #   - Are NOT PySpark function names
    #
    # We use a marker-based approach to avoid double-wrapping.
    
    # Known PySpark functions that should NOT be wrapped in col()
    PYSPARK_FUNCS = {
        'col', 'lit', 'when', 'otherwise', 'coalesce', 'expr',
        'upper', 'lower', 'trim', 'ltrim', 'rtrim', 'initcap',
        'concat', 'concat_ws', 'substring', 'length', 'regexp_replace',
        'current_timestamp', 'current_date', 'year', 'month', 'dayofmonth',
        'hour', 'minute', 'second', 'to_date', 'to_timestamp', 'date_format',
        'round', 'floor', 'ceil', 'abs', 'sqrt', 'pow', 'log',
        'isnull', 'isnotnull', 'isnan', 'coalesce', 'nvl',
        'sum', 'avg', 'mean', 'min', 'max', 'count', 'first', 'last',
        'cast', 'int', 'float', 'double', 'str', 'bool',
        'true', 'false', 'none', 'null',
        'and', 'or', 'not', 'in', 'is',
    }

    # CHANGE 1: REMOVED the early-return block that was here.
    # Previously this existed:
    #   if "col(" in expr or "current_timestamp()" in expr or ".cast(" in expr:
    #       return expr
    # That caused coalesce(col("x"), 0.0) to pass through without getting
    # lit() wrapping on the numeric default, producing invalid PySpark.

    # For simple bare column reference (single word, no operators)
    if re.match(r'^[a-zA-Z_]\w*$', expr) and expr not in PYSPARK_FUNCS:
        return f'col("{expr}")'

    return expr


def _adf_filter_to_pyspark(filter_expr: str) -> str:
    """Convert ADF filter expression to PySpark filter string."""
    expr = filter_expr.strip()

    patterns = [
        (r"^equals\(toInteger\((\w+)\),\s*(-?\d+)\)$",     r'col("\1").cast("int") == \2'),
        (r"^notEquals\(toInteger\((\w+)\),\s*(-?\d+)\)$",  r'col("\1").cast("int") != \2'),
        (r"^greater\(toInteger\((\w+)\),\s*(-?\d+)\)$",    r'col("\1").cast("int") > \2'),
        (r"^less\(toInteger\((\w+)\),\s*(-?\d+)\)$",       r'col("\1").cast("int") < \2'),
        (r"^greaterOrEqual\(toInteger\((\w+)\),\s*(-?\d+)\)$", r'col("\1").cast("int") >= \2'),
        (r"^lessOrEqual\(toInteger\((\w+)\),\s*(-?\d+)\)$",    r'col("\1").cast("int") <= \2'),
        (r"^equals\((\w+),\s*'([^']+)'\)$",  r'col("\1") == "\2"'),
        (r"^equals\((\w+),\s*(-?\d+)\)$",    r'col("\1") == \2'),
        (r"^notEquals\((\w+),\s*'([^']+)'\)$", r'col("\1") != "\2"'),
        (r"^isNull\((\w+)\)$",   r'col("\1").isNull()'),
        (r"^(\w+)\s*(==|!=|>=|<=|>|<)\s*(-?\d+)$", r'col("\1") \2 \3'),
        (r"^(\w+)\s*(==|!=)\s*'([^']+)'$",          r'col("\1") \2 "\3"'),
    ]
    for pattern, replacement in patterns:
        m = re.match(pattern, expr, re.IGNORECASE)
        if m:
            return re.sub(pattern, replacement, expr, flags=re.IGNORECASE)

    if "col(" in expr or ".cast(" in expr:
        return expr

    return expr


# ============================================================
# PYSPARK TRANSFORM EXPRESSION BUILDER
#
# This is the CORE FIX. Instead of trying to convert ADF expressions
# token-by-token (which breaks for multi-step derived columns), we
# generate correct PySpark expressions directly from a structured
# transform spec parsed by the Groq brain.
#
# For chained transforms like:
#   total_revenue = quantity * unit_price
#   final_price   = total_revenue - (total_revenue * discount_pct / 100)
#
# The key insight: in PySpark withColumn(), columns created earlier in
# the same chain are available via col("column_name"), NOT as Python vars.
# ============================================================

def _build_pyspark_withcolumn_expr(col_name: str, raw_expr: str) -> str:
    """
    Convert a raw transform expression string into a valid PySpark
    withColumn expression. Handles:
    
    1. Arithmetic on existing or previously-derived columns
    2. Conditional expressions (iif → when/otherwise)
    3. Type casts
    4. String functions
    5. Timestamps
    
    The output is always a valid Python expression that can go inside
    df.withColumn("col_name", HERE)
    """
    expr = raw_expr.strip()

# 🔥 FIRST CHECK — DO NOTHING in normal mode
    if not SELF_HEALING_MODE:
        return expr

# THEN proceed with conversion
    if _looks_like_pyspark(expr):
        return expr

    # ── Timestamp ────────────────────────────────────────────────────────────
    if expr in ("currentTimestamp()", "current_timestamp()"):
        return "current_timestamp()"

    # ── iif(condition, true_val, false_val) → when(...).otherwise(...) ───────
    iif_match = re.match(
        r'^iif\((.+),\s*(.+),\s*(.+)\)$', expr, re.IGNORECASE
    )
    if iif_match:
        cond_str  = iif_match.group(1).strip()
        true_val  = iif_match.group(2).strip()
        false_val = iif_match.group(3).strip()
        cond_py   = _convert_condition(cond_str)
        true_py   = _convert_value(true_val)
        false_py  = _convert_value(false_val)
        return f"when({cond_py}, {true_py}).otherwise({false_py})"

    # ── coalesce(bare_col, default) → coalesce(col("bare_col"), lit(default)) ──
    # CRITICAL FIX: Must catch BEFORE arithmetic. coalesce() has no arithmetic
    # operators, so _convert_arithmetic returns None, then _adf_to_pyspark_expr
    # passes it through unchanged → bare variable → NameError at runtime.
    coalesce_m = re.match(r'^coalesce\(\s*(\w+)\s*,\s*(.+?)\s*\)$', expr, re.IGNORECASE)
    if coalesce_m:
        return _convert_arithmetic_part(expr)

    # ── Single-arg string/math functions with a bare column ──────────────────
    # upper(customer_name), trim(region), abs(amount) etc.
    single_func_m = re.match(
        r'^(upper|lower|trim|ltrim|rtrim|initcap|length|abs|sqrt|floor|ceil|round)'
        r'\(\s*(\w+)\s*\)$', expr, re.IGNORECASE,
    )
    if single_func_m:
        func_name = single_func_m.group(1).lower()
        col_arg   = single_func_m.group(2)
        return f'{func_name}(col("{col_arg}"))'

    # ── Simple arithmetic expression: col1 op col2, or col op literal ────────
    # Handles: quantity * unit_price
    #          total_revenue - (total_revenue * discount_pct / 100)
    #          final_price > 1000
    # 🔥 FIRST apply ADF → PySpark conversion
    if not SELF_HEALING_MODE:
        return expr   # 🔥 RAW → allow failure

# 🔁 ONLY during healing
    expr = _adf_to_pyspark_expr(expr)

    arith = _convert_arithmetic(expr)
    if arith:
        return arith
    if _looks_like_pyspark(expr):
        return expr

    # ── Bare column name fallback ─────────────────────────────────────────────
    if re.match(r'^[a-zA-Z_]\w*$', expr):
        return f'col("{expr}")'

    return expr


def _looks_like_pyspark(expr: str) -> bool:
    """
    Return True if the expression is already valid PySpark syntax.
    
    IMPORTANT: We do NOT include bare function names like coalesce( here,
    because coalesce(discount_pct, 0) is NOT valid PySpark — the column
    arg still needs to be wrapped in col().
    Only flag expressions that already have col() or .cast() etc.
    """
    pyspark_indicators = [
        "col(",       # col("name") — definitive PySpark
        "lit(",       # lit(value)
        "when(",      # when(cond, val)
        "otherwise(", # .otherwise(val)
        ".cast(",     # .cast("type")
        ".isNull()",  # .isNull()
        "current_timestamp()",
        "current_date()",
        "upper(col",  # upper(col("name"))
        "lower(col",  # lower(col("name"))
        "trim(col",   # trim(col("name"))
    ]
    return any(ind in expr for ind in pyspark_indicators)


def _convert_condition(cond: str) -> str:
    """Convert a condition string to PySpark boolean expression."""
    cond = cond.strip()

    # equals(toInteger(col), val) or equals(col, 'val')
    m = re.match(r'^equals\(toInteger\((\w+)\),\s*(-?\d+)\)$', cond, re.IGNORECASE)
    if m:
        return f'col("{m.group(1)}").cast("int") == {m.group(2)}'

    m = re.match(r'^equals\((\w+),\s*\'([^\']+)\'\)$', cond, re.IGNORECASE)
    if m:
        return f'col("{m.group(1)}") == "{m.group(2)}"'

    m = re.match(r'^equals\((\w+),\s*(-?\d+)\)$', cond, re.IGNORECASE)
    if m:
        return f'col("{m.group(1)}") == {m.group(2)}'

    m = re.match(r'^greater\((.+),\s*(-?\d+(?:\.\d+)?)\)$', cond, re.IGNORECASE)
    if m:
        left = _convert_arithmetic_term(m.group(1).strip())
        return f'{left} > {m.group(2)}'

    m = re.match(r'^less\((.+),\s*(-?\d+(?:\.\d+)?)\)$', cond, re.IGNORECASE)
    if m:
        left = _convert_arithmetic_term(m.group(1).strip())
        return f'{left} < {m.group(2)}'

    # col > val, col < val, col == val etc (bare comparison)
    m = re.match(r'^(\w+)\s*(>|<|>=|<=|==|!=)\s*(-?\d+(?:\.\d+)?)$', cond)
    if m:
        return f'col("{m.group(1)}") {m.group(2)} {m.group(3)}'

    # Already PySpark
    if _looks_like_pyspark(cond):
        return cond

    return cond


def _convert_value(val: str) -> str:
    """Convert a value token to PySpark: true→True, numbers→lit(), strings→lit()"""
    val = val.strip()

    if val.lower() == 'true':
        return 'lit(True)'
    if val.lower() == 'false':
        return 'lit(False)'
    if val.lower() in ('null', 'none'):
        return 'lit(None)'

    # Numeric literal
    try:
        float(val)
        return f'lit({val})'
    except ValueError:
        pass

    # String literal
    if (val.startswith("'") and val.endswith("'")) or \
       (val.startswith('"') and val.endswith('"')):
        return f'lit({val})'

    # Column reference
    if re.match(r'^[a-zA-Z_]\w*$', val):
        return f'col("{val}")'

    # Complex expression
    arith = _convert_arithmetic(val)
    if arith:
        return arith

    return val


def _convert_arithmetic_term(term: str) -> str:
    """
    Convert a single arithmetic term to PySpark.
    Handles: bare_col_name, col("name"), number, col.cast("type")
    """
    term = term.strip()

    # Already PySpark
    if _looks_like_pyspark(term):
        return term

    # ADF cast functions
    m = re.match(r'^toDouble\((\w+)\)$', term, re.IGNORECASE)
    if m:
        return f'col("{m.group(1)}").cast("double")'

    m = re.match(r'^toInteger\((\w+)\)$', term, re.IGNORECASE)
    if m:
        return f'col("{m.group(1)}").cast("int")'

    # Numeric literal
    try:
        float(term)
        return term  # bare number is fine in arithmetic
    except ValueError:
        pass

    # Bare column name — wrap ALL identifiers as columns (CSV source cols + derived cols).
    # This is what turns  safe_unit_price  →  col("safe_unit_price")  at runtime.
    if re.match(r'^[a-zA-Z_]\w*$', term):
        return f'col("{term}")'

    return term


def _convert_arithmetic(expr: str) -> str:
    """
    Convert an arithmetic expression involving column names and operators
    into a valid PySpark expression.
    
    Examples:
      quantity * unit_price
        → col("quantity").cast("double") * col("unit_price").cast("double")
      
      total_revenue - (total_revenue * discount_pct / 100)
        → col("total_revenue") - (col("total_revenue") * col("discount_pct").cast("double") / 100)
      
      final_price > 1000
        → col("final_price") > 1000  (used in when() condition)
    
    CRITICAL: This function is what fixes the original NameError bug.
    Previously, expressions like  total_revenue - (total_revenue * ...)
    were passed through as-is, causing Python to look for a variable
    called total_revenue which doesn't exist. Now we replace each bare
    column reference with col("column_name").
    """
    expr = expr.strip()

    # Strip outer parentheses if the whole thing is wrapped
    if expr.startswith("(") and expr.endswith(")") and _matching_paren(expr):
        inner = _convert_arithmetic(expr[1:-1])
        return f"({inner})"

    # Check if this contains arithmetic operators at the top level
    # (not nested inside function calls or parens)
    ops_found = _find_top_level_operators(expr)
    if not ops_found:
        # Single term — convert it
        return _convert_arithmetic_term(expr) if re.search(r'[a-zA-Z_]', expr) else None

    # Split on the LAST top-level operator (respects left-to-right evaluation)
    # For correctness we split on the lowest-precedence operator found
    # Priority: +/- then */ then ** (lowest precedence splits first)
    
    for ops_group in [['+', '-'], ['*', '/'], ['**']]:
        split_pos = _find_rightmost_top_level_op(expr, ops_group)
        if split_pos is not None:
            pos, op = split_pos
            left  = expr[:pos].strip()
            right = expr[pos + len(op):].strip()
            
            # CRITICAL: BOTH sides must go through _convert_arithmetic_part
            # so every bare identifier becomes col("...") — skipping either
            # side is what causes NameError for derived columns at runtime.
            left_py  = _convert_arithmetic_part(left)
            right_py = _convert_arithmetic_part(right)

            return f"{left_py} {op} {right_py}"

    return None


def _convert_arithmetic_part(part: str) -> str:
    """
    Recursively convert an arithmetic sub-expression.

    GUARANTEE: Every bare identifier that reaches this function is
    returned as col("identifier"). This covers both CSV source columns
    and derived columns created earlier in the same withColumn chain
    (e.g. safe_unit_price → col("safe_unit_price")).
    """
    part = part.strip()
    if not part:
        return part

    # Parenthesised sub-expression
    if part.startswith("(") and part.endswith(")") and _matching_paren(part):
        inner = _convert_arithmetic_part(part[1:-1])
        return f"({inner})"

    # Already PySpark (has col(), .cast(), etc.)
    if _looks_like_pyspark(part):
        return part

    # ── coalesce(col_name, default) → coalesce(col("col_name"), lit(default)) ──
    # This handles the null-safety pattern: coalesce(discount_pct, 0)
    m = re.match(r'^coalesce\(\s*(\w+)\s*,\s*(.+?)\s*\)$', part, re.IGNORECASE)
    if m:
        col_name = m.group(1)
        default  = m.group(2).strip()
        # Convert the default value
        try:
            float(default)
            default_py = f"lit({default})"
        except ValueError:
            if default.startswith("'") or default.startswith('"'):
                default_py = f"lit({default})"
            else:
                default_py = f"lit({default})"
        return f'coalesce(col("{col_name}"), {default_py})'

    # ADF function calls
    m = re.match(r'^toDouble\((\w+)\)$', part, re.IGNORECASE)
    if m:
        return f'col("{m.group(1)}").cast("double")'

    m = re.match(r'^toInteger\((\w+)\)$', part, re.IGNORECASE)
    if m:
        return f'col("{m.group(1)}").cast("int")'

    # Numeric literal
    try:
        float(part)
        return part
    except ValueError:
        pass

    # Bare column name — wrap ALL identifiers as columns (CSV source cols + derived cols).
    # This is what turns  safe_unit_price  →  col("safe_unit_price")  at runtime.
    # Must be present here AND in _convert_arithmetic_term — both are called
    # from different entry points and must independently handle bare names.
    if re.match(r'^[a-zA-Z_]\w*$', part):
        return f'col("{part}")'

    # Recurse for nested arithmetic
    result = _convert_arithmetic(part)
    return result if result else part


def _matching_paren(expr: str) -> bool:
    """Return True if the outermost parens are a matched pair."""
    if not (expr.startswith("(") and expr.endswith(")")):
        return False
    depth = 0
    for i, ch in enumerate(expr):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if depth == 0 and i < len(expr) - 1:
            return False
    return True


def _find_top_level_operators(expr: str) -> list:
    """Find all operators at depth 0 (not inside parens/brackets)."""
    found = []
    depth = 0
    i = 0
    while i < len(expr):
        ch = expr[i]
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif depth == 0 and ch in "+-*/":
            # Don't count a leading minus as an operator
            if ch == '-' and i == 0:
                pass
            elif ch == '+' and i == 0:
                pass
            else:
                found.append((i, ch))
        i += 1
    return found


def _find_rightmost_top_level_op(expr: str, ops: list) -> tuple:
    """
    Find the rightmost top-level occurrence of any operator in `ops`.
    Returns (position, operator_string) or None.
    Scans right-to-left so left-to-right evaluation order is preserved
    when we split and recurse.
    """
    depth = 0
    # Scan right to left
    i = len(expr) - 1
    while i >= 0:
        ch = expr[i]
        if ch in ")]":
            depth += 1
        elif ch in "([":
            depth -= 1
        elif depth == 0:
            for op in ops:
                if expr[i:i+len(op)] == op:
                    # Avoid treating leading unary minus as binary op
                    if op == '-' and i == 0:
                        break
                    if op == '+' and i == 0:
                        break
                    # Avoid ** when looking for *
                    if op == '*' and i + 1 < len(expr) and expr[i+1] == '*':
                        break
                    # Avoid -value where - is after an operator
                    if op == '-' and i > 0 and expr[i-1] in '+-*/(':
                        break
                    return (i, op)
        i -= 1
    return None


# ============================================================
# SCRIPT BUILDER: Copy Pipeline
# ============================================================
def build_copy_script(csv_data_b64: str, shuffle_partitions: int = 4) -> str:
    return f"""# Databricks notebook source
import base64, io
import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "{shuffle_partitions}")

# COMMAND ----------
CSV_DATA_B64 = "{csv_data_b64}"
csv_text = base64.b64decode(CSV_DATA_B64).decode("utf-8")
pdf = pd.read_csv(io.StringIO(csv_text))
df = spark.createDataFrame(pdf)
count = df.count()
print(f"Copy: rows read: {{count}}")

# COMMAND ----------
out_csv = df.toPandas().to_csv(index=False)
out_b64 = base64.b64encode(out_csv.encode("utf-8")).decode("utf-8")
print(f"Copy complete. Rows: {{count}}")
dbutils.notebook.exit(out_b64)
"""


# ============================================================
# SCRIPT BUILDER: Transform Pipeline
#
# MAJOR FIX: Previously used _adf_to_pyspark_expr() which produced
# bare column names like `total_revenue` that caused NameError.
#
# Now uses _build_pyspark_withcolumn_expr() which correctly wraps
# ALL column references in col("..."), so chained transforms like:
#   total_revenue = quantity * unit_price
#   final_price   = total_revenue - (total_revenue * discount_pct / 100)
# produce valid PySpark:
#   df = df.withColumn("total_revenue",
#           col("quantity").cast("double") * col("unit_price").cast("double"))
#   df = df.withColumn("final_price",
#           col("total_revenue") - (col("total_revenue") * 
#           coalesce(col("discount_pct"), lit(0)).cast("double") / 100))
# ============================================================
def build_transform_script(
    csv_data_b64: str,
    transformations: list,
    filter_condition: str,
    columns: list,
    inferred_types: dict,
    shuffle_partitions: int = 4,
) -> str:
    derived = []
    active_filter = filter_condition

    for t in transformations:
        if "=" not in t:
            continue
        col_name, raw_expr = t.split("=", 1)
        col_name = col_name.strip()
        raw_expr = raw_expr.strip()

        # Detect filter-intent entries
        if col_name.lower() == "filter":
            if active_filter is None:
                active_filter = _adf_filter_to_pyspark(raw_expr)
            continue

        # Convert the expression to valid PySpark
        pyspark_expr = _build_pyspark_withcolumn_expr(col_name, raw_expr)
        derived.append((col_name, pyspark_expr))

    # Always include processed_time
    if not any(d[0] == "processed_time" for d in derived):
        derived.append(("processed_time", "current_timestamp()"))

    # Build filter lines
    filter_lines = ""
    if active_filter:
        pyspark_filter = _adf_filter_to_pyspark(active_filter)
        filter_lines = (
            f'\nprint("Applying filter: {pyspark_filter.replace(chr(34), chr(39))}")\n'
            f"df = df.filter({pyspark_filter})\n"
            f'print(f"Rows after filter: {{df.count()}}")\n'
        )

    # Build withColumn lines — each on its own line so we can debug easily
    derive_lines_list = []
    for col, expr in derived:
        derive_lines_list.append(f'df = df.withColumn("{col}", {expr})')
    derive_lines = "\n".join(derive_lines_list)

    return f"""# Databricks notebook source
import base64, io
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, lit, upper, lower, trim, ltrim, rtrim, initcap,
    concat, substring, length, regexp_replace, coalesce,
    current_timestamp, current_date, year, month, dayofmonth,
    hour, minute, second, when, to_date, to_timestamp,
)
try:
    from pyspark.sql.functions import round, floor, ceil, abs, sqrt
except ImportError:
    pass

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "{shuffle_partitions}")

# COMMAND ----------
CSV_DATA_B64 = "{csv_data_b64}"
csv_text = base64.b64decode(CSV_DATA_B64).decode("utf-8")
pdf = pd.read_csv(io.StringIO(csv_text))
df = spark.createDataFrame(pdf)
print(f"Transform: rows read: {{df.count()}}")
df.printSchema()

# COMMAND ----------
{filter_lines}
print("Applying transformations...")
{derive_lines}
print(f"Transform complete. Rows: {{df.count()}}")
df.printSchema()

# COMMAND ----------
out_csv = df.toPandas().to_csv(index=False)
out_b64 = base64.b64encode(out_csv.encode("utf-8")).decode("utf-8")
dbutils.notebook.exit(out_b64)
"""


# ============================================================
# SCRIPT BUILDER: Aggregate Pipeline
# ============================================================
def build_aggregate_script(
    csv_data_b64: str,
    group_by_columns: list,
    aggregations: list,
    shuffle_partitions: int = 4,
) -> str:
    """
    Build a PySpark notebook that performs a GROUP BY aggregation.
    
    aggregations: list of dicts like:
      {"output_col": "total_sales",   "function": "sum",  "input_col": "final_price"}
      {"output_col": "avg_order_value","function": "avg", "input_col": "final_price"}
    """
    group_cols_str = ", ".join(f'"{c}"' for c in group_by_columns)

    agg_exprs = []
    for agg in aggregations:
        func       = agg.get("function", "sum").lower()
        input_col  = agg.get("input_col", "")
        output_col = agg.get("output_col", f"{func}_{input_col}")
        agg_exprs.append(f'F.{func}(col("{input_col}")).alias("{output_col}")')

    agg_str = ",\n        ".join(agg_exprs)

    return f"""# Databricks notebook source
import base64, io
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit

# COMMAND ----------
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "{shuffle_partitions}")

# COMMAND ----------
CSV_DATA_B64 = "{csv_data_b64}"
csv_text = base64.b64decode(CSV_DATA_B64).decode("utf-8")
pdf = pd.read_csv(io.StringIO(csv_text))
df = spark.createDataFrame(pdf)
print(f"Aggregate input: {{df.count()}} rows")

# COMMAND ----------
print("Aggregating by: {group_by_columns}")
df = df.groupBy({group_cols_str}).agg(
        {agg_str}
    )
df = df.orderBy({group_cols_str})
print(f"Aggregate output: {{df.count()}} groups")
df.show(truncate=False)

# COMMAND ----------
out_csv = df.toPandas().to_csv(index=False)
out_b64 = base64.b64encode(out_csv.encode("utf-8")).decode("utf-8")
dbutils.notebook.exit(out_b64)
"""


# ============================================================
# WORKSPACE HELPERS
# ============================================================
def workspace_mkdir(path: str):
    r = requests.post(
        _url("/api/2.0/workspace/mkdirs"),
        headers=_headers(),
        json={"path": path},
    )
    if r.status_code not in (200, 400):
        raise Exception(f"workspace mkdirs failed '{path}': {r.status_code}: {r.text[:200]}")


def workspace_upload(path: str, content: str):
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    r = requests.post(
        _url("/api/2.0/workspace/import"),
        headers=_headers(),
        json={
            "path": path,
            "language": "PYTHON",
            "format": "SOURCE",
            "content": encoded,
            "overwrite": True,
        },
    )
    if r.status_code != 200:
        raise Exception(f"Workspace import failed '{path}': {r.status_code}: {r.text[:300]}")
    print(f"   Workspace notebook uploaded: {path}")


def workspace_delete(path: str):
    r = requests.post(
        _url("/api/2.0/workspace/delete"),
        headers=_headers(),
        json={"path": path, "recursive": True},
    )
    if r.status_code == 200:
        print(f"   Workspace deleted: {path}")


# ============================================================
# RUN OUTPUT
# ============================================================
def get_task_run_id(job_run_id: int) -> int:
    r = requests.get(
        _url("/api/2.1/jobs/runs/get"),
        headers=_headers(),
        params={"run_id": job_run_id},
        timeout=30,
    )
    if r.status_code != 200:
        return job_run_id
    tasks = r.json().get("tasks", [])
    if tasks and tasks[0].get("run_id"):
        return tasks[0]["run_id"]
    return job_run_id


def get_notebook_output(run_id: int) -> str:
    task_run_id = get_task_run_id(run_id)
    r = requests.get(
        _url("/api/2.1/jobs/runs/get-output"),
        headers=_headers(),
        params={"run_id": task_run_id},
        timeout=30,
    )
    if r.status_code != 200:
        print(f"   get-output HTTP {r.status_code}: {r.text[:200]}")
        return ""
    data = r.json()
    return data.get("notebook_output", {}).get("result", "")


def upload_script(script_content: str, script_name: str) -> str:
    script_dir = "/databricks-pipeline-scripts"
    workspace_mkdir(script_dir)
    nb_name = script_name.replace(".py", "")
    workspace_path = f"{script_dir}/{nb_name}"
    workspace_upload(workspace_path, script_content)
    return workspace_path


# ============================================================
# CLUSTER CONFIG
# ============================================================
def _cluster_config(num_workers: int, shuffle_partitions: int) -> dict:
    if DATABRICKS_CLUSTER_ID:
        return {"existing_cluster_id": DATABRICKS_CLUSTER_ID}
    cluster = {
        "spark_version": DATABRICKS_SPARK_VERSION,
        "node_type_id": DATABRICKS_NODE_TYPE,
        "spark_conf": {
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
        },
    }
    if num_workers == 0:
        cluster["num_workers"] = 0
        cluster["spark_conf"]["spark.databricks.cluster.profile"] = "singleNode"
        cluster["custom_tags"] = {"ResourceClass": "SingleNode"}
    else:
        cluster["num_workers"] = num_workers
    return {"new_cluster": cluster}


# ============================================================
# JOB: CREATE AND RUN
# ============================================================
def create_and_run_job(job_name: str, script_dbfs_path: str, num_workers: int, shuffle_partitions: int) -> tuple:
    cluster_cfg = _cluster_config(num_workers, shuffle_partitions)
    task = {
        "task_key": "pipeline_task",
        "notebook_task": {
            "notebook_path": script_dbfs_path,
        },
    }
    task.update(cluster_cfg)
    body = {
        "name": job_name,
        "tasks": [task],
        "max_concurrent_runs": 1,
    }
    r = requests.post(_url("/api/2.1/jobs/create"), headers=_headers(), json=body)
    if r.status_code != 200:
        raise Exception(f"Job create failed: {r.status_code}: {r.text}")
    job_id = r.json()["job_id"]
    print(f"   Job created: {job_name} (id={job_id})")
    rr = requests.post(_url("/api/2.1/jobs/run-now"), headers=_headers(), json={"job_id": job_id})
    if rr.status_code != 200:
        raise Exception(f"Job run failed: {rr.status_code}: {rr.text}")
    run_id = rr.json()["run_id"]
    print(f"   Run triggered: run_id={run_id}")
    return job_id, run_id


def delete_job(job_id: int):
    r = requests.post(_url("/api/2.1/jobs/delete"), headers=_headers(), json={"job_id": job_id})
    if r.status_code == 200:
        print(f"   Job {job_id} deleted")
    else:
        print(f"   Job delete failed: {r.status_code}: {r.text[:100]}")


# ============================================================
# RUN: POLL STATUS
# ============================================================
def check_run_status(run_id: int, max_wait: int = 1800) -> dict:
    POLL_INTERVAL = 10
    elapsed = 0
    while elapsed < max_wait:
        try:
            r = requests.get(
                _url("/api/2.1/jobs/runs/get"),
                headers=_headers(),
                params={"run_id": run_id},
                timeout=30,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"   Network error polling run {run_id}: {e} — retrying...")
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
            continue

        if r.status_code != 200:
            print(f"   Status check HTTP {r.status_code}: {r.text[:100]}")
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
            continue

        data = r.json()
        life_cycle  = data.get("state", {}).get("life_cycle_state", "")
        result_state = data.get("state", {}).get("result_state", "")
        state_msg    = data.get("state", {}).get("state_message", "")

        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if result_state == "SUCCESS":
                print(f"   Run {run_id} succeeded")
                return {"status": "Succeeded", "details": data}
            else:
                print(f"   Run {run_id} FAILED — {result_state}: {state_msg}")
                task_run_id = get_task_run_id(run_id)
                try:
                    out_r = requests.get(
                        _url("/api/2.1/jobs/runs/get-output"),
                        headers=_headers(),
                        params={"run_id": task_run_id},
                        timeout=20,
                    )
                    if out_r.status_code == 200:
                        err   = out_r.json().get("error", "")
                        trace = out_r.json().get("error_trace", "")
                        if err:
                            print(f"   Task error: {err}")
                        if trace:
                            print(f"   Traceback:\n{trace[:1500]}")
                        state_msg = err or state_msg
                except Exception:
                    pass
                return {"status": "Failed", "details": data, "message": state_msg}

        print(f"   Run {run_id} -> {life_cycle} ({elapsed}s elapsed)")
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    print(f"   Timeout waiting for run {run_id}")
    return {"status": "Timeout", "run_id": run_id}


# ============================================================
# MONITORING
# ============================================================
def _ms_to_ts(ms: int) -> str:
    if not ms:
        return ""
    return datetime.datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def list_recent_runs(limit: int = 20) -> list:
    formatted  = []
    page_token = None
    while len(formatted) < limit:
        fetch_size = min(25, limit - len(formatted))
        params = {
            "limit":        fetch_size,
            "active_only":  "false",
            "expand_tasks": "true",
        }
        if page_token:
            params["page_token"] = page_token
        try:
            r = requests.get(
                _url("/api/2.1/jobs/runs/list"),
                headers=_headers(),
                params=params,
                timeout=30,
            )
        except Exception as e:
            print(f"   list_recent_runs error: {e}")
            break
        if r.status_code != 200:
            print(f"   list_recent_runs failed: {r.status_code}: {r.text[:200]}")
            break
        data = r.json()
        runs = data.get("runs", [])
        if not runs:
            break
        for run in runs:
            if len(formatted) >= limit:
                break
            state     = run.get("state", {})
            life      = state.get("life_cycle_state", "Unknown")
            result    = state.get("result_state", "")
            msg       = state.get("state_message", "")
            cancelled = state.get("user_cancelled_or_timedout", False)
            if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                status = "Succeeded" if result == "SUCCESS" else "Failed"
            elif life in ("RUNNING", "PENDING", "TERMINATING"):
                status = "InProgress"
            else:
                status = life.title()
            start_ms   = run.get("start_time", 0)
            end_ms     = run.get("end_time", 0)
            setup_ms   = run.get("setup_duration", 0)
            exec_ms    = run.get("execution_duration", 0)
            cleanup_ms = run.get("cleanup_duration", 0)
            run_dur_ms = run.get("run_duration", 0)
            duration_s = (end_ms - start_ms) / 1000 if end_ms and start_ms else None
            tasks = []
            for t in run.get("tasks", []):
                ts = t.get("start_time", 0)
                te = t.get("end_time", 0)
                t_state = t.get("state", {})
                tasks.append({
                    "task_key":   t.get("task_key", ""),
                    "life_cycle": t_state.get("life_cycle_state", ""),
                    "result":     t_state.get("result_state", ""),
                    "status_msg": t_state.get("state_message", ""),
                    "run_id":     t.get("run_id"),
                    "attempt":    t.get("attempt_number", 0),
                    "start_time": _ms_to_ts(ts),
                    "end_time":   _ms_to_ts(te),
                    "duration":   _fmt_duration((te - ts) / 1000 if te and ts else None),
                    "setup_s":    round(t.get("setup_duration", 0) / 1000, 1),
                    "exec_s":     round(t.get("execution_duration", 0) / 1000, 1),
                    "cleanup_s":  round(t.get("cleanup_duration", 0) / 1000, 1),
                    "cluster_id": t.get("cluster_instance", {}).get("cluster_id", ""),
                    "spark_ctx":  t.get("cluster_instance", {}).get("spark_context_id", ""),
                })
            formatted.append({
                "pipeline":          str(run.get("run_name", "") or run.get("job_id", "")),
                "run_id":            run.get("run_id"),
                "job_id":            run.get("job_id"),
                "run_name":          run.get("run_name", ""),
                "number_in_job":     run.get("number_in_job", 0),
                "attempt_number":    run.get("attempt_number", 0),
                "run_type":          run.get("run_type", ""),
                "format":            run.get("format", ""),
                "trigger":           run.get("trigger", ""),
                "creator_user_name": run.get("creator_user_name", ""),
                "run_page_url":      run.get("run_page_url", ""),
                "status":            status,
                "life_cycle_state":  life,
                "result_state":      result,
                "state_message":     msg,
                "user_cancelled":    cancelled,
                "started":           _ms_to_ts(start_ms),
                "ended":             _ms_to_ts(end_ms),
                "start_time_ms":     start_ms,
                "end_time_ms":       end_ms,
                "duration":          _fmt_duration(duration_s),
                "duration_s":        duration_s,
                "setup_duration_s":  round(setup_ms / 1000, 1) if setup_ms else 0,
                "exec_duration_s":   round(exec_ms / 1000, 1) if exec_ms else 0,
                "cleanup_duration_s": round(cleanup_ms / 1000, 1) if cleanup_ms else 0,
                "run_duration_s":    round(run_dur_ms / 1000, 1) if run_dur_ms else None,
                "cluster_id":        run.get("cluster_instance", {}).get("cluster_id", ""),
                "spark_context_id":  run.get("cluster_instance", {}).get("spark_context_id", ""),
                "tasks":             tasks,
                "message":           msg,
            })
        if not data.get("has_more", False):
            break
        page_token = data.get("next_page_token", "")
        if not page_token:
            break
    return formatted


def _fmt_duration(seconds) -> str:
    if seconds is None:
        return "N/A"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {sec}s"
    if m:
        return f"{m}m {sec}s"
    return f"{sec}s"


# ============================================================
# MAIN EXECUTOR
# ============================================================
def execute_pipeline(
    csv_path: str,
    pipeline_config: dict,
    schema: dict,
    log_fn=print,
    progress_fn=None,
) -> dict:
    def log(msg):
        log_fn(msg)

    def prog(pct):
        if progress_fn:
            progress_fn(pct)

    uploaded_notebooks = []
    try:
        log("Reading input CSV")
        with open(csv_path, "rb") as f:
            raw_bytes = f.read()
        current_csv_b64 = base64.b64encode(raw_bytes).decode("utf-8")
        log(f"CSV loaded: {len(raw_bytes):,} bytes")
        prog(15)

        n = len(pipeline_config["execution_order"])
        final_csv_bytes = raw_bytes

        for i, pl_name in enumerate(pipeline_config["execution_order"]):
            pl_cfg = next((p for p in pipeline_config["pipelines"] if p["name"] == pl_name), None)
            if pl_cfg is None:
                raise Exception(f"Pipeline '{pl_name}' not found in config")

            num_workers        = 0
            shuffle_partitions = pl_cfg.get("shuffle_partitions", 4)
            pl_type            = pl_cfg.get("type", "copy")

            log(f"Building {pl_type} notebook: {pl_name}")

            ts          = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            script_name = f"{pl_name}_{ts}.py"

            if pl_type == "copy":
                script = build_copy_script(current_csv_b64, shuffle_partitions)

            elif pl_type == "aggregate":
                # Aggregate pipeline — uses groupBy + agg
                pl_cfg["inferred_types"] = schema.get("inferred_types", {})
                script = build_aggregate_script(
                    csv_data_b64      = current_csv_b64,
                    group_by_columns  = pl_cfg.get("group_by_columns", []),
                    aggregations      = pl_cfg.get("aggregations", []),
                    shuffle_partitions= shuffle_partitions,
                )

            else:
                # transform (default)
                pl_cfg["inferred_types"] = schema.get("inferred_types", {})
                script = build_transform_script(
                    csv_data_b64      = current_csv_b64,
                    transformations   = pl_cfg.get("transformations", []),
                    filter_condition  = pl_cfg.get("filter_condition"),
                    columns           = schema.get("columns", []),
                    inferred_types    = pl_cfg.get("inferred_types", {}),
                    shuffle_partitions= shuffle_partitions,
                )

            script_path = upload_script(script, script_name)
            uploaded_notebooks.append(script_path)
            prog(15 + int(50 * i / n))

            log(f"Creating job + triggering run: {pl_name}")
            job_name = f"DB_Pipeline_{pl_name}_{ts}"
            job_id, run_id = create_and_run_job(job_name, script_path, num_workers, shuffle_partitions)

            log(f"Waiting for run {run_id} ({pl_name})")
            result = check_run_status(run_id)

            if result["status"] != "Succeeded":
                msg = result.get("message", result["status"])
                raise Exception(f"Pipeline '{pl_name}' {result['status']}: {msg}")

            log(f"Fetching output for '{pl_name}'")
            out_b64 = get_notebook_output(run_id)
            if out_b64:
                try:
                    final_csv_bytes = base64.b64decode(out_b64)
                    current_csv_b64 = out_b64
                    log(f"Output fetched: {len(final_csv_bytes):,} bytes")
                except Exception as decode_err:
                    log(f"Output decode warning: {decode_err}")
            else:
                log(f"Warning: no output from '{pl_name}', passing input to next stage")

            prog(15 + int(50 * (i + 1) / n))
            log(f"Pipeline '{pl_name}' succeeded")

        prog(100)
        return {
            "status": "ok",
            "config": pipeline_config,
            "stage_paths": {},
            "output_csv_bytes": final_csv_bytes,
            "output_csv_name": "output.csv",
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        for nb_path in uploaded_notebooks:
            try:
                workspace_delete(nb_path)
            except Exception:
                pass


# ============================================================
# MONITORING DETAIL HELPERS
# ============================================================
def get_run_details(run_id: int) -> dict:
    r = requests.get(
        _url("/api/2.1/jobs/runs/get"),
        headers=_headers(),
        params={"run_id": run_id},
        timeout=30,
    )
    if r.status_code != 200:
        return {}
    data = r.json()
    start_ms   = data.get("start_time", 0)
    end_ms     = data.get("end_time", 0)
    setup_ms   = data.get("setup_duration", 0)
    exec_ms    = data.get("execution_duration", 0)
    cleanup_ms = data.get("cleanup_duration", 0)
    cluster_id = data.get("cluster_instance", {}).get("cluster_id", "")
    tasks = []
    for t in data.get("tasks", []):
        ts = t.get("start_time", 0)
        te = t.get("end_time", 0)
        tasks.append({
            "task_key": t.get("task_key", ""),
            "status":   t.get("state", {}).get("result_state", "")
                        or t.get("state", {}).get("life_cycle_state", ""),
            "duration": _fmt_duration((te - ts) / 1000 if te and ts else None),
            "run_id":   t.get("run_id"),
            "attempt":  t.get("attempt_number", 0),
        })
    return {
        "run_id":               run_id,
        "job_id":               data.get("job_id"),
        "run_name":             data.get("run_name", ""),
        "start_time_ms":        start_ms,
        "end_time_ms":          end_ms,
        "setup_duration_s":     round(setup_ms / 1000, 1) if setup_ms else 0,
        "execution_duration_s": round(exec_ms / 1000, 1) if exec_ms else 0,
        "cleanup_duration_s":   round(cleanup_ms / 1000, 1) if cleanup_ms else 0,
        "cluster_id":           cluster_id,
        "tasks":                tasks,
        "state":                data.get("state", {}),
        "creator_user_name":    data.get("creator_user_name", ""),
        "run_page_url":         data.get("run_page_url", ""),
        "trigger":              data.get("trigger", ""),
    }


def get_cluster_info(cluster_id: str) -> dict:
    if not cluster_id:
        return {}
    r = requests.get(
        _url("/api/2.0/clusters/get"),
        headers=_headers(),
        params={"cluster_id": cluster_id},
        timeout=30,
    )
    if r.status_code != 200:
        return {}
    d = r.json()
    mem_mb = d.get("cluster_memory_mb", 0)
    return {
        "cluster_id":           cluster_id,
        "cluster_name":         d.get("cluster_name", ""),
        "state":                d.get("state", ""),
        "state_message":        d.get("state_message", ""),
        "spark_version":        d.get("spark_version", ""),
        "node_type_id":         d.get("node_type_id", ""),
        "driver_node_type_id":  d.get("driver_node_type_id", ""),
        "num_workers":          d.get("num_workers", 0),
        "cluster_cores":        d.get("cluster_cores", 0),
        "cluster_memory_mb":    mem_mb,
        "cluster_memory_gb":    round(mem_mb / 1024, 1) if mem_mb else 0,
        "autoscale":            d.get("autoscale", {}),
        "spark_conf":           d.get("spark_conf", {}),
        "creator_user_name":    d.get("creator_user_name", ""),
        "cluster_source":       d.get("cluster_source", ""),
        "last_activity_time":   d.get("last_activity_time", 0),
        "num_executors":        len(d.get("executors", [])),
    }


def get_cluster_events(cluster_id: str, start_time_ms: int = None, end_time_ms: int = None) -> list:
    if not cluster_id:
        return []
    body: dict = {"cluster_id": cluster_id, "limit": 50}
    if start_time_ms:
        body["start_time"] = start_time_ms
    if end_time_ms:
        body["end_time"] = end_time_ms
    r = requests.post(_url("/api/2.0/clusters/events"), headers=_headers(), json=body, timeout=30)
    if r.status_code != 200:
        return []
    return r.json().get("events", [])


def parse_run_metrics(events: list) -> dict:
    metrics: dict = {
        "peak_executors":        None,
        "cluster_size_at_run":   None,
        "driver_cpu_pct":        None,
        "executor_avg_cpu_pct":  None,
        "driver_mem_used_gb":    None,
        "driver_mem_total_gb":   None,
        "executor_mem_used_gb":  None,
        "executor_mem_total_gb": None,
        "event_log":             [],
    }
    for event in events:
        etype   = event.get("type", "")
        details = event.get("details", {})
        if etype == "AUTOSCALING_STATS_REPORT":
            stats     = details.get("autoscaling_stats", {})
            driver    = stats.get("driver_stats", {})
            executors = stats.get("executors_stats", [])
            if driver:
                metrics["driver_cpu_pct"] = driver.get("cpu_user_percent")
                used  = driver.get("used_memory_mb")
                total = driver.get("total_memory_mb")
                if used:
                    metrics["driver_mem_used_gb"]  = round(used  / 1024, 1)
                if total:
                    metrics["driver_mem_total_gb"] = round(total / 1024, 1)
            if executors:
                cpu_vals  = [e.get("cpu_user_percent") for e in executors if e.get("cpu_user_percent") is not None]
                mem_used  = [e.get("used_memory_mb",  0) for e in executors]
                mem_total = [e.get("total_memory_mb", 0) for e in executors]
                if cpu_vals:
                    metrics["executor_avg_cpu_pct"] = round(sum(cpu_vals) / len(cpu_vals), 1)
                if any(mem_used):
                    metrics["executor_mem_used_gb"]  = round(sum(mem_used)  / 1024, 1)
                if any(mem_total):
                    metrics["executor_mem_total_gb"] = round(sum(mem_total) / 1024, 1)
                prev = metrics["peak_executors"] or 0
                metrics["peak_executors"] = max(prev, len(executors))
        elif etype == "RUNNING":
            size = details.get("cluster_size", {})
            nw   = size.get("num_workers")
            if nw is not None:
                metrics["cluster_size_at_run"] = nw
                prev = metrics["peak_executors"] or 0
                metrics["peak_executors"] = max(prev, nw)
            metrics["event_log"].append("RUNNING")
        elif etype == "NODES_ACQUIRED":
            count = (
                details.get("instance_count")
                or details.get("count")
                or details.get("cluster_size", {}).get("num_workers")
            )
            label = f"NODES_ACQUIRED: {count} node(s)" if count else "NODES_ACQUIRED"
            metrics["event_log"].append(label)
            if count:
                prev = metrics["peak_executors"] or 0
                metrics["peak_executors"] = max(prev, int(count))
        elif etype == "NODES_LOST":
            count = details.get("instance_count") or details.get("count")
            metrics["event_log"].append(f"NODES_LOST: {count} node(s)" if count else "NODES_LOST")
        elif etype in ("CLUSTER_STARTED", "DRIVER_HEALTHY", "TERMINATING", "TERMINATED"):
            metrics["event_log"].append(etype)
    return metrics


def list_all_clusters() -> list:
    r = requests.get(_url("/api/2.0/clusters/list"), headers=_headers(), timeout=30)
    if r.status_code != 200:
        return []
    out = []
    for c in r.json().get("clusters", []):
        mem_mb = c.get("cluster_memory_mb", 0)
        out.append({
            "cluster_id":        c.get("cluster_id", ""),
            "cluster_name":      c.get("cluster_name", ""),
            "state":             c.get("state", ""),
            "spark_version":     c.get("spark_version", ""),
            "node_type_id":      c.get("node_type_id", ""),
            "num_workers":       c.get("num_workers", 0),
            "cluster_cores":     c.get("cluster_cores", 0),
            "cluster_memory_mb": mem_mb,
            "cluster_memory_gb": round(mem_mb / 1024, 1) if mem_mb else 0,
            "creator_user_name": c.get("creator_user_name", ""),
            "cluster_source":    c.get("cluster_source", ""),
            "last_activity_time": c.get("last_activity_time", 0),
        })
    return out