import re
import json
from adf_api import *


class SelfHealingAgent:

    def __init__(self, token):
        self.token = token

    # ============================================================
    # MAIN ENTRY POINT
    # ============================================================
    def heal(self, error_message: str, pipeline_config: dict, csv_columns: list = None):
        print("\n🛠  SELF-HEALING AGENT ACTIVATED")

        cause_info = self.detect_root_cause(error_message, pipeline_config)
        cause = cause_info["cause"]

        print(f"🔍 Detected issue: {cause} | Details: {cause_info.get('details', {})}")

        fixed_config = self.apply_fix(cause_info, error_message, pipeline_config, csv_columns=csv_columns)

        print("🔧 Fix applied to pipeline config — caller will redeploy")

        return True, fixed_config

    # ============================================================
    # ROOT CAUSE DETECTION
    # ============================================================
    def detect_root_cause(self, error_message: str, pipeline_config: dict = None) -> dict:
        """
        Detect root cause from either:
        - A structured error message from ADF (real infrastructure failure)
        - Dropped transforms stored on the pipeline config (planner limitation)
        """

        # ── Check for planner-dropped transforms first ──────────────────
        # Natural trigger: Groq generated expressions with unknown column/
        # function references that the validator could not reconcile.
        if pipeline_config:
            all_dropped = []
            for p in pipeline_config.get("pipelines", []):
                dropped = p.get("_dropped_transforms", [])
                if dropped:
                    all_dropped.extend([
                        {**d, "pipeline_name": p["name"]}
                        for d in dropped
                    ])
            if all_dropped:
                first = all_dropped[0]
                col = first.get("column", "unknown_column")
                return {
                    "cause": "invalid_transforms",
                    "details": {
                        "dropped": all_dropped,
                        "column": col,
                        "count": len(all_dropped),
                    }
                }

        # ── ADF error envelope parsing ───────────────────────────────────
        error_lower = error_message.lower()

        status_code = None
        try:
            parsed = json.loads(error_message)
            status_code = parsed.get("StatusCode", "")
            msg = parsed.get("Message", error_message)
            error_lower = msg.lower()
            error_message = msg
        except (json.JSONDecodeError, TypeError):
            pass

        # ── ADF DataFlow error code routing ─────────────────────────────
        if status_code:
            if status_code in ("DF-Executor-InvalidOutputColumns",
                               "DF-Executor-InvalidColumnMapping"):
                return {"cause": "dataflow_error", "details": {
                    "code": status_code,
                    "hint": "sink column mapping missing or schema mismatch"
                }}

            if status_code in ("DF-Executor-SourceInvalidData",
                               "DF-Executor-InvalidData"):
                col = self._extract_column(error_message)
                return {"cause": "null_values", "details": {"column": col, "code": status_code}}

            if status_code in ("DF-Executor-UserError", "DF-EXPR-010",
                               "DF-Executor-ExpressionError"):
                col = self._extract_column(error_message)
                return {"cause": "expression_error", "details": {"column": col, "code": status_code}}

            if status_code in ("DF-Executor-AuthError",) or any(
                x in error_lower for x in ["403", "forbidden", "signature", "authentication"]
            ):
                return {"cause": "storage_auth", "details": {"code": status_code}}

            if "timeout" in error_lower or status_code == "DF-Executor-Timeout":
                return {"cause": "timeout", "details": {"code": status_code}}

        # ── Fallback: keyword matching on raw message ────────────────────
        col = self._extract_column(error_message)

        if any(x in error_lower for x in [
            "null value", "non-null", "non-nullable", "contains null"
        ]):
            return {"cause": "null_values", "details": {"column": col}}

        if any(x in error_lower for x in [
            "cannot convert", "invalid value", "cast", "tointeger",
            "type conversion", "type mismatch"
        ]):
            return {"cause": "type_cast_error", "details": {"column": col}}

        if any(x in error_lower for x in [
            "schema", "mismatch", "incompatible", "cannot map", "0 output columns",
            "no column", "column mapping"
        ]):
            return {"cause": "dataflow_error", "details": {"column": col}}

        if any(x in error_lower for x in [
            "authentication", "authorization", "403", "forbidden", "signature"
        ]):
            return {"cause": "storage_auth", "details": {}}

        if any(x in error_lower for x in [
            "dataflow", "df-executor", "df-expr", "expression", "script"
        ]):
            return {"cause": "expression_error", "details": {"column": col}}

        if "timeout" in error_lower:
            return {"cause": "timeout", "details": {}}

        if any(x in error_lower for x in [
            "usererrorsourceblobnotexist", "blob is missing",
            "container does not exist", "blobnotfound",
            "the specified container does not exist",
        ]):
            container = "incoming"
            m = re.search(r"containername[:\s]+([a-z0-9\-]+)", error_lower)
            if m:
                container = m.group(1)
            return {"cause": "blob_missing", "details": {"container": container}}

        if "copy" in error_lower and "failed" in error_lower:
            return {"cause": "copy_failure", "details": {}}

        return {"cause": "unknown", "details": {}}

    def _extract_column(self, message: str) -> str:
        """Try to pull a column name out of any ADF error message."""
        patterns = [
            r"column ['\"]?([a-zA-Z_][a-zA-Z0-9_]*)['\"]?",
            r"field ['\"]?([a-zA-Z_][a-zA-Z0-9_]*)['\"]?",
            r"'([a-zA-Z_][a-zA-Z0-9_]*)'",
        ]
        for pattern in patterns:
            m = re.search(pattern, message, re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return "unknown_column"

    # ============================================================
    # HELPERS
    # ============================================================
    def _get_known_columns(self, config: dict, csv_columns: list = None) -> list:
        """
        Return the real CSV column names for fuzzy matching.

        Priority:
          1. csv_columns passed in directly from schema — this is the
             authoritative ground truth from the actual uploaded file.
             Always use this when available.
          2. Legacy fallback: scan LHS of valid transforms in the config.
             Less reliable because these are often derived/computed names,
             not the original CSV columns.

        NOTE: We deliberately do NOT harvest invalid_tokens from
        _dropped_transforms here. Those tokens are Groq's hallucinated
        column names — adding them to known_columns causes the fuzzy
        matcher to match unknowns against other unknowns, which produces
        wrong substitutions or a no-op that silently drops the transform.
        """
        if csv_columns:
            return list(csv_columns)

        # Legacy fallback — only reached if csv_columns was not passed in
        known = set()
        for p in config.get("pipelines", []):
            if p.get("type") != "dataflow":
                continue
            for t in p.get("transformations", []):
                if "=" in t:
                    col = t.split("=", 1)[0].strip()
                    known.add(col)

        return list(known)

    def _fuzzy_match_column(self, unknown: str, known_columns: list) -> str:
        """
        Find the closest real column name using character overlap scoring.
        Returns None if no reasonable match found (score below threshold).

        Scoring:
          - Base score  = shared characters / max(len(unknown), len(known))
          - Bonus +0.4  if one string is a substring of the other
          - Threshold   = 0.4  (requires meaningful overlap)
        """
        unknown_lower = unknown.lower()
        best_match    = None
        best_score    = 0.0

        for col in known_columns:
            col_lower = col.lower()

            shared = sum(1 for c in unknown_lower if c in col_lower)
            score  = shared / max(len(unknown_lower), len(col_lower))

            # Bonus: substring containment
            if unknown_lower in col_lower or col_lower in unknown_lower:
                score += 0.4

            if score > best_score:
                best_score = score
                best_match = col

        return best_match if best_score >= 0.4 else None

    def _is_adf_function(self, token: str) -> bool:
        """Return True if the token is a known ADF expression function."""
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
        return token in ADF_FUNCTIONS

    # ============================================================
    # APPLY INTELLIGENT FIX
    # ============================================================
    def apply_fix(self, cause_info: dict, error_message: str, config: dict, csv_columns: list = None) -> dict:
        cause    = cause_info["cause"]
        details  = cause_info.get("details", {})
        col      = details.get("column", "unknown_column")
        pipelines = config.setdefault("pipelines", [])

        # ── INVALID TRANSFORMS ───────────────────────────────────────────
        # The planner validator dropped transforms whose expressions
        # referenced columns/tokens not present in the CSV schema.
        # Strategy:
        #   1. For each dropped transform, identify which tokens are
        #      unknown column references (not ADF functions).
        #   2. Fuzzy-match each unknown column to the closest real column.
        #   3. Substitute and keep the transform if a match is found.
        #   4. Drop the transform cleanly if no match is close enough.
        if cause == "invalid_transforms":
            dropped_list  = details.get("dropped", [])
            known_columns = self._get_known_columns(config, csv_columns=csv_columns)

            print(f"🔧 Planner dropped {len(dropped_list)} transform(s) — applying intelligent fix")
            print(f"   Known columns available: {known_columns}")

            for p in pipelines:
                if p.get("type") != "dataflow":
                    continue

                pipeline_dropped = [
                    d for d in dropped_list
                    if d.get("pipeline_name") == p["name"]
                ]
                if not pipeline_dropped:
                    continue

                existing = list(p.get("transformations", []))

                for dropped_item in pipeline_dropped:
                    bad_col     = dropped_item.get("column", "unknown_column")
                    bad_tokens  = dropped_item.get("invalid_tokens", [])
                    orig_expr   = dropped_item.get("expr", "")

                    print(f"\n   Processing dropped transform: '{bad_col} = {orig_expr}'")
                    print(f"   Invalid tokens: {bad_tokens}")

                    # Separate unknown columns from unsupported functions
                    unknown_cols = [
                        t for t in bad_tokens
                        if not self._is_adf_function(t)
                    ]
                    bad_funcs = [
                        t for t in bad_tokens
                        if self._is_adf_function(t)
                    ]

                    if bad_funcs and not unknown_cols:
                        # Unsupported function with no column fix possible
                        existing = [
                            t for t in existing
                            if not t.strip().startswith(f"{bad_col} =")
                        ]
                        print(f"   → Dropped '{bad_col}' — unsupported function(s) {bad_funcs}, no safe substitute")
                        continue

                    # ── Key logic: check if all bad_tokens are actually
                    # real CSV columns that the planner validator wrongly
                    # flagged (this happens when Groq names a new derived
                    # column the same as an existing one, or when the
                    # validator's known_columns set was incomplete).
                    # If every "invalid" token IS a real CSV column,
                    # the expression is already valid — just add it back.
                    known_lower = {c.lower() for c in known_columns}
                    all_tokens_are_real_cols = all(
                        t.lower() in known_lower
                        for t in bad_tokens
                        if not self._is_adf_function(t)
                    )

                    if all_tokens_are_real_cols and unknown_cols:
                        # Expression is fine — validator was working with
                        # incomplete column info. Add it back as-is.
                        existing = [
                            t for t in existing
                            if not t.strip().startswith(f"{bad_col} =")
                        ]
                        final_transform = f"{bad_col} = {orig_expr}"
                        existing = [final_transform] + existing
                        print(f"   ✅ All tokens are real columns — restored transform as-is: {final_transform}")
                        continue

                    # Attempt fuzzy substitution for each unknown column token
                    healed_expr = orig_expr
                    all_substituted = True

                    for unknown in unknown_cols:
                        # Skip if it's already a real column name (exact match)
                        if unknown.lower() in known_lower:
                            continue

                        closest = self._fuzzy_match_column(unknown, known_columns)
                        if closest:
                            healed_expr = re.sub(
                                rf'\b{re.escape(unknown)}\b',
                                closest,
                                healed_expr
                            )
                            print(f"   → '{unknown}' not in schema — substituted with closest real column '{closest}'")
                        else:
                            print(f"   → '{unknown}' has no close match in schema — cannot substitute")
                            all_substituted = False

                    # Remove any existing entry for this output column
                    existing = [
                        t for t in existing
                        if not t.strip().startswith(f"{bad_col} =")
                    ]

                    if all_substituted:
                        final_transform = f"{bad_col} = {healed_expr}"
                        existing = [final_transform] + existing
                        print(f"   ✅ Healed transform: {final_transform}")
                    else:
                        # Could not substitute all unknowns — drop cleanly
                        print(f"   → Dropped '{bad_col}' entirely — no close column match found in schema")

                p["transformations"]     = existing
                p["_dropped_transforms"] = []   # cleared — healed
                p["healing"]             = True

                print(f"\n   ✅ Pipeline '{p['name']}' healed. Final transforms:")
                for t in existing:
                    print(f"      {t}")

            return config

        # ── NULL / TYPE CAST ─────────────────────────────────────────────
        # Surgically fix the column that has nulls or type issues by
        # wrapping it in iifNull so ADF doesn't choke on bad rows.
        if cause in ("null_values", "type_cast_error"):
            print(f"🔧 Bad data in '{col}' → adding iifNull() safe default")
            for p in pipelines:
                if p.get("type") == "dataflow":
                    existing = p.get("transformations", [])
                    fix_expr = f"{col} = iifNull(toInteger({col}), 0)"
                    existing = [t for t in existing if not t.strip().startswith(f"{col} =")]
                    p["transformations"] = [fix_expr] + existing
                    p["healing"] = True
                    print(f"   → Surgically replaced '{col}' transform: {fix_expr}")
                    break
            return config

        # ── EXPRESSION / SCHEMA ERROR ────────────────────────────────────
        # Drop only the bad transform and keep everything else intact.
        if cause in ("expression_error", "dataflow_error"):
            print(f"🔧 Expression/schema error → dropping transform for '{col}', keeping rest")
            for p in pipelines:
                if p.get("type") == "dataflow":
                    existing = p.get("transformations", [])
                    if col != "unknown_column":
                        cleaned = [t for t in existing if not t.strip().startswith(f"{col} =")]
                        if len(cleaned) == len(existing):
                            cleaned = ["processed_time = currentTimestamp()"]
                            print("   → Could not isolate bad transform — reset to baseline")
                        else:
                            print(f"   → Dropped transform for '{col}', kept {len(cleaned)} others")
                    else:
                        cleaned = ["processed_time = currentTimestamp()"]
                        print("   → Reset to: processed_time = currentTimestamp()")
                    p["transformations"] = cleaned
                    p["healing"] = True
                    break
            return config

        # ── STORAGE AUTH ─────────────────────────────────────────────────
        # Recreate the linked service with fresh credentials.
        if cause == "storage_auth":
            print("🔧 Re-creating Linked Service with fresh credentials...")
            try:
                create_linked_service(self.token)
                print("   → Linked service recreated")
            except Exception as e:
                print(f"   ❌ Storage fix failed: {e}")
            return config

        # ── BLOB / CONTAINER MISSING ─────────────────────────────────────
        # The source container was deleted between pipeline creation and
        # trigger (e.g. manual deletion during testing, or a race condition).
        # Fix: recreate the container and re-upload the CSV if available.
        if cause == "blob_missing":
            missing_container = details.get("container", "incoming")
            print(f"🔧 Container '{missing_container}' missing → recreating and re-uploading data")
            try:
                create_blob_container(missing_container)
                print(f"   ✅ Container '{missing_container}' recreated")

                # Re-upload the CSV if we have a path stored on the config
                csv_path = config.get("_csv_path")
                if csv_path:
                    import os
                    if os.path.isfile(csv_path):
                        upload_csv(csv_path, missing_container)
                        print(f"   ✅ CSV re-uploaded to '{missing_container}'")
                    else:
                        print(f"   ⚠️  CSV path '{csv_path}' no longer exists — container recreated but empty")
                else:
                    print(f"   ⚠️  No CSV path in config — container recreated but empty")
                    print(f"       The pipeline will fail again unless data is present.")
            except Exception as e:
                print(f"   ❌ Container recreation failed: {e}")
            return config

        # ── COPY FAILURE ─────────────────────────────────────────────────
        # Fall back to a minimal single-stage copy pipeline.
        if cause == "copy_failure":
            print("🔧 Copy failed → falling back to minimal copy pipeline")
            config["pipelines"] = [{
                "name":            "Pipeline_Raw_to_Bronze",
                "type":            "copy",
                "source_dataset":  "DS_Raw",
                "sink_dataset":    "DS_Bronze",
                "merge_files":     True,
                "parallel_copies": 2,
                "diu":             2,
            }]
            config["execution_order"] = ["Pipeline_Raw_to_Bronze"]
            return config

        # ── TIMEOUT ──────────────────────────────────────────────────────
        # Boost compute so the dataflow has enough resources to finish.
        if cause == "timeout":
            print("🔧 Timeout → boosting compute resources")
            for p in pipelines:
                if p.get("type") == "dataflow":
                    p["core_count"]      = 16
                    p["partition_count"] = 16
                    p["healing"]         = True
                    print("   → Cores: 16, Partitions: 16")
            return config

        # ── UNKNOWN FALLBACK ─────────────────────────────────────────────
        print("🔧 Unknown error → safe baseline reset")
        for p in pipelines:
            if p.get("type") == "dataflow":
                p["transformations"] = ["processed_time = currentTimestamp()"]
                p["healing"]         = True
        return config