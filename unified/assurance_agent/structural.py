"""
STRUCTURAL VALIDATION layer — pure Python, deterministic, NO model.

This is the bulk of the Assurance Agent and must be 100% reliable. It never
calls an LLM and never mutates the plan (unlike the Planner's own
_structural_validate, which coerces). It only inspects and reports.

Four checks, each returns a clean pass/fail + a specific violation message:
  1. json_schema       — plan parses and matches the expected plan contract
  2. column_references — every column the plan references exists in the schema
  3. allowed_operations— every operation is in the configurable whitelist
  4. stage_ordering    — stages follow the configurable ordering rules

The "expected plan contract" (which top-level keys a plan must have) is the
intrinsic shape the rest of the orchestrator emits. The data schema, the
operation whitelist, and the ordering rules all come from external config.
"""

import json
import re

from .result import CheckResult


# Top-level keys every plan must contain, with their required python type.
PLAN_CONTRACT = {
    "containers":           dict,
    "containers_to_create": list,
    "datasets":             list,
    "stages":               list,
    "execution_order":      list,
}

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_FUNC_CALL = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(")
# split a transformation on the first single '=' that is not ==, >=, <=, !=
_ASSIGN = re.compile(r"(?<![<>=!])=(?!=)")


class StructuralValidator:
    """Runs the four deterministic checks from an external config bundle."""

    def __init__(self, allowed_ops: dict, ordering_rules: dict):
        self.allowed_ops = allowed_ops or {}
        self.ordering = ordering_rules or {}
        # tokens that are NOT column references when found inside expressions
        self.functions = {f.lower() for f in self.allowed_ops.get("sql_functions", [])}
        self.keywords = {k.lower() for k in self.allowed_ops.get("sql_keywords", [])}

    # ── public entry ─────────────────────────────────────────────────────────
    def validate(self, plan, schema: dict) -> list:
        """
        plan   : either a parsed dict, OR a raw JSON string (so the json_schema
                 check can catch malformed JSON itself).
        schema : target data schema {columns: [...], inferred_types: {...}}.
        Returns a list[CheckResult]. Short-circuits later checks if the plan
        does not even parse / match the contract (they cannot run reliably).
        """
        parsed, json_check = self._check_json_schema(plan)
        results = [json_check]
        if not json_check.passed or parsed is None:
            for name, label, tier in (
                ("column_references",  "Column references",  "schema"),
                ("allowed_operations", "Allowed operations", "structure"),
                ("stage_ordering",     "Stage ordering",     "structure"),
            ):
                results.append(CheckResult(
                    name, label, False,
                    "skipped — plan failed JSON/schema check", tier,
                ))
            return results

        results.append(self._check_column_references(parsed, schema))
        results.append(self._check_allowed_operations(parsed))
        results.append(self._check_stage_ordering(parsed))
        return results

    # ── check 1: JSON validity + plan contract ───────────────────────────────
    def _check_json_schema(self, plan):
        name, label, tier = "json_schema", "JSON & schema", "structure"
        if isinstance(plan, str):
            try:
                parsed = json.loads(plan)
            except json.JSONDecodeError as e:
                return None, CheckResult(name, label, False,
                                         f"plan is not valid JSON: {e}", tier)
        elif isinstance(plan, dict):
            parsed = plan
        else:
            return None, CheckResult(name, label, False,
                                     f"plan must be a JSON object, got {type(plan).__name__}", tier)

        missing = [k for k in PLAN_CONTRACT if k not in parsed]
        if missing:
            return parsed, CheckResult(name, label, False,
                                       f"plan missing required keys: {missing}", tier)
        wrong = [
            f"'{k}' should be {t.__name__}, got {type(parsed[k]).__name__}"
            for k, t in PLAN_CONTRACT.items() if not isinstance(parsed[k], t)
        ]
        if wrong:
            return parsed, CheckResult(name, label, False,
                                       "wrong types: " + "; ".join(wrong), tier)
        if not parsed["stages"]:
            return parsed, CheckResult(name, label, False, "plan has zero stages", tier)
        return parsed, CheckResult(name, label, True,
                                   f"valid JSON, {len(parsed['stages'])} stage(s), all required keys present", tier)

    # ── check 2: column references exist in schema ───────────────────────────
    def _refs_in_expr(self, expr) -> set:
        """Identifiers in an expression that denote column references."""
        if not expr or not isinstance(expr, str):
            return set()
        funcs_called = {m.group(1).lower() for m in _FUNC_CALL.finditer(expr)}
        cleaned = re.sub(r"'[^']*'|\"[^\"]*\"", " ", expr)   # strip string literals
        out = set()
        for m in _IDENT.finditer(cleaned):
            tok = m.group(0)
            low = tok.lower()
            if low in self.keywords or low in self.functions or low in funcs_called:
                continue
            out.add(tok)
        return out

    def _check_column_references(self, plan: dict, schema: dict):
        name, label, tier = "column_references", "Column references", "schema"
        schema_cols = set(schema.get("columns", []))
        if not schema_cols:
            return CheckResult(name, label, False,
                               "target schema defines no columns — cannot verify references", tier)

        known = set(schema_cols)   # grows as stages create new columns
        violations = []

        for s in plan.get("stages", []):
            sname = s.get("name", "?")
            refs = set()

            for t in s.get("transformations", []) or []:
                if not isinstance(t, str):
                    continue
                parts = _ASSIGN.split(t, maxsplit=1)
                if len(parts) == 2:
                    created, rhs = parts[0].strip(), parts[1]
                    refs |= self._refs_in_expr(rhs)
                    if created:
                        known.add(created)        # LHS is a new column, not a ref
                else:
                    refs |= self._refs_in_expr(t)

            refs |= self._refs_in_expr(s.get("filter_condition"))

            agg = s.get("aggregation")
            if isinstance(agg, dict):
                for g in agg.get("group_by", []) or []:
                    refs.add(g)
                for a in agg.get("aggregations", []) or []:
                    col = a.get("column")
                    if col and col != "*":
                        refs.add(col)
                    if a.get("alias"):
                        known.add(a["alias"])     # alias creates a column

            for r in sorted(refs):
                if r not in known:
                    violations.append(f"stage '{sname}' references unknown column '{r}'")

            # An aggregation collapses the frame: only the group_by columns,
            # the aggregation aliases, and the re-added processed_time survive
            # into later stages. Without this reset, a downstream stage
            # referencing a dropped column would pass validation falsely.
            if isinstance(agg, dict) and (agg.get("group_by") or agg.get("aggregations")):
                survivors = set(agg.get("group_by") or [])
                survivors |= {
                    a["alias"] for a in (agg.get("aggregations") or [])
                    if isinstance(a, dict) and a.get("alias")
                }
                survivors.add("processed_time")
                known = survivors

        if violations:
            return CheckResult(name, label, False, "; ".join(violations), tier)
        return CheckResult(name, label, True,
                           f"all referenced columns exist in schema ({len(schema_cols)} cols)", tier)

    # ── check 3: allowed operations whitelist ────────────────────────────────
    def _check_allowed_operations(self, plan: dict):
        name, label, tier = "allowed_operations", "Allowed operations", "structure"
        allowed_types = set(self.allowed_ops.get("stage_types", []))
        allowed_aggs = set(self.allowed_ops.get("aggregation_ops", []))
        violations = []

        for s in plan.get("stages", []):
            sname = s.get("name", "?")
            stype = s.get("type")
            if stype not in allowed_types:
                violations.append(
                    f"stage '{sname}' uses type '{stype}' not in whitelist {sorted(allowed_types)}")
            agg = s.get("aggregation")
            if isinstance(agg, dict):
                for a in agg.get("aggregations", []) or []:
                    op = str(a.get("op", "")).lower()
                    if op not in allowed_aggs:
                        violations.append(
                            f"stage '{sname}' uses aggregation op '{op}' not in whitelist {sorted(allowed_aggs)}")

        if violations:
            return CheckResult(name, label, False, "; ".join(violations), tier)
        return CheckResult(name, label, True, "all operations are in the allowed whitelist", tier)

    # ── check 4: stage ordering rules ────────────────────────────────────────
    def _check_stage_ordering(self, plan: dict):
        name, label, tier = "stage_ordering", "Stage ordering", "structure"
        stages = plan.get("stages", [])
        types_cfg = self.ordering.get("stage_types", {})
        violations = []

        # 4a. every stage type is known to the ordering ruleset
        for s in stages:
            if s.get("type") not in types_cfg:
                violations.append(
                    f"stage '{s.get('name','?')}' has type '{s.get('type')}' with no ordering rank defined")
        if violations:
            return CheckResult(name, label, False, "; ".join(violations), tier)

        # 4b. first stage must be the configured ingest type (no transform before load)
        first_type = self.ordering.get("first_stage_type")
        if first_type and stages and stages[0].get("type") != first_type:
            violations.append(
                f"first stage must be '{first_type}' (load before transform), got '{stages[0].get('type')}'")

        # 4c. only the first stage may be the ingest/copy type, if configured
        if self.ordering.get("single_copy_only") and first_type:
            for i, s in enumerate(stages):
                if i > 0 and s.get("type") == first_type:
                    violations.append(
                        f"stage '{s.get('name','?')}' (index {i}) is '{first_type}', only the first stage may be")

        # 4d. ranks must be non-decreasing (no write-before-read style inversion)
        if self.ordering.get("require_non_decreasing_rank", True):
            prev_rank, prev_name = None, None
            for s in stages:
                rank = types_cfg.get(s.get("type"), {}).get("rank", 0)
                if prev_rank is not None and rank < prev_rank:
                    violations.append(
                        f"stage '{s.get('name','?')}' (type '{s.get('type')}') comes after "
                        f"higher-rank stage '{prev_name}' — ordering inverted")
                prev_rank, prev_name = rank, s.get("name", "?")

        # 4e. execution_order must match the stage list exactly
        if self.ordering.get("require_execution_order_match", True):
            stage_names = [s.get("name") for s in stages]
            exec_order = plan.get("execution_order", [])
            if exec_order != stage_names:
                violations.append(
                    f"execution_order {exec_order} does not match stage sequence {stage_names}")

        # 4f. execution_groups (optional concurrency plan): must partition the
        # stages, and no stage may run in the same group as — or before — a
        # stage that produces its source container.
        groups = plan.get("execution_groups")
        if isinstance(groups, list) and groups:
            flat = [n for g in groups if isinstance(g, list) for n in g]
            stage_names = [s.get("name") for s in stages]
            if sorted(flat) != sorted(stage_names):
                violations.append(
                    f"execution_groups {flat} do not partition the stage list {stage_names}")
            else:
                def _norm(tok) -> str:
                    t = str(tok or "").lower().strip()
                    if t.startswith("ds_"):
                        t = t[3:]
                    return t.replace("-", "").replace("_", "")

                by_name = {s.get("name"): s for s in stages}
                sinks = {}
                for s in stages:
                    tok = _norm(s.get("sink_container") or s.get("sink_dataset"))
                    if tok:
                        sinks[tok] = s.get("name")

                done = set()   # stages completed in earlier groups
                for g in groups:
                    for n in g:
                        src = _norm(by_name[n].get("source_container")
                                    or by_name[n].get("source_dataset"))
                        upstream = sinks.get(src)
                        if upstream and upstream != n and upstream not in done:
                            violations.append(
                                f"stage '{n}' is scheduled with or before its dependency "
                                f"'{upstream}' — they cannot run concurrently")
                    done.update(g)

        if violations:
            return CheckResult(name, label, False, "; ".join(violations), tier)
        return CheckResult(name, label, True, f"{len(stages)} stage(s) in valid order", tier)
