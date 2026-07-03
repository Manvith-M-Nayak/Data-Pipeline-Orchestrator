"""
Central Manager Agent — deterministic orchestrator that sits between
the Planner and all specialized agents.

Responsibilities:
  1. Validate the Planner's output (completeness, coherence)
  2. Run pre-execution checks (resource prediction, cost estimate, parallelism analysis)
  3. Invoke the Executor with retry + backoff policy
  4. Run post-execution assurance checks
  5. Record outcomes to a feedback log for future plan refinement
  6. Maintain a complete audit trail of every decision

Deliberately kept as pure orchestration code — no LLM calls here.
The Planner is the "brain"; the Manager is the "nervous system".
"""

import asyncio
import datetime
import json
import os
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, List, Any


# ── Required plan keys (what the Planner must produce) ──────────────────────
REQUIRED_PLAN_KEYS = {
    "stages",
    "execution_order",
    "containers_to_create",
    "recommended_settings",
    "num_containers",
}

# Valid stage types the executor supports
KNOWN_STAGE_TYPES = {"copy", "notebook"}

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def _utcnow() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ── RunState — single source of truth for one managed run ───────────────────
@dataclass
class RunState:
    run_id: str
    status: str = "pending"
    # pending | validating | assuring_plan | pre_checks | executing | assurance | feedback | completed | failed
    phase: str = "init"
    step: str = "Queued"
    plan: dict = field(default_factory=dict)
    user_request: str = ""   # original Planner prompt — drives the semantic assurance layer
    decisions: List[Dict[str, Any]] = field(default_factory=list)
    retries: int = 0
    started_at: str = ""
    completed_at: Optional[str] = None
    error: Optional[str] = None
    # Independent plan verification (Assurance Agent — structural + semantic)
    plan_assurance: dict = field(default_factory=dict)
    # Pre-check outputs
    validation: dict = field(default_factory=dict)
    predictions: dict = field(default_factory=dict)
    resource_plan: dict = field(default_factory=dict)
    cost_estimate: dict = field(default_factory=dict)
    parallelism: dict = field(default_factory=dict)
    # Execution outputs
    executor_result: Optional[dict] = None
    # Post-execution outputs
    assurance: dict = field(default_factory=dict)
    performance_prediction: dict = field(default_factory=dict)


# ── CentralManager ───────────────────────────────────────────────────────────
class CentralManager:
    MAX_RETRIES = 2
    RETRY_BACKOFF_S = [10, 30]   # seconds to wait before retry 1, retry 2

    def __init__(self):
        self._runs: Dict[str, RunState] = {}

    # ── Logging ──────────────────────────────────────────────────────────────
    def _log(
        self,
        state: RunState,
        action: str,
        reason: str,
        outcome: str = "",
        severity: str = "info",
    ):
        entry = {
            "ts":       _utcnow(),
            "phase":    state.phase,
            "action":   action,
            "reason":   reason,
            "outcome":  outcome,
            "severity": severity,
        }
        state.decisions.append(entry)
        tag = {"info": "ℹ", "warn": "⚠", "error": "✖", "ok": "✔"}.get(severity, "·")
        print(f"[Manager {state.run_id[:8]}] {tag} [{state.phase}] {action}: {reason} → {outcome}")

    def _enter(self, state: RunState, phase: str, step: str):
        state.phase = phase
        state.step = step
        self._log(state, f"PHASE:{phase.upper()}", step, "started")

    # ────────────────────────────────────────────────────────────────────────
    # Phase 1 — Plan validation
    # ────────────────────────────────────────────────────────────────────────
    def validate_plan(self, state: RunState) -> dict:
        self._enter(state, "validating", "Validating plan from Planner Agent")
        plan = state.plan
        issues: List[str] = []
        warnings: List[str] = []

        # Required top-level keys
        missing = REQUIRED_PLAN_KEYS - set(plan.keys())
        if missing:
            issues.append(f"Missing keys: {', '.join(sorted(missing))}")

        stages = plan.get("stages", [])
        execution_order = plan.get("execution_order", [])
        stage_names = {s.get("name") for s in stages if s.get("name")}

        # execution_order references only known stages
        for name in execution_order:
            if name not in stage_names:
                issues.append(f"execution_order references unknown stage '{name}'")

        # Every stage has a name and type; field requirements differ by type
        for i, s in enumerate(stages):
            if not s.get("name"):
                issues.append(f"Stage[{i}] has no 'name'")
            stype = s.get("type")
            if stype not in KNOWN_STAGE_TYPES:
                warnings.append(
                    f"Stage '{s.get('name', i)}' has unknown type '{stype}'"
                )
            # copy stages use dataset refs (source_dataset / sink_dataset)
            # notebook stages use container refs (source_container / sink_container)
            if stype == "notebook":
                if not s.get("source_container"):
                    issues.append(f"Stage '{s.get('name', i)}' (notebook) missing 'source_container'")
                if not s.get("sink_container"):
                    issues.append(f"Stage '{s.get('name', i)}' (notebook) missing 'sink_container'")
            elif stype == "copy":
                if not s.get("source_dataset") and not s.get("source_container"):
                    warnings.append(f"Stage '{s.get('name', i)}' (copy) missing 'source_dataset'")

        if not stages:
            issues.append("Plan has no stages")

        if not plan.get("containers_to_create"):
            issues.append("containers_to_create is empty")

        # recommended_settings sanity
        rec = plan.get("recommended_settings", {})
        if not rec:
            warnings.append("recommended_settings is empty — will use defaults")

        # Fallback flag
        if plan.get("used_fallback"):
            warnings.append(
                "Planner used fallback config (Groq unavailable or parse failed) — "
                "validate settings before production"
            )

        # Derive execution_order from stages if absent
        if stages and not execution_order:
            warnings.append("execution_order empty; derived from stages list order")
            plan["execution_order"] = [s["name"] for s in stages]

        ok = len(issues) == 0
        result = {"ok": ok, "issues": issues, "warnings": warnings}
        state.validation = result

        if ok:
            self._log(
                state, "VALIDATE", f"{len(stages)} stages, {len(warnings)} warnings",
                f"ok — {', '.join(warnings[:2]) or 'clean'}", "ok",
            )
        else:
            self._log(
                state, "VALIDATE FAIL", "; ".join(issues),
                "aborting run", "error",
            )
        return result

    # ────────────────────────────────────────────────────────────────────────
    # Phase 1.5 — Independent plan assurance (Assurance Agent)
    #   Structural layer (deterministic) is an authoritative gate: a failure
    #   aborts the run. Semantic layer (base LLM) is advisory — it warns about
    #   intent mismatches but never aborts. Independent of the Planner: shares
    #   no generation logic and uses clean base weights (no LoRA adapter).
    # ────────────────────────────────────────────────────────────────────────
    async def run_plan_assurance(self, state: RunState, schema: dict) -> dict:
        from assurance_agent import AssuranceAgent
        from fastapi.concurrency import run_in_threadpool

        self._enter(state, "assuring_plan", "Verifying plan with Assurance Agent")

        # Semantic layer only runs if we have the original request to compare against.
        run_semantic = bool(state.user_request)
        try:
            agent = AssuranceAgent()
            result = await run_in_threadpool(
                agent.assure, state.user_request, state.plan, schema, run_semantic
            )
            d = result.to_dict()
        except Exception as exc:
            # Never let a verifier crash the orchestrator — fail open with a warning.
            self._log(state, "ASSURE ERROR", str(exc)[:200],
                      "skipping assurance (fail-open)", "warn")
            d = {"overall_status": "pass", "structural_results": [],
                 "semantic_result": None, "summary": "assurance skipped (error)",
                 "tiers": {"failure": None}}
            state.plan_assurance = d
            return d

        state.plan_assurance = d

        for c in d["structural_results"]:
            self._log(
                state, f"ASSURE: {c['label']}", c["message"],
                "pass" if c["passed"] else "FAIL",
                "ok" if c["passed"] else "error",
            )

        sem = d.get("semantic_result")
        if sem and sem.get("available"):
            if sem.get("flagged"):
                self._log(state, "ASSURE: Intent match", sem["reasoning"],
                          "flagged — advisory, human may override", "warn")
            else:
                self._log(state, "ASSURE: Intent match", "plan matches request",
                          "ok", "ok")

        self._log(
            state, "ASSURANCE COMPLETE", d["summary"], d["overall_status"],
            "ok" if d["overall_status"] == "pass" else "error",
        )
        return d

    # ────────────────────────────────────────────────────────────────────────
    # Phase 2a — Resource prediction (via Resource Agent)
    # ────────────────────────────────────────────────────────────────────────
    def predict_resources(
        self, state: RunState, csv_size_bytes: int, schema: dict = None
    ) -> dict:
        from resource_agent.resource_agent import ResourceAgent
        plan   = state.plan
        stages = plan.get("stages", [])
        mb     = csv_size_bytes / (1024 * 1024) if csv_size_bytes else 0.0

        # Use execution groups already computed by parallelism analysis (if done)
        exec_groups = state.parallelism.get("execution_groups") or None

        rp = ResourceAgent().analyze(
            plan=plan,
            csv_size_bytes=csv_size_bytes,
            schema=schema,
            execution_groups=exec_groups,
        )
        state.resource_plan = rp

        # Surface top-level summary into state.predictions for backward compat
        copy_count     = sum(1 for s in stages if s.get("type") == "copy")
        notebook_count = sum(1 for s in stages if s.get("type") == "notebook")
        rec            = plan.get("recommended_settings", {})

        complexity = "low"
        if mb > 100 or len(stages) > 5:
            complexity = "high"
        elif mb > 10 or len(stages) > 2:
            complexity = "medium"

        result = {
            "file_size_mb":         round(mb, 6),
            "stage_count":          len(stages),
            "copy_stages":          copy_count,
            "notebook_stages":      notebook_count,
            "complexity":           complexity,
            "suggested_workers":    rp.get("peak_concurrent_workers", 0),
            "estimated_duration_s": rp.get("estimated_total_s", 0),
            "node_type":            rec.get("node_type", "Standard_D4s_v3"),
            "shuffle_partitions":   rec.get("shuffle_partitions", 8),
            "total_memory_gb":      rp.get("total_memory_gb", 0),
            "feasible":             rp.get("feasible", True),
            "correction_factors":   rp.get("correction_factors", {}),
        }
        state.predictions = result

        feasible  = rp.get("feasible", True)
        violations = rp.get("constraint_violations", [])
        warnings   = rp.get("warnings", [])

        if not feasible:
            self._log(
                state, "RESOURCE PREDICT — INFEASIBLE",
                "; ".join(violations),
                "aborting: plan exceeds hard resource limits",
                "error",
            )
        else:
            warn_str = f" · {len(warnings)} warning(s)" if warnings else ""
            self._log(
                state, "RESOURCE PREDICT",
                f"{mb:.1f} MB · {len(stages)} stages · complexity={complexity}",
                f"~{rp.get('estimated_total_s', 0)}s · "
                f"{rp.get('peak_concurrent_workers', 0)} peak workers{warn_str}",
                "ok" if not warnings else "warn",
            )
        return result

    # ────────────────────────────────────────────────────────────────────────
    # Phase 2b — Cost estimation (Azure, student tier)
    # ────────────────────────────────────────────────────────────────────────
    def estimate_cost(self, state: RunState, predictions: dict) -> dict:
        plan = state.plan
        stages = plan.get("stages", [])
        duration_s = predictions["estimated_duration_s"]
        workers = max(predictions["suggested_workers"], 1)

        copy_count     = predictions["copy_stages"]
        notebook_count = predictions["notebook_stages"]

        # ADF: $0.001 per activity run (copy = 1 activity)
        adf_usd = copy_count * 0.001

        # Databricks serverless: ~0.07 DBU/s per notebook at min scale
        dbu_per_s = 0.07 * workers
        dbx_usd = notebook_count * duration_s * dbu_per_s * 0.00025

        # Azure Blob Storage: negligible for student volumes
        storage_usd = (predictions["file_size_mb"] / 1024) * 0.018  # $0.018/GB

        total = round(adf_usd + dbx_usd + storage_usd, 5)
        budget_ok = total < 1.0

        result = {
            "adf_activity_usd":     round(adf_usd, 5),
            "databricks_usd":       round(dbx_usd, 5),
            "storage_usd":          round(storage_usd, 5),
            "total_usd":            total,
            "budget_ok":            budget_ok,
            "currency":             "USD",
        }
        state.cost_estimate = result

        level = "ok" if budget_ok else "warn"
        self._log(
            state, "COST ESTIMATE",
            f"ADF ${adf_usd:.4f} + DBX ${dbx_usd:.4f} + Storage ${storage_usd:.5f}",
            f"total ${total:.4f} — {'within budget' if budget_ok else 'over $1 threshold'}",
            level,
        )
        if not budget_ok:
            self._log(
                state, "COST WARN",
                f"Estimated ${total:.4f} > $1.00 threshold",
                "proceeding — student tier limits apply",
                "warn",
            )
        return result
# PASTE THIS METHOD:
 
    # ────────────────────────────────────────────────────────────────────────
    # Phase 2b.5 — Performance prediction (via Performance Prediction Agent)
    # ────────────────────────────────────────────────────────────────────────
    def predict_performance(
        self,
        state: "RunState",
        sla_target_s: int = 900,
    ) -> dict:
        """
        Calls the PerformancePredictionAgent with the already-populated
        state.resource_plan and state.predictions (set by predict_resources).
 
        Must be called AFTER predict_resources() in Phase 2.
 
        Stores result on state.performance_prediction and logs a summary.
        Returns the prediction dict.
        """
        from performance_prediction_agent.performance_agent import PerformancePredictionAgent
 
        result = PerformancePredictionAgent().predict(
            resource_plan=state.resource_plan,
            predictions=state.predictions,
            plan=state.plan,
            sla_target_s=sla_target_s,
        )
        state.performance_prediction = result
 
        outcome    = result.get("outcome", "unknown")
        total_s    = result.get("predicted_total_s", 0)
        bottleneck = result.get("bottleneck_stage", "?")
        confidence = result.get("confidence", 0.0)
        sla_risk   = result.get("sla_breach_risk", False)
 
        level = "ok"
        if outcome == "failure":
            level = "error"
        elif outcome == "slowdown" or sla_risk:
            level = "warn"
 
        self._log(
            state, "PERF PREDICT",
            f"outcome={outcome} · total={total_s}s · bottleneck='{bottleneck}' · "
            f"confidence={confidence:.0%}{'  ⚠ SLA BREACH RISK' if sla_risk else ''}",
            f"history_runs_used={result.get('history_runs_used', 0)} · "
            f"adj_factor={result.get('adjustment_factor', 1.0):.3f}",
            level,
        )
 
        if outcome == "failure":
            self._log(
                state, "PERF PREDICT — ABORT",
                f"Performance prediction classified outcome as FAILURE "
                f"(confidence {confidence:.0%}). Stopping run early.",
                "aborting — predicted failure before execution",
                "error",
            )
 
        return result
 
 
    # ────────────────────────────────────────────────────────────────────────
    # Phase 2c — Parallelism analysis
    # ────────────────────────────────────────────────────────────────────────
    def analyze_parallelism(self, state: RunState) -> dict:
        stages = state.plan.get("stages", [])

        # Build dependency graph: stage B depends on stage A if A.sink == B.source.
        # copy stages use dataset refs (DS_Bronze); notebook stages use container refs (bronze).
        # Normalize both to a lowercase bare name: "DS_Bronze" → "bronze", "bronze" → "bronze".
        def _norm(token: str) -> str:
            t = token.lower().strip()
            if t.startswith("ds_"):
                t = t[3:]
            return t.replace("-", "_")

        def _sink_token(s: dict) -> str:
            raw = s.get("sink_container") or s.get("sink_dataset", "")
            return _norm(raw) if raw else ""

        def _src_token(s: dict) -> str:
            raw = s.get("source_container") or s.get("source_dataset", "")
            return _norm(raw) if raw else ""

        sinks: Dict[str, str] = {}       # token → stage_name that produces it
        deps:  Dict[str, List[str]] = {}  # stage_name → [stage_names it depends on]
        groups: List[List[str]] = []      # ordered parallel groups

        for s in stages:
            token = _sink_token(s)
            if token:
                sinks[token] = s["name"]
            deps[s["name"]] = []

        for s in stages:
            src = _src_token(s)
            if src and src in sinks:
                deps[s["name"]].append(sinks[src])

        # Topological sort into parallel groups
        remaining = list(state.plan.get("execution_order", [s["name"] for s in stages]))
        resolved: set = set()
        while remaining:
            group = [n for n in remaining if all(d in resolved for d in deps.get(n, []))]
            if not group:
                group = [remaining[0]]   # break cycle
            groups.append(group)
            for n in group:
                resolved.add(n)
                remaining.remove(n)

        parallel_count = sum(1 for g in groups if len(g) > 1)
        sequential_count = len(groups) - parallel_count

        result = {
            "execution_groups": groups,
            "parallel_groups":  parallel_count,
            "sequential_groups": sequential_count,
            "can_parallelize":  parallel_count > 0,
        }
        state.parallelism = result

        if parallel_count > 0:
            self._log(
                state, "PARALLELISM",
                f"{parallel_count} parallel group(s) detected",
                f"groups: {groups}",
                "ok",
            )
        else:
            self._log(
                state, "PARALLELISM",
                "All stages sequential (each depends on previous output)",
                f"{len(groups)} sequential groups",
                "info",
            )
        return result

    # ────────────────────────────────────────────────────────────────────────
    # Phase 3 — Execute with retry + backoff
    # ────────────────────────────────────────────────────────────────────────
    async def execute_with_retry(
        self,
        state: RunState,
        csv_path: str,
        config: dict,
        schema: dict,
    ) -> dict:
        from executor_agent.executor import execute_pipeline
        from fastapi.concurrency import run_in_threadpool

        last_error = ""

        for attempt in range(self.MAX_RETRIES + 1):
            if attempt > 0:
                backoff = self.RETRY_BACKOFF_S[min(attempt - 1, len(self.RETRY_BACKOFF_S) - 1)]
                self._log(
                    state, f"RETRY {attempt}/{self.MAX_RETRIES}",
                    f"previous attempt failed: {last_error[:120]}",
                    f"waiting {backoff}s before retry",
                    "warn",
                )
                state.step = f"Retry {attempt}/{self.MAX_RETRIES} — waiting {backoff}s…"
                state.retries = attempt
                await asyncio.sleep(backoff)

            self._log(
                state, f"EXECUTE attempt {attempt + 1}",
                f"invoking Executor Agent (csv={os.path.basename(csv_path)})",
                "dispatched",
            )
            state.step = (
                f"Executing pipeline — attempt {attempt + 1}"
                if attempt == 0
                else f"Retry {attempt}/{self.MAX_RETRIES} — executing"
            )

            def _progress(msg: str, dbx_run_id=None):
                state.step = msg
                # Surface Databricks run_id in the log
                if dbx_run_id is not None:
                    self._log(state, "DBX RUN", f"run_id={dbx_run_id}", msg, "info")

            try:
                result = await run_in_threadpool(
                    execute_pipeline, csv_path, config, schema, _progress
                )

                if isinstance(result, dict) and result.get("status") == "ok":
                    self._log(
                        state, f"EXECUTE attempt {attempt + 1} OK",
                        f"stages: {result.get('stages', [])}",
                        "success",
                        "ok",
                    )
                    return result

                # Executor returned a failure dict (not an exception)
                last_error = (
                    result.get("message", "Unknown failure")
                    if isinstance(result, dict)
                    else str(result)
                )
                self._log(
                    state, f"EXECUTE attempt {attempt + 1} FAILED",
                    last_error[:200],
                    "retry" if attempt < self.MAX_RETRIES else "abort",
                    "warn" if attempt < self.MAX_RETRIES else "error",
                )

            except Exception as exc:
                last_error = str(exc)
                self._log(
                    state, f"EXECUTE attempt {attempt + 1} EXCEPTION",
                    last_error[:200],
                    "retry" if attempt < self.MAX_RETRIES else "abort",
                    "warn" if attempt < self.MAX_RETRIES else "error",
                )

        raise RuntimeError(
            f"Pipeline failed after {self.MAX_RETRIES + 1} attempt(s): {last_error}"
        )

    # ────────────────────────────────────────────────────────────────────────
    # Phase 4 — Post-execution assurance
    # ────────────────────────────────────────────────────────────────────────
    async def run_assurance(
        self,
        state: RunState,
        result: dict,
        actual_duration_s: float,
    ) -> dict:
        self._enter(state, "assurance", "Running post-execution assurance checks")
        checks: Dict[str, Any] = {}
        predicted_s = state.predictions.get("estimated_duration_s", 0)

        # Check 1 — timing
        if predicted_s > 0:
            ratio = actual_duration_s / predicted_s
            checks["timing_ratio"]    = round(ratio, 2)
            checks["timing_ok"]       = ratio < 4.0
            checks["actual_duration_s"] = round(actual_duration_s, 1)
            checks["predicted_duration_s"] = predicted_s
            level = "ok" if checks["timing_ok"] else "warn"
            self._log(
                state, "ASSURANCE TIMING",
                f"actual {actual_duration_s:.0f}s vs predicted {predicted_s}s (×{ratio:.1f})",
                "ok" if checks["timing_ok"] else "slow — exceeds 4× prediction",
                level,
            )

        # Check 2 — stage completion
        stages_expected = len(state.plan.get("stages", []))
        stages_ran = len(result.get("stages", []))
        checks["stages_expected"] = stages_expected
        checks["stages_completed"] = stages_ran
        checks["all_stages_completed"] = stages_ran >= stages_expected
        self._log(
            state, "ASSURANCE STAGES",
            f"{stages_ran}/{stages_expected} stages completed",
            "ok" if checks["all_stages_completed"] else "incomplete — some stages skipped",
            "ok" if checks["all_stages_completed"] else "warn",
        )

        # Check 3 — output exists
        checks["has_output"] = bool(result.get("sink_container"))
        self._log(
            state, "ASSURANCE OUTPUT",
            f"sink_container={result.get('sink_container', 'none')}",
            "output present" if checks["has_output"] else "no output container",
            "ok" if checks["has_output"] else "warn",
        )

        # Check 4 — retry count
        checks["retries_used"] = state.retries
        checks["retry_ok"] = state.retries <= 1
        if state.retries > 0:
            self._log(
                state, "ASSURANCE RETRIES",
                f"{state.retries} retry/retries needed",
                "succeeded after retries" if state.status != "failed" else "failed",
                "warn",
            )

        checks["passed"] = (
            checks.get("all_stages_completed", False)
            and checks.get("has_output", False)
            and checks.get("timing_ok", True)
        )
        state.assurance = checks

        overall = "ok" if checks["passed"] else "warn"
        self._log(
            state, "ASSURANCE COMPLETE",
            f"passed={checks['passed']}",
            ", ".join(f"{k}={v}" for k, v in checks.items() if isinstance(v, bool)),
            overall,
        )
        return checks

    # ────────────────────────────────────────────────────────────────────────
    # Phase 5 — Feedback / learning loop record
    # ────────────────────────────────────────────────────────────────────────
    async def record_feedback(self, state: RunState, actual_duration_s: float):
        self._enter(state, "feedback", "Recording outcome to feedback log")
        try:
            os.makedirs(_DATA_DIR, exist_ok=True)
            log_path = os.path.join(_DATA_DIR, "manager_feedback.jsonl")
            record = {
                "ts":                  _utcnow(),
                "run_id":              state.run_id,
                "final_status":        state.status,
                "stage_count":         len(state.plan.get("stages", [])),
                "retries":             state.retries,
                "actual_duration_s":   round(actual_duration_s, 1),
                "predicted_duration_s": state.predictions.get("estimated_duration_s"),
                "cost_estimate_usd":   state.cost_estimate.get("total_usd"),
                "assurance_passed":    state.assurance.get("passed"),
                "plan_assurance_passed": state.plan_assurance.get("overall_status") == "pass" if state.plan_assurance else None,
                "used_fallback":       state.plan.get("used_fallback", False),
                "complexity":          state.predictions.get("complexity"),
                "validation_issues":   state.validation.get("issues", []),
            }
            with open(log_path, "a") as f:
                f.write(json.dumps(record) + "\n")

            # Resource Agent self-correction: record actual vs predicted per stage type
            self._record_resource_feedback(state, actual_duration_s)

            self._log(state, "FEEDBACK RECORDED", "→ data/manager_feedback.jsonl", "ok", "ok")
        except Exception as exc:
            self._log(state, "FEEDBACK WARN", str(exc), "non-fatal", "warn")

    def _record_resource_feedback(self, state: RunState, actual_duration_s: float):
        """Apportion actual duration proportionally across stage types for Resource Agent learning."""
        try:
            from resource_agent.resource_agent import ResourceAgent
            rp = state.resource_plan
            if not rp:
                return
            allocs = rp.get("allocations", [])
            total_predicted = sum(a.get("duration_s", 0) for a in allocs) or 1
            agent = ResourceAgent()
            for alloc in allocs:
                pred_s = alloc.get("duration_s", 0)
                if pred_s <= 0:
                    continue
                # Proportional actual: each stage gets (pred_s / total_pred) * actual_total
                actual_s = actual_duration_s * (pred_s / total_predicted)
                agent.record_actual(
                    stage_name=alloc.get("stage_name", ""),
                    stage_type=alloc.get("stage_type", "notebook"),
                    predicted_duration_s=float(pred_s),
                    actual_duration_s=actual_s,
                    predicted_workers=int(alloc.get("workers", 0)),
                    actual_workers=int(alloc.get("workers", 0)),
                    run_id=state.run_id,
                )
        except Exception as exc:
            print(f"[Manager] resource feedback non-fatal: {exc}")

    # ────────────────────────────────────────────────────────────────────────
    # Public API
    # ────────────────────────────────────────────────────────────────────────
    def pre_create(self, plan: dict) -> str:
        """Create a RunState before async execution starts. Returns run_id."""
        run_id = str(uuid.uuid4())
        state = RunState(
            run_id=run_id,
            plan=plan,
            started_at=_utcnow(),
        )
        self._runs[run_id] = state
        return run_id

    async def execute_run(
        self,
        run_id: str,
        csv_path: str,
        schema: dict,
        csv_size: int,
        user_request: str = "",
    ):
        """Main orchestration entry point — drives the complete run lifecycle."""
        state = self._runs[run_id]
        state.user_request = user_request or state.user_request
        t0 = time.time()

        try:
            # ── Phase 1: Validate ────────────────────────────────────────
            state.status = "validating"
            v = self.validate_plan(state)
            if not v["ok"]:
                state.status = "failed"
                state.error = "Plan validation failed: " + "; ".join(v["issues"])
                state.step = "Failed: invalid plan"
                state.completed_at = _utcnow()
                await self.record_feedback(state, time.time() - t0)
                return

            # ── Phase 1.5: Plan assurance (independent verifier) ─────────
            state.status = "assuring_plan"
            pa = await self.run_plan_assurance(state, schema)
            if pa.get("overall_status") != "pass":
                failing = [c["label"] for c in pa.get("structural_results", []) if not c["passed"]]
                state.status = "failed"
                state.error = (
                    "Assurance Agent rejected plan — "
                    + (pa.get("tiers", {}).get("failure") or ("failed: " + ", ".join(failing)))
                )
                state.step = "Failed: plan failed assurance"
                state.completed_at = _utcnow()
                await self.record_feedback(state, time.time() - t0)
                return

            # ── Phase 2: Pre-checks ──────────────────────────────────────
            state.status = "pre_checks"
            self._enter(state, "pre_checks", "Running resource prediction and cost estimation")
            # Parallelism first — Resource Agent uses exec groups for contention detection
            self.analyze_parallelism(state)
            self.predict_resources(state, csv_size, schema)

            # Abort on hard resource-constraint violation BEFORE cost/perf —
            # an infeasible plan yields incomplete estimates that would crash
            # the downstream predictors.
            if not state.predictions.get("feasible", True):
                violations = state.resource_plan.get("constraint_violations", [])
                state.status = "failed"
                state.error = "Resource constraints violated: " + "; ".join(violations)
                state.step = "Failed: resource limits exceeded"
                state.completed_at = _utcnow()
                await self.record_feedback(state, time.time() - t0)
                return

            self.estimate_cost(state, state.predictions)
            perf = self.predict_performance(state)

            # Abort if Performance Agent predicts certain failure
            if perf.get("outcome") == "failure":
                state.status = "failed"
                state.error = (
                    f"Performance prediction: outcome=failure "
                    f"(confidence {perf.get('confidence', 0):.0%}). "
                    f"Bottleneck: '{perf.get('bottleneck_stage', '?')}'. "
                    f"Rationale: {perf.get('rationale', '')[:200]}"
                )
                state.step = "Failed: performance prediction aborted run"
                state.completed_at = _utcnow()
                await self.record_feedback(state, time.time() - t0)
                return
            # ── Phase 3: Execute ─────────────────────────────────────────
            state.status = "executing"
            self._enter(state, "executing", "Handing off to Executor Agent")
            result = await self.execute_with_retry(state, csv_path, state.plan, schema)

            # ── Phase 4: Assurance ───────────────────────────────────────
            state.status = "assurance"
            await self.run_assurance(state, result, time.time() - t0)

            # ── Phase 5: Feedback ────────────────────────────────────────
            state.status = "feedback"
            await self.record_feedback(state, time.time() - t0)

            # ── Done ─────────────────────────────────────────────────────
            state.status = "completed"
            state.executor_result = result
            state.step = "Complete"
            state.completed_at = _utcnow()
            self._log(
                state, "RUN COMPLETE",
                f"total={time.time()-t0:.0f}s retries={state.retries}",
                "success",
                "ok",
            )

        except Exception as exc:
            state.status = "failed"
            state.error = str(exc)
            state.step = f"Failed: {str(exc)[:120]}"
            state.completed_at = _utcnow()
            self._log(state, "RUN FAILED", str(exc)[:300], "abort", "error")
            try:
                await self.record_feedback(state, time.time() - t0)
            except Exception:
                pass

    def get_state_dict(self, run_id: str) -> Optional[dict]:
        state = self._runs.get(run_id)
        if state is None:
            return None
        d = asdict(state)
        # Add derived fields the UI can use without recomputing
        d["plan_summary"] = {
            "stage_count":     len(state.plan.get("stages", [])),
            "stages":          [s.get("name") for s in state.plan.get("stages", [])],
            "execution_order": state.plan.get("execution_order", []),
        }
        return d

    def list_runs(self) -> list:
        out = []
        for r in self._runs.values():
            out.append({
                "run_id":       r.run_id,
                "status":       r.status,
                "phase":        r.phase,
                "step":         r.step,
                "started_at":   r.started_at,
                "completed_at": r.completed_at,
                "retries":      r.retries,
                "stage_count":  len(r.plan.get("stages", [])),
            })
        return sorted(out, key=lambda x: x["started_at"], reverse=True)

    def get_feedback_history(self) -> list:
        log_path = os.path.join(_DATA_DIR, "manager_feedback.jsonl")
        if not os.path.exists(log_path):
            return []
        records = []
        try:
            with open(log_path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
        except Exception:
            pass
        return records[-50:]  # last 50
