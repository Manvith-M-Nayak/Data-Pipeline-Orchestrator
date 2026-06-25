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
    # pending | validating | pre_checks | executing | assurance | feedback | completed | failed
    phase: str = "init"
    step: str = "Queued"
    plan: dict = field(default_factory=dict)
    decisions: List[Dict[str, Any]] = field(default_factory=list)
    retries: int = 0
    started_at: str = ""
    completed_at: Optional[str] = None
    error: Optional[str] = None
    # Pre-check outputs
    validation: dict = field(default_factory=dict)
    predictions: dict = field(default_factory=dict)
    cost_estimate: dict = field(default_factory=dict)
    parallelism: dict = field(default_factory=dict)
    # Execution outputs
    executor_result: Optional[dict] = None
    # Post-execution outputs
    assurance: dict = field(default_factory=dict)


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
    # Phase 2a — Resource prediction
    # ────────────────────────────────────────────────────────────────────────
    def predict_resources(self, state: RunState, csv_size_bytes: int) -> dict:
        plan = state.plan
        stages = plan.get("stages", [])
        rec = plan.get("recommended_settings", {})
        mb = csv_size_bytes / (1024 * 1024) if csv_size_bytes else 0.0

        copy_count     = sum(1 for s in stages if s.get("type") == "copy")
        notebook_count = sum(1 for s in stages if s.get("type") == "notebook")

        complexity = "low"
        if mb > 100 or len(stages) > 5:
            complexity = "high"
        elif mb > 10 or len(stages) > 2:
            complexity = "medium"

        # Cap at student/free-tier limits
        suggested_workers = min(int(rec.get("num_workers", 0)), 2)

        # Rough estimate: 60s base + 60s per copy + 120s per notebook + 2s/MB
        estimated_s = 60 + copy_count * 60 + notebook_count * 120 + int(mb * 2)

        result = {
            "file_size_mb":       round(mb, 2),
            "stage_count":        len(stages),
            "copy_stages":        copy_count,
            "notebook_stages":    notebook_count,
            "complexity":         complexity,
            "suggested_workers":  suggested_workers,
            "estimated_duration_s": estimated_s,
            "node_type":          rec.get("node_type", "Standard_D4s_v3"),
            "shuffle_partitions": rec.get("shuffle_partitions", 8),
        }
        state.predictions = result
        self._log(
            state, "RESOURCE PREDICT",
            f"{mb:.1f} MB · {len(stages)} stages · complexity={complexity}",
            f"~{estimated_s}s estimate · {suggested_workers} workers",
            "info",
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
                "used_fallback":       state.plan.get("used_fallback", False),
                "complexity":          state.predictions.get("complexity"),
                "validation_issues":   state.validation.get("issues", []),
            }
            with open(log_path, "a") as f:
                f.write(json.dumps(record) + "\n")
            self._log(state, "FEEDBACK RECORDED", "→ data/manager_feedback.jsonl", "ok", "ok")
        except Exception as exc:
            self._log(state, "FEEDBACK WARN", str(exc), "non-fatal", "warn")

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
    ):
        """Main orchestration entry point — drives the complete run lifecycle."""
        state = self._runs[run_id]
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

            # ── Phase 2: Pre-checks ──────────────────────────────────────
            state.status = "pre_checks"
            self._enter(state, "pre_checks", "Running resource prediction and cost estimation")
            self.predict_resources(state, csv_size)
            self.estimate_cost(state, state.predictions)
            self.analyze_parallelism(state)

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
