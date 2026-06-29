"""
Performance Prediction Agent

Sits one level above the Resource Agent: while the Resource Agent says
"this *stage* allocation will take ~X seconds", this agent looks at the
*whole plan* and asks:

  - What is the total expected wall-clock runtime?
  - Which stage is the bottleneck?
  - Will this run succeed, slow down, or fail — and how confident are we?
  - Will it breach the SLA (target time)?

Inputs (all already on RunState after Phase 2a):
  - resource_plan  : ResourceAgent.analyze() output (allocations, execution_groups,
                     estimated_total_s, correction_factors, feasible)
  - predictions    : Manager's predictions dict (complexity, file_size_mb, stage_count,
                     estimated_duration_s)
  - plan           : raw Planner plan (stages, recommended_settings)
  - history        : data/manager_feedback.jsonl (actual vs predicted, assurance_passed)

Outputs (PerformancePrediction dataclass → serialised dict):
  - predicted_total_s      : formula-based total runtime estimate
  - bottleneck_stage        : stage name most likely to be the slowest
  - outcome                 : "success" | "slowdown" | "failure"
  - confidence              : 0.0 – 1.0
  - sla_breach_risk         : bool  (True if predicted_total_s > sla_target_s)
  - sla_target_s            : the threshold used (default 900 s / 15 min)
  - stage_forecasts         : per-stage {name, predicted_s, risk_level}
  - history_runs_used       : how many historical records informed this prediction
  - adjustment_factor       : multiplier derived from history (1.0 = no correction)
  - rationale               : human-readable reasoning string

Integration:
  - Central Manager calls predict() in Phase 2 (pre_checks), right after
    predict_resources() and estimate_cost().
  - Result stored on RunState.performance_prediction.
  - Manager aborts early if outcome == "failure" (hard gate).
  - Router exposes /performance-prediction/predict for the dashboard.
  - Cost Agent will read predicted_total_s in a future milestone.
"""

import json
import math
import os
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_FEEDBACK_LOG = os.path.join(_DATA_DIR, "manager_feedback.jsonl")

# ── Tuneable constants ────────────────────────────────────────────────────────
DEFAULT_SLA_TARGET_S = 900          # 15 min — matches student-tier expectations
SLOWDOWN_RATIO       = 1.6          # predicted > 1.6× baseline → "slowdown"
FAILURE_RATIO        = 3.0          # predicted > 3.0× baseline → "failure"
MIN_HISTORY_FOR_ML   = 5            # need at least this many runs before using history
DAMPING              = 0.4          # how aggressively history shifts the estimate
STAGE_RISK_WARN_S    = 300          # per-stage warning threshold (5 min)
STAGE_RISK_HIGH_S    = 600          # per-stage high-risk threshold (10 min)


# ── Data classes ──────────────────────────────────────────────────────────────
@dataclass
class StageForecast:
    name:        str
    predicted_s: int
    risk_level:  str    # "ok" | "warning" | "high"
    is_bottleneck: bool


@dataclass
class PerformancePrediction:
    predicted_total_s:    int
    bottleneck_stage:     str
    outcome:              str            # "success" | "slowdown" | "failure"
    confidence:           float          # 0.0 – 1.0
    sla_breach_risk:      bool
    sla_target_s:         int
    stage_forecasts:      List[StageForecast]
    history_runs_used:    int
    adjustment_factor:    float
    throughput_mb_per_s:  Optional[float]   # MB processed per second, None if unknown
    throughput_rows_per_s: Optional[float]  # rows processed per second, None if unknown
    rationale:            str


# ── Performance Prediction Agent ─────────────────────────────────────────────
class PerformancePredictionAgent:

    # ── Public entry point ────────────────────────────────────────────────────
    def predict(
        self,
        resource_plan: dict,
        predictions:   dict,
        plan:          dict,
        sla_target_s:  int = DEFAULT_SLA_TARGET_S,
    ) -> dict:
        """
        Main prediction entry point. Called by the Central Manager during Phase 2.

        Args:
            resource_plan : output of ResourceAgent.analyze()
            predictions   : Manager's state.predictions dict
            plan          : raw Planner plan (stages list, recommended_settings)
            sla_target_s  : wall-clock SLA budget in seconds (default 15 min)

        Returns:
            Serialisable dict of PerformancePrediction.
        """
        # ── 1. Extract per-stage durations from resource plan ─────────────
        allocations      = resource_plan.get("allocations", [])
        execution_groups = resource_plan.get("execution_groups", [])

        if not allocations:
            return self._empty_prediction("No allocations in resource plan", sla_target_s)

        # ── 2. Compute baseline critical-path total ───────────────────────
        #    Critical path = sum of each group's slowest stage (parallel aware)
        stage_dur: Dict[str, int] = {
            a["stage_name"]: int(a.get("duration_s", 0)) for a in allocations
        }

        if not execution_groups:
            # Fallback: treat all as sequential
            execution_groups = [[a["stage_name"]] for a in allocations]

        baseline_s = self._critical_path_duration(stage_dur, execution_groups)

        # ── 3. Load history and compute adjustment factor ─────────────────
        history       = self._load_feedback()
        adj_factor, history_used = self._compute_adjustment(
            history, predictions.get("complexity", "medium")
        )

        # ── 4. Adjusted total ─────────────────────────────────────────────
        predicted_total_s = max(60, int(baseline_s * adj_factor))

        # ── 5. Per-stage forecasts ────────────────────────────────────────
        stage_forecasts, bottleneck = self._build_stage_forecasts(
            allocations, adj_factor
        )

        # ── 6. Outcome classification ─────────────────────────────────────
        #    Compare against the resource agent's own baseline estimate.
        resource_estimate_s = resource_plan.get("estimated_total_s", baseline_s) or baseline_s
        outcome, confidence = self._classify_outcome(
            predicted_total_s, resource_estimate_s, history, predictions
        )
# ── 7a. Throughput ────────────────────────────────────────────────
        file_size_mb = predictions.get("file_size_mb", 0) or 0
        row_count    = int((plan.get("schema") or {}).get("row_count", 0) or 0)

        throughput_mb_per_s = (
            round(file_size_mb / predicted_total_s, 3)
            if predicted_total_s > 0 and file_size_mb > 0
            else None
        )
        throughput_rows_per_s = (
            round(row_count / predicted_total_s, 1)
            if predicted_total_s > 0 and row_count > 0
            else None
        )
        # ── 7. SLA breach check ───────────────────────────────────────────
        sla_breach = predicted_total_s > sla_target_s

        # ── 8. Build rationale ────────────────────────────────────────────
        rationale = self._build_rationale(
            baseline_s, adj_factor, predicted_total_s, bottleneck,
            outcome, confidence, history_used, sla_breach, sla_target_s,
            resource_plan.get("correction_factors", {}),
        )

        result = PerformancePrediction(
            predicted_total_s=predicted_total_s,
            bottleneck_stage=bottleneck,
            outcome=outcome,
            confidence=round(confidence, 3),
            sla_breach_risk=sla_breach,
            sla_target_s=sla_target_s,
            stage_forecasts=stage_forecasts,
            history_runs_used=history_used,
            adjustment_factor=round(adj_factor, 3),
            throughput_mb_per_s=throughput_mb_per_s,
            throughput_rows_per_s=throughput_rows_per_s,
            rationale=rationale,
        )
        return asdict(result)

    # ── Critical-path calculator ──────────────────────────────────────────────
    def _critical_path_duration(
        self,
        stage_dur: Dict[str, int],
        execution_groups: List[List[str]],
    ) -> int:
        """
        Sum the slowest stage in each parallel group across all groups.
        This is the minimum possible wall-clock time for the whole plan.
        """
        total = 0
        for group in execution_groups:
            if not group:
                continue
            group_max = max(stage_dur.get(name, 0) for name in group)
            total += group_max
        return max(total, 60)

    # ── History loading ───────────────────────────────────────────────────────
    def _load_feedback(self) -> List[dict]:
        if not os.path.exists(_FEEDBACK_LOG):
            return []
        records = []
        try:
            with open(_FEEDBACK_LOG) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
        except Exception:
            pass
        return records

    # ── Adjustment factor from history ────────────────────────────────────────
    def _compute_adjustment(
        self, history: List[dict], complexity: str
    ) -> Tuple[float, int]:
        """
        Returns (adjustment_factor, runs_used).

        Strategy:
          - Filter history to same complexity bucket.
          - If fewer than MIN_HISTORY_FOR_ML records → return 1.0 (no correction).
          - Compute mean(actual / predicted) for recent runs.
          - Damp by DAMPING to avoid overcorrection.
        """
        same_complexity = [
            r for r in history
            if r.get("complexity") == complexity
            and r.get("actual_duration_s", 0) > 0
            and r.get("predicted_duration_s", 0) > 0
        ]

        # Fall back to all history if not enough same-complexity runs
        if len(same_complexity) < MIN_HISTORY_FOR_ML:
            usable = [
                r for r in history
                if r.get("actual_duration_s", 0) > 0
                and r.get("predicted_duration_s", 0) > 0
            ]
        else:
            usable = same_complexity

        if len(usable) < MIN_HISTORY_FOR_ML:
            return 1.0, len(usable)

        recent = usable[-10:]   # last 10 runs
        ratios = [
            r["actual_duration_s"] / r["predicted_duration_s"]
            for r in recent
        ]
        mean_ratio = sum(ratios) / len(ratios)
        # Damped: don't overcorrect; move only DAMPING fraction toward observed ratio
        adj = 1.0 + (mean_ratio - 1.0) * DAMPING
        return round(adj, 3), len(recent)

    # ── Per-stage forecasts ───────────────────────────────────────────────────
    def _build_stage_forecasts(
        self,
        allocations: List[dict],
        adj_factor: float,
    ) -> Tuple[List[StageForecast], str]:
        """
        Build per-stage forecast objects and identify the bottleneck stage.
        Returns (stage_forecasts, bottleneck_stage_name).
        """
        forecasts: List[StageForecast] = []
        max_s = 0
        bottleneck = allocations[0]["stage_name"] if allocations else "unknown"

        for alloc in allocations:
            raw_s      = int(alloc.get("duration_s", 0))
            adjusted_s = max(30, int(raw_s * adj_factor))

            if adjusted_s >= STAGE_RISK_HIGH_S:
                risk = "high"
            elif adjusted_s >= STAGE_RISK_WARN_S:
                risk = "warning"
            else:
                risk = "ok"

            is_bottleneck = adjusted_s > max_s
            if is_bottleneck:
                max_s = adjusted_s
                bottleneck = alloc["stage_name"]

            forecasts.append(StageForecast(
                name=alloc["stage_name"],
                predicted_s=adjusted_s,
                risk_level=risk,
                is_bottleneck=False,   # will set the winner below
            ))

        # Mark only the actual bottleneck
        for f in forecasts:
            if f.name == bottleneck:
                f.is_bottleneck = True

        return forecasts, bottleneck

    # ── Outcome classification ────────────────────────────────────────────────
    def _classify_outcome(
        self,
        predicted_total_s:   int,
        resource_estimate_s: int,
        history:             List[dict],
        predictions:         dict,
    ) -> Tuple[str, float]:
        """
        Returns (outcome, confidence).

        outcome:
          "success"   → predicted within acceptable range of resource estimate
          "slowdown"  → predicted is SLOWDOWN_RATIO× or more above estimate
          "failure"   → predicted is FAILURE_RATIO× or more, or historical failure rate is high

        confidence:
          Starts at 0.5 (formula-only), rises toward 0.9 as history accumulates.
        """
        ratio = (
            predicted_total_s / resource_estimate_s
            if resource_estimate_s > 0 else 1.0
        )

        # Historical failure signal
        recent_history = history[-10:] if history else []
        failure_rate = 0.0
        if recent_history:
            failures = sum(
                1 for r in recent_history
                if not r.get("assurance_passed", True)
            )
            failure_rate = failures / len(recent_history)

        # Base outcome from ratio
        if ratio >= FAILURE_RATIO or failure_rate > 0.5:
            outcome = "failure"
        elif ratio >= SLOWDOWN_RATIO or failure_rate > 0.25:
            outcome = "slowdown"
        else:
            outcome = "success"

        # Confidence: formula alone = 0.5; grows with history, capped at 0.90
        history_boost = min(len(recent_history) / 10.0, 1.0) * 0.40
        confidence = round(min(0.50 + history_boost, 0.90), 3)

        # Reduce confidence if resource agent correction factors are large
        corr = predictions.get("correction_factors", {})
        max_corr = max((abs(v - 1.0) for v in corr.values()), default=0.0)
        if max_corr > 0.4:
            confidence = round(max(confidence - 0.10, 0.30), 3)

        return outcome, confidence

    # ── Rationale builder ─────────────────────────────────────────────────────
    def _build_rationale(
        self,
        baseline_s:        int,
        adj_factor:        float,
        predicted_total_s: int,
        bottleneck:        str,
        outcome:           str,
        confidence:        float,
        history_used:      int,
        sla_breach:        bool,
        sla_target_s:      int,
        correction_factors: dict,
    ) -> str:
        parts = [
            f"Critical-path baseline from Resource Agent: {baseline_s}s.",
            f"History adjustment factor: {adj_factor:.3f} "
            f"(derived from {history_used} prior run(s)).",
            f"Adjusted total: {predicted_total_s}s.",
            f"Bottleneck stage: '{bottleneck}'.",
            f"Outcome: {outcome.upper()} (confidence {confidence:.0%}).",
        ]
        if sla_breach:
            parts.append(
                f"SLA BREACH RISK: predicted {predicted_total_s}s > "
                f"target {sla_target_s}s."
            )
        if correction_factors:
            cf_str = ", ".join(f"{k}={v:.3f}" for k, v in correction_factors.items())
            parts.append(f"Resource Agent correction factors: {cf_str}.")
        if history_used < MIN_HISTORY_FOR_ML:
            parts.append(
                f"Only {history_used} historical run(s) available — "
                "formula-only estimate; confidence will improve as runs accumulate."
            )
        return " ".join(parts)

    # ── Empty fallback ────────────────────────────────────────────────────────
    def _empty_prediction(self, reason: str, sla_target_s: int) -> dict:
        return asdict(PerformancePrediction(
            predicted_total_s=0,
            bottleneck_stage="unknown",
            outcome="unknown",
            confidence=0.0,
            sla_breach_risk=False,
            sla_target_s=sla_target_s,
            stage_forecasts=[],
            history_runs_used=0,
            adjustment_factor=1.0,
            throughput_mb_per_s=None,
            throughput_rows_per_s=None,
            rationale=f"Prediction skipped: {reason}",
        ))