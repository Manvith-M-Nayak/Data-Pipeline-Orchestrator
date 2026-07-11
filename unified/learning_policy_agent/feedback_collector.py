"""
Phase 1 — Feedback Collection.

The Central Manager's Phase 5 (record_feedback) already writes one record per
run to unified/data/manager_feedback.jsonl. This module READS that log — it
does not write a second record (no duplicate rows). Its job is to normalize
the Manager's actual record shape onto a fixed learning schema.

Actual record shape written by CentralManager.record_feedback():

  ts                      ISO-8601 string ("2026-07-10T04:18:00.123Z")
  run_id
  final_status            "feedback" for successful runs (record_feedback is
                          called BEFORE state.status is set to "completed"),
                          "failed" for failures
  stage_count, retries
  actual_duration_s
  predicted_duration_s    Resource Agent's estimated_duration_s (baseline)
  perf_predicted_total_s  Performance Prediction Agent's predicted_total_s
                          (added by the learning integration patch)
  prediction_source       "ml_model" | "formula" (added by the patch)
  cost_estimate_usd
  assurance_passed, plan_assurance_passed
  used_fallback, complexity, validation_issues
"""

from __future__ import annotations

import datetime
import json
import os
from typing import Any, Dict, List, Optional

# unified/data/manager_feedback.jsonl — same file the Manager writes and the
# Performance Prediction Agent's formula fallback reads.
_DEFAULT_FEEDBACK_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "manager_feedback.jsonl",
)


def _to_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _ts_to_epoch(ts) -> Optional[float]:
    """Manager writes ISO strings; learning-agent tools may write epoch floats."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    try:
        s = str(ts).rstrip("Z")
        return datetime.datetime.fromisoformat(s).replace(
            tzinfo=datetime.timezone.utc
        ).timestamp()
    except ValueError:
        return None


class FeedbackCollector:
    """Reads and normalizes the Manager's per-run outcome records."""

    def __init__(self, feedback_path: str = _DEFAULT_FEEDBACK_PATH):
        self.feedback_path = feedback_path

    # ------------------------------------------------------------------ read

    def load_records(self, limit: Optional[int] = None) -> List[Dict]:
        """Load all (or last `limit`) normalized records from the log."""
        if not os.path.exists(self.feedback_path):
            return []
        records: List[Dict] = []
        with open(self.feedback_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue  # never let one corrupt line kill learning
                records.append(self.normalize(raw))
        if limit is not None:
            records = records[-limit:]
        return records

    # ------------------------------------------------------------- normalize

    @staticmethod
    def normalize(raw: Dict) -> Dict:
        """
        Map a raw feedback record onto the fixed learning schema.
        Missing fields become None — never raises.
        """
        actual_s = _to_float(raw.get("actual_duration_s"))

        # Performance Prediction Agent's forecast (preferred for error
        # measurement) vs the Resource Agent baseline (legacy records only
        # have the baseline).
        perf_predicted_s = _to_float(raw.get("perf_predicted_total_s"))
        baseline_predicted_s = _to_float(raw.get("predicted_duration_s"))

        est_cost = _to_float(
            raw.get("cost_estimate_usd") or raw.get("estimated_cost_usd")
        )
        actual_cost = _to_float(raw.get("actual_cost_usd"))

        status = str(
            raw.get("final_status") or raw.get("status") or raw.get("outcome") or ""
        ).lower()
        success: Optional[bool]
        # "feedback" means the run reached Phase 5 successfully — the Manager
        # calls record_feedback() before setting status to "completed".
        if status in ("feedback", "completed", "success", "succeeded", "ok"):
            success = True
        elif status in ("failed", "failure", "error", "aborted"):
            success = False
        else:
            success = raw.get("success") if isinstance(raw.get("success"), bool) else None

        stage_count = raw.get("stage_count")
        complexity = raw.get("complexity") or "unknown"

        return {
            "run_id": raw.get("run_id") or raw.get("id"),
            "timestamp": _ts_to_epoch(raw.get("ts") or raw.get("timestamp")),
            # coarse signature so errors can be aggregated per pipeline type
            "pipeline_signature": f"{stage_count}stages_{complexity}",
            "stage_count": stage_count,
            "complexity": complexity,
            "success": success,
            "retries": raw.get("retries"),
            "actual_duration_s": actual_s,
            # what the learning loop measures error against:
            "perf_predicted_total_s": perf_predicted_s,
            "baseline_predicted_s": baseline_predicted_s,
            "predicted_duration_s": perf_predicted_s or baseline_predicted_s,
            "prediction_source": raw.get("prediction_source"),
            "estimated_cost_usd": est_cost,
            "actual_cost_usd": actual_cost,
            "assurance_passed": raw.get("assurance_passed"),
            "plan_assurance_passed": raw.get("plan_assurance_passed"),
            "used_fallback": raw.get("used_fallback"),
        }