"""
Phase 2 — Measure Errors.

Turns raw run records into "how wrong were we?" numbers. Pure statistics,
no ML. All metrics are computed over a sliding window of the most recent
runs (default 50) so that old behavior stops influencing decisions.

── Why errors are split by prediction_source ───────────────────────────────
The Performance Prediction Agent has two paths with fundamentally different
error characteristics:

  * "formula"   — already self-corrects via its own damped history
                  adjustment (see performance_agent.py's _compute_adjustment,
                  which walks 0.4x toward the observed actual/predicted ratio
                  on every call). Its remaining error after that correction
                  is usually small and shouldn't be corrected again.
  * "ml_model"  — trained once, offline, on synthetic data; its predictions
                  are frozen until the next retrain and get ZERO runtime
                  correction of their own (adjustment_factor is hard-coded
                  to 1.0 in _build_ml_response). This is the path the
                  Learning Agent's duration_correction_factor is meant for.

Pre-patch feedback records have no prediction_source field at all — they
predate the field being logged, but were, in fact, all formula-path results
(the ML path only started working once feature_encoder.pkl existed). They
are bucketed as "formula" for that reason, not left as a third "unknown"
category, since that's what they actually are.

Blending the two into one aggregate ratio is wrong: real data confirms it.
Five real ml_model runs showed ratios of 0.39/0.49/0.46/0.42/0.87 (the
model overestimates duration ~2x in the low-complexity/2-stage regime this
project actually runs in) while ten formula-path runs, already
self-corrected, sat around 0.6-0.8. A single blended ratio would produce a
correction factor calibrated to neither path.

So: duration_ratio / duration_mape (top-level, backward compatible) still
describe the whole window for dashboards. But the actionable numbers the
Learning Agent's policy engine acts on come from `by_source["ml_model"]`
only — see policy_engine.py.
"""

from __future__ import annotations

from statistics import mean
from typing import Dict, List, Optional


def _safe_mean(xs: List[float]) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    return round(mean(xs), 4) if xs else None


def _source_bucket(record: Dict) -> str:
    """
    Pre-patch records (no prediction_source key at all) were still formula
    results in practice — the ML path couldn't load until
    feature_encoder.pkl existed. Bucket them as "formula", not "unknown",
    since that's what actually produced them.
    """
    src = record.get("prediction_source")
    if src in ("ml_model", "formula"):
        return src
    return "formula"


def _run_failed(record: Dict) -> bool:
    """
    IMPORTANT: by the time records reach ErrorAnalyzer, they've already
    passed through FeedbackCollector.normalize(), which converts the
    Manager's raw final_status into a "success" boolean and does NOT pass
    final_status through at all. Checking "final_status" here (an earlier
    version of this function did) would silently always return False —
    the exact same class of dead-filter bug found twice already in this
    project. "success" is the correct, actually-present field.

    A failed run's actual_duration_s includes retry backoff sleep (10s/30s)
    and repeated failed connection attempts, not real execution time —
    including it in duration_ratio/cost_ratio would teach both correction
    factors from noise, not signal. success is None (not False) for records
    where the outcome couldn't be determined — treated as NOT failed, since
    that's the safer default (matches how normalize() already treats
    unrecognized status strings).
    """
    return record.get("success") is False


class ErrorAnalyzer:
    def __init__(self, window: int = 50, min_runs: int = 5):
        self.window = window
        self.min_runs = min_runs  # below this, metrics are reported but
                                  # flagged as insufficient evidence

    # ------------------------------------------------------------- per run

    @staticmethod
    def per_run_errors(record: Dict) -> Dict:
        out: Dict = {"run_id": record.get("run_id")}
        if _run_failed(record):
            # A failed run's actual_duration_s is mostly retry-backoff sleep
            # and repeated failed connection attempts, not real work — never
            # let it contribute a duration_ratio/cost_ratio/cost_ape. It can
            # still be counted elsewhere (failure_rate) since that's a valid,
            # different signal.
            return out

        actual = record.get("actual_duration_s")
        predicted = record.get("predicted_duration_s")
        est_cost = record.get("estimated_cost_usd")
        act_cost = record.get("actual_cost_usd")

        if actual and predicted and predicted > 0:
            out["duration_ratio"] = round(actual / predicted, 4)
            out["duration_ape"] = round(abs(actual - predicted) / actual, 4) if actual > 0 else None
        if est_cost is not None and act_cost is not None and act_cost > 0:
            out["cost_ape"] = round(abs(est_cost - act_cost) / act_cost, 4)
            if est_cost > 0:
                out["cost_ratio"] = round(act_cost / est_cost, 4)
        return out

    # ------------------------------------------------------------ aggregate

    def analyze(self, records: List[Dict]) -> Dict:
        recent = records[-self.window :]
        n = len(recent)

        ratios, apes, cost_apes, cost_ratios, fails = [], [], [], [], []
        assurance_fails, plan_assurance_fails = [], []
        by_sig: Dict[str, Dict[str, list]] = {}
        # per-source: what the policy engine actually acts on
        by_source: Dict[str, Dict[str, list]] = {
            "ml_model": {"ratios": [], "apes": []},
            "formula": {"ratios": [], "apes": []},
        }

        for r in recent:
            e = self.per_run_errors(r)
            sig = r.get("pipeline_signature") or "unknown"
            bucket = by_sig.setdefault(
                sig, {"ratios": [], "fails": [], "assurance_fails": [],
                      "plan_assurance_fails": [], "n": 0}
            )
            bucket["n"] += 1

            src_bucket = by_source[_source_bucket(r)]

            if "duration_ratio" in e:
                ratios.append(e["duration_ratio"])
                bucket["ratios"].append(e["duration_ratio"])
                src_bucket["ratios"].append(e["duration_ratio"])
            if e.get("duration_ape") is not None:
                apes.append(e["duration_ape"])
                src_bucket["apes"].append(e["duration_ape"])
            if e.get("cost_ape") is not None:
                cost_apes.append(e["cost_ape"])
            if e.get("cost_ratio") is not None:
                cost_ratios.append(e["cost_ratio"])
            if r.get("success") is not None:
                is_fail = 1.0 if r["success"] is False else 0.0
                fails.append(is_fail)
                bucket["fails"].append(is_fail)

            # ── Plan-correctness patterns (FR2's "was the plan correct?"
            # input, previously logged but never analyzed) ─────────────────
            if r.get("assurance_passed") is not None:
                a_fail = 0.0 if r["assurance_passed"] else 1.0
                assurance_fails.append(a_fail)
                bucket["assurance_fails"].append(a_fail)
            if r.get("plan_assurance_passed") is not None:
                pa_fail = 0.0 if r["plan_assurance_passed"] else 1.0
                plan_assurance_fails.append(pa_fail)
                bucket["plan_assurance_fails"].append(pa_fail)

        per_signature = {
            sig: {
                "runs": b["n"],
                "duration_ratio": _safe_mean(b["ratios"]),
                "failure_rate": _safe_mean(b["fails"]),
                "assurance_failure_rate": _safe_mean(b["assurance_fails"]),
                "plan_assurance_failure_rate": _safe_mean(b["plan_assurance_fails"]),
            }
            for sig, b in by_sig.items()
        }

        per_source_metrics = {
            src: {
                "runs": len(b["ratios"]),
                "duration_ratio": _safe_mean(b["ratios"]),
                "duration_mape": _safe_mean(b["apes"]),
            }
            for src, b in by_source.items()
        }

        return {
            "runs_analyzed": n,
            "window": self.window,
            "sufficient_evidence": n >= self.min_runs,
            # whole-window aggregate — for dashboards only, NOT used to
            # compute duration_correction_factor (see by_source below)
            "duration_ratio": _safe_mean(ratios),      # actual / predicted
            "duration_mape": _safe_mean(apes),          # 0.20 = 20% avg error
            "cost_mape": _safe_mean(cost_apes),
            "cost_ratio": _safe_mean(cost_ratios),      # actual_cost_usd / estimated_cost_usd
            "cost_runs": len(cost_ratios),              # evidence count for cost_correction_factor gating
            "failure_rate": _safe_mean(fails),
            "assurance_failure_rate": _safe_mean(assurance_fails),
            "plan_assurance_failure_rate": _safe_mean(plan_assurance_fails),
            "per_signature": per_signature,
            # what policy_engine.py actually acts on
            "by_source": per_source_metrics,
        }