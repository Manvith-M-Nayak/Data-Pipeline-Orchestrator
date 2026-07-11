"""
Phase 3 — Policy Updates (rules, no ML).

A "policy" is a named value other agents consult (or the Manager applies on
their behalf): correction factors, retrain thresholds, failure flags.
Policies live in data/policies.json; every change is:

  * evidence-gated  — requires min_runs_for_update runs in the RELEVANT
                      bucket before updating anything (FR5),
  * gradual         — moves at most `learning_rate` of the way toward the
                      observed value, and is clamped to hard bounds,
  * snapshotted     — SafetyManager copies policies.json before writing,
  * logged          — one JSONL line per change with the reason (FR6),
  * reviewed        — the NEXT cycle checks whether the change actually
                      helped (using only records logged after the change)
                      and automatically rolls it back if it made things
                      worse (roadmap Phase 5: "if a new update performs
                      worse in the next batch of runs, automatically roll
                      back"). See _review_pending_changes().

── duration_correction_factor is ML-path-only ──────────────────────────────
manager.py applies this factor ONLY when prediction_source == "ml_model"
(the formula path already self-corrects via its own damped history
adjustment inside performance_agent.py — applying this on top would double-
correct it). So this factor must be LEARNED only from ml_model-sourced
records too, or the two would be inconsistent. It's computed from
metrics["by_source"]["ml_model"] (see error_analyzer.py), not the blended
top-level metrics["duration_ratio"].

Real project data proved this matters: 16 total logged runs, but only 5
were ml_model. The blended ratio across all 16 was 0.747; the ml_model-only
ratio was 0.526 (the model overestimates ~2x in this project's actual
low-complexity/2-stage regime). Using the blended number would have
under-corrected the one path that actually needs correcting.

── resource_headroom_factor is per-signature, not global ───────────────────
An earlier version of this file computed ONE global resource_headroom_factor
from whichever pipeline signature happened to be processed last in the
per-signature loop — with multiple distinct signatures in the window, each
would silently overwrite the last one's contribution within the same cycle.
It's now a dict keyed by signature (resource_headroom_factors), matching
how flagged_signatures already worked. NOTE: this factor is computed but
NOT YET wired into the Resource Agent's own allocation logic — the Resource
Agent already has its own record_actual()-based self-correction loop (see
manager.py's _record_resource_feedback), and wiring this in without first
confirming how that interacts risks the exact double-correction bug found
and fixed for the Performance Prediction Agent above. Treat this as
diagnostic/logged-only until that's confirmed.

── resource_headroom_factors is permanently diagnostic-only ────────────────
This is a deliberate architectural decision, not a TODO. The Resource Agent
(resource_agent.py) already owns a complete, closed feedback loop for this
exact concern: record_actual() -> get_correction_factor(stage_type) feeds
directly back into predict_stage()'s sizing math for the next run, damped
50% toward the observed ratio, computed per stage TYPE (copy/notebook).
That already IS "if a pipeline keeps needing more resources than
predicted, raise its default allocation" — it's just owned by the Resource
Agent, not this one.

resource_headroom_factors here is computed from a DIFFERENT, coarser signal
(overall pipeline duration ratio from manager_feedback.jsonl, per pipeline
SIGNATURE) than the Resource Agent's own correction (per-stage-TYPE ratio
from its own resource_feedback.jsonl). Feeding this back into the Resource
Agent's allocation logic would inject a second, differently-grained
correction on top of one that's already running — the same double-
correction shape found and fixed for the Performance Prediction Agent
above. The codebase already has an explicit convention against this: the
Cost Optimization Agent's own README states "Boundary discipline: the
Resource Agent decides *what's needed*... Neither steps on the other's
territory." This factor respects that boundary by staying diagnostic.

What this agent DOES usefully do in the resource domain: cross-check
whether the Resource Agent's OWN correction factor is actually converging
(see check_resource_agent_drift() below) and flag it if it's stuck far from
1.0 despite plenty of data — a signal that the Resource Agent's underlying
heuristic constants (ADF_MB_PER_DIU_PER_S, DBX_ROWS_PER_S, etc.) may need
re-tuning, which damped correction alone can't fix.

── cost_correction_factor is currently inactive by design, not a bug ────────
It requires actual_cost_usd, which nothing in the system populates yet (no
real Azure billing integration exists) — cost_mape will read as null and
no cost-related policy will ever fire until that data source exists. This
is expected, not something the Learning Agent can fix on its own.

Current policies and what consumes them:

  duration_correction_factor    Multiplier the Manager applies ONLY to the
                                Performance Prediction Agent's ML-path
                                predicted_total_s. Learned exclusively from
                                ml_model-sourced feedback records. Auto-
                                reviewed and rolled back if it makes the
                                next batch of predictions worse.
  resource_headroom_factors     {signature: factor} — diagnostic only for
                                now (see note above); not yet consumed.
  cost_correction_factor        Inactive until real billing data exists.
  retrain_error_threshold       ml_model-path duration_mape above which
                                retraining triggers.
  failure_flag_threshold        failure_rate (or assurance failure rate)
                                above which a pipeline signature is flagged
                                for human review.
"""

from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

from .safety import SafetyManager

_AGENT_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_POLICY_PATH = os.path.join(_AGENT_DIR, "data", "policies.json")
_DEFAULT_LOG_PATH = os.path.join(_AGENT_DIR, "data", "learning_log.jsonl")

DEFAULT_POLICIES: Dict = {
    "version": 3,   # bumped: resource_headroom_factors is now a dict,
                    # added rollback-review + assurance-pattern tracking
    "updated": None,
    # -- learned correction factors (all start neutral) --
    "duration_correction_factor": 1.0,       # applied to ML path only
    "cost_correction_factor": 1.0,           # inactive — see module docstring
    "resource_headroom_factors": {},         # {signature: factor} — diagnostic only
    # -- thresholds / knobs --
    "retrain_error_threshold": 0.20,   # 20% mean duration error (ML path) → retrain
    "failure_flag_threshold": 0.30,    # 30% failure/assurance-failure rate → flag
    "min_runs_for_update": 10,         # FR5: pattern, not one bad run
                                       # (applies to the RELEVANT bucket —
                                       # e.g. ml_model run count for
                                       # duration_correction_factor — not
                                       # the total window size)
    "learning_rate": 0.30,             # gradual: move 30% toward observed
    # Bounds intentionally wide on the low end: real ml_model runs have
    # shown ratios as low as 0.39 (the model can be ~2.5x too high in this
    # project's actual usage regime), so a 0.5 floor would silently clip
    # legitimate correction. 2.0 ceiling unchanged — no observed case yet
    # of underestimation this severe.
    "factor_bounds": {"min": 0.3, "max": 2.0},
    # -- rollback review --
    "rollback_review_min_runs": 5,     # runs needed post-change before judging it
    "rollback_tolerance": 0.05,        # only roll back if it's >5% worse, not noise
    # -- flags raised for humans: {signature: {reasons: [...], ...}} --
    "flagged_signatures": {},
    # -- internal bookkeeping, not meant to be hand-edited --
    "_pending_reviews": {},
}


class PolicyEngine:
    def __init__(
        self,
        policy_path: str = _DEFAULT_POLICY_PATH,
        log_path: str = _DEFAULT_LOG_PATH,
        safety: Optional[SafetyManager] = None,
    ):
        self.policy_path = policy_path
        self.log_path = log_path
        self.safety = safety or SafetyManager()

    # ---------------------------------------------------------------- storage

    def load(self) -> Dict:
        if os.path.exists(self.policy_path):
            try:
                with open(self.policy_path) as f:
                    stored = json.load(f)
                merged = {**DEFAULT_POLICIES, **stored}  # forward-compatible
                return merged
            except json.JSONDecodeError:
                pass
        return dict(DEFAULT_POLICIES)

    def _save(self, policies: Dict):
        policies["updated"] = time.time()
        os.makedirs(os.path.dirname(self.policy_path), exist_ok=True)
        with open(self.policy_path, "w") as f:
            json.dump(policies, f, indent=2)

    def _log(self, entry: Dict):
        entry["timestamp"] = time.time()
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        with open(self.log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def get_log(self, limit: int = 100) -> List[Dict]:
        if not os.path.exists(self.log_path):
            return []
        entries = []
        with open(self.log_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return entries[-limit:]

    # ---------------------------------------------------------------- updates

    @staticmethod
    def _gradual(current: float, target: float, lr: float, lo: float, hi: float) -> float:
        moved = current + (target - current) * lr
        return round(min(max(moved, lo), hi), 4)

    # ---------------------------------------------------- rollback review (Phase 5)

    def _review_pending_changes(
        self, records: List[Dict], policies: Dict
    ) -> List[Dict]:
        """
        For every policy change still awaiting review, check whether records
        logged SINCE the change actually improved things. Currently only
        duration_correction_factor is reviewed this way (it's the only
        policy fed back into a live prediction path today).

        Returns a list of {policy, action: "rolled_back"|"confirmed", ...}
        entries. Mutates `policies` in place (reverts the value on rollback).
        """
        events: List[Dict] = []
        pending = policies.get("_pending_reviews") or {}
        review_min = policies.get("rollback_review_min_runs", 5)
        tolerance = policies.get("rollback_tolerance", 0.05)

        still_pending = {}
        for policy_name, review in pending.items():
            changed_at = review.get("changed_at", 0)

            if policy_name == "duration_correction_factor":
                # Only ml_model records logged AFTER the change reflect its
                # real effect (the corrected value is what's actually
                # logged as predicted_duration_s once applied).
                post_change = [
                    r for r in records
                    if r.get("prediction_source") == "ml_model"
                    and (r.get("timestamp") or 0) > changed_at
                    and r.get("actual_duration_s")
                    and r.get("predicted_duration_s")
                ]
                if len(post_change) < review_min:
                    still_pending[policy_name] = review  # not enough evidence yet
                    continue

                apes = [
                    abs(r["actual_duration_s"] - r["predicted_duration_s"]) / r["actual_duration_s"]
                    for r in post_change if r["actual_duration_s"] > 0
                ]
                post_change_mape = sum(apes) / len(apes) if apes else None
                pre_change_mape = review.get("pre_change_mape")

                if post_change_mape is not None and pre_change_mape is not None:
                    if post_change_mape > pre_change_mape * (1 + tolerance):
                        old_value = review["old_value"]
                        policies[policy_name] = old_value
                        events.append({
                            "policy": policy_name,
                            "action": "rolled_back",
                            "reverted_to": old_value,
                            "reverted_from": review["new_value"],
                            "reason": f"post-change duration_mape ({post_change_mape:.1%} over "
                                      f"{len(post_change)} runs) was worse than pre-change "
                                      f"({pre_change_mape:.1%}) — automatic rollback per Phase 5",
                        })
                    else:
                        events.append({
                            "policy": policy_name,
                            "action": "confirmed",
                            "value": review["new_value"],
                            "reason": f"post-change duration_mape ({post_change_mape:.1%} over "
                                      f"{len(post_change)} runs) held or improved vs pre-change "
                                      f"({pre_change_mape:.1%}) — keeping the update",
                        })
                # else: couldn't compute either metric — drop the pending
                # review silently rather than block future updates forever.

        policies["_pending_reviews"] = still_pending
        return events

    # ------------------------------------------- Resource Agent drift check

    def check_resource_agent_drift(
        self,
        min_records: int = 15,
        stuck_threshold: float = 0.3,
    ) -> List[Dict]:
        """
        Cross-check whether the Resource Agent's OWN self-correction is
        actually converging. Reads unified/data/resource_feedback.jsonl
        directly — the same file resource_agent.py's record_actual() writes
        and get_correction_factor() reads — using the identical damped-ratio
        computation, so this never disagrees with what the Resource Agent
        itself would compute. This does NOT feed anything back into the
        Resource Agent; it only surfaces a flag if that agent's own
        correction has been stuck far from 1.0 despite having plenty of
        data, which points at its underlying heuristic constants
        (ADF_MB_PER_DIU_PER_S, DBX_ROWS_PER_S, etc. in resource_agent.py)
        needing a look, not something more damping can fix.
        """
        # unified/data/resource_feedback.jsonl — same _DATA_DIR pattern
        # resource_agent.py itself uses (one level up from its own module dir).
        feedback_path = os.path.join(_AGENT_DIR, "..", "data", "resource_feedback.jsonl")
        if not os.path.exists(feedback_path):
            return []

        records = []
        with open(feedback_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        flags: List[Dict] = []
        for stage_type in ("copy", "notebook"):
            type_records = [r for r in records if r.get("stage_type") == stage_type]
            if len(type_records) < min_records:
                continue

            # Identical to ResourceAgent.get_correction_factor(): last 10,
            # damped 50% toward the observed ratio.
            ratios = [
                r["actual_duration_s"] / r["predicted_duration_s"]
                for r in type_records
                if r.get("predicted_duration_s", 0) > 0
            ]
            if not ratios:
                continue
            recent = ratios[-10:]
            avg = sum(recent) / len(recent)
            current_factor = round(1.0 + (avg - 1.0) * 0.5, 3)

            if abs(current_factor - 1.0) > stuck_threshold:
                flags.append({
                    "stage_type": stage_type,
                    "correction_factor": current_factor,
                    "records": len(type_records),
                    "reason": f"Resource Agent's own '{stage_type}' correction factor is "
                              f"{current_factor} after {len(type_records)} records — still "
                              f"{abs(current_factor - 1.0):.0%} off from 1.0 despite plenty of "
                              f"data. Its damped self-correction (50% toward observed ratio) "
                              f"should be converging closer to 1.0 by now; a persistent gap "
                              f"this size suggests the underlying heuristic constants for "
                              f"'{stage_type}' stages need re-tuning, not just more correction.",
                })
        return flags

    def evaluate_and_apply(self, metrics: Dict) -> Dict:
        """
        Look at aggregate error metrics and apply gradual policy updates.
        Returns a report of what changed (possibly nothing).
        """
        policies = self.load()
        changes: List[Dict] = []
        review_events: List[Dict] = []

        min_runs = policies["min_runs_for_update"]
        lr = policies["learning_rate"]
        lo, hi = policies["factor_bounds"]["min"], policies["factor_bounds"]["max"]

        by_source = metrics.get("by_source") or {}
        ml_stats = by_source.get("ml_model") or {}
        ml_runs = ml_stats.get("runs", 0)
        ml_ratio = ml_stats.get("duration_ratio")
        ml_mape = ml_stats.get("duration_mape")

        # We need the raw records (with timestamps) to review pending
        # changes — evaluate_and_apply is normally called with just the
        # metrics dict, so accept records via a back door: the caller
        # (LearningPolicyAgent.run_cycle) is updated to pass them through
        # metrics["_records"] for this purpose only; never persisted.
        records = metrics.get("_records") or []

        # ── Phase 5: review any pending change from a previous cycle FIRST,
        # before potentially creating a new one ────────────────────────────
        if records:
            review_events = self._review_pending_changes(records, policies)

        pending = policies.get("_pending_reviews") or {}
        duration_change_in_flight = "duration_correction_factor" in pending

        # A rollback just happened THIS cycle for duration_correction_factor —
        # don't immediately propose a fresh change on top of the reverted
        # value in the same breath. Give the reverted value at least one
        # full cycle to gather its own evidence before touching it again,
        # otherwise the rollback safety net never actually rests anywhere.
        just_rolled_back = {
            e["policy"] for e in review_events if e.get("action") == "rolled_back"
        }
        duration_just_rolled_back = "duration_correction_factor" in just_rolled_back

        # ── duration_correction_factor: ML-path-only, gated on ML run count ──
        if duration_just_rolled_back:
            duration_update_skipped = (
                "duration_correction_factor was just rolled back this cycle — "
                "resting at the reverted value for at least one more cycle "
                "before proposing another change"
            )
        elif duration_change_in_flight:
            duration_update_skipped = (
                "a previous duration_correction_factor change is still "
                "awaiting review — not stacking another change on top of "
                "an unconfirmed one"
            )
        elif ml_runs < min_runs:
            duration_update_skipped = (
                f"only {ml_runs} ml_model run(s) in window; need {min_runs} "
                f"before updating duration_correction_factor "
                f"(FR5: pattern, not one bad run; formula-path runs don't "
                f"count here since the factor is never applied to them)"
            )
        else:
            duration_update_skipped = None
            if ml_ratio is not None and abs(ml_ratio - policies["duration_correction_factor"]) > 0.05:
                old = policies["duration_correction_factor"]
                new = self._gradual(old, ml_ratio, lr, lo, hi)
                if new != old:
                    policies["duration_correction_factor"] = new
                    policies["_pending_reviews"]["duration_correction_factor"] = {
                        "changed_at": time.time(),
                        "old_value": old,
                        "new_value": new,
                        "pre_change_mape": ml_mape,
                    }
                    changes.append({
                        "policy": "duration_correction_factor",
                        "old": old, "new": new,
                        "reason": f"mean actual/predicted duration ratio over last {ml_runs} "
                                  f"ml_model run(s) = {ml_ratio} (ML path "
                                  f"{'over' if ml_ratio < 1 else 'under'}estimates durations). "
                                  f"Computed from ml_model-sourced records only — "
                                  f"NOT the blended window average. Will be auto-reviewed "
                                  f"next cycle and rolled back if it makes things worse.",
                    })

        # Overall window metrics still need enough TOTAL runs for the rest
        # of this method's signature/failure-rate checks below.
        n = metrics.get("runs_analyzed", 0)
        if n < min_runs and not changes and not review_events:
            return {
                "changes": [],
                "review_events": [],
                "skipped": True,
                "reason": duration_update_skipped or
                          f"only {n} runs in window; need {min_runs} (FR5: pattern, not one bad run)",
            }

        # --- cost correction: intentionally inactive — see module docstring.
        # cost_mape will be null until actual_cost_usd is populated by a real
        # billing integration; nothing to compute or gate here yet.
        cost_mape = metrics.get("cost_mape")
        if cost_mape is not None and cost_mape > 0.15:
            changes.append({
                "policy": "cost_estimate_accuracy_flag",
                "old": None, "new": round(cost_mape, 4),
                "reason": f"cost estimates off by {cost_mape:.0%} on average over last {n} runs "
                          f"— review COST_MODEL_ASSUMPTIONS in cost_optimizer.py "
                          f"(informational only — no correction factor is applied)",
            })

        # --- per-signature: resource under-provisioning, failure, and
        # plan-correctness patterns
        for sig, stats in (metrics.get("per_signature") or {}).items():
            if stats.get("runs", 0) < min_runs:
                continue

            reasons_for_flag = []

            sig_ratio = stats.get("duration_ratio")
            if sig_ratio is not None and sig_ratio > 1.3:
                headroom = policies.setdefault("resource_headroom_factors", {})
                old = headroom.get(sig, 1.0)
                new = self._gradual(old, sig_ratio, lr, lo, hi)
                if new != old:
                    headroom[sig] = new
                    changes.append({
                        "policy": f"resource_headroom_factors[{sig}]",
                        "old": old, "new": new,
                        "reason": f"signature '{sig}' consistently runs {sig_ratio}x longer than "
                                  f"predicted over {stats['runs']} runs — likely under-provisioned. "
                                  f"Diagnostic only: the Resource Agent already self-corrects this "
                                  f"via its own get_correction_factor()/record_actual() loop; this "
                                  f"value is intentionally never fed back into it (see module "
                                  f"docstring — boundary discipline).",
                    })

            fail_rate = stats.get("failure_rate")
            if fail_rate is not None and fail_rate > policies["failure_flag_threshold"]:
                reasons_for_flag.append(("run_failure_rate", fail_rate))

            assurance_fail_rate = stats.get("assurance_failure_rate")
            if assurance_fail_rate is not None and assurance_fail_rate > policies["failure_flag_threshold"]:
                reasons_for_flag.append(("assurance_failure_rate", assurance_fail_rate))

            plan_assurance_fail_rate = stats.get("plan_assurance_failure_rate")
            if plan_assurance_fail_rate is not None and plan_assurance_fail_rate > policies["failure_flag_threshold"]:
                reasons_for_flag.append(("plan_assurance_failure_rate", plan_assurance_fail_rate))

            if reasons_for_flag:
                already = policies["flagged_signatures"].get(sig)
                policies["flagged_signatures"][sig] = {
                    "reasons": {name: val for name, val in reasons_for_flag},
                    "runs": stats["runs"],
                    "flagged_at": time.time(),
                }
                if not already:
                    reason_str = ", ".join(f"{name}={val:.0%}" for name, val in reasons_for_flag)
                    changes.append({
                        "policy": "flagged_signatures",
                        "old": None, "new": sig,
                        "reason": f"signature '{sig}' flagged over {stats['runs']} runs: "
                                  f"{reason_str} — needs human review",
                    })

        # --- persist (snapshot first) + log
        if changes or review_events:
            self.safety.snapshot([self.policy_path], label="policies",
                                 reason="pre-update snapshot before policy changes")
            self._save(policies)
            for c in changes:
                self._log({"type": "policy_update", **c})
            for e in review_events:
                self._log({"type": "policy_review", **e})

        return {
            "changes": changes,
            "review_events": review_events,
            "skipped": False,
            "policies": policies,
            "ml_runs_in_window": ml_runs,
            "ml_duration_ratio": ml_ratio,
            "ml_duration_mape": ml_mape,
        }