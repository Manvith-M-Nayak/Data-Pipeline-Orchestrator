"""
Learning and Policy Update Agent — orchestrator.

One learning cycle = collect → measure → policy update → maybe retrain.

The Central Manager's record_feedback() already writes one record per run to
manager_feedback.jsonl — this agent READS that log rather than writing a
duplicate. The Manager just calls the hook after recording:

  from learning_policy_agent import get_learning_agent
  get_learning_agent().on_run_recorded()   # end of record_feedback()

Every CYCLE_EVERY_N_RUNS runs, the hook triggers a full cycle. Cycles are
cheap (JSONL read + statistics) except retraining, which runs in a background
thread and never blocks live execution.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional

from .feedback_collector import FeedbackCollector
from .error_analyzer import ErrorAnalyzer
from .policy_engine import PolicyEngine
from .retraining_manager import RetrainingManager
from .safety import SafetyManager

_AGENT_DIR = os.path.dirname(os.path.abspath(__file__))
_CYCLE_STATE = os.path.join(_AGENT_DIR, "data", "cycle_state.json")

# Manager triggers a cycle every N recorded runs (see manager integration)
CYCLE_EVERY_N_RUNS = 5


class LearningPolicyAgent:
    def __init__(
        self,
        feedback_path: Optional[str] = None,
        window: int = 50,
    ):
        self.safety = SafetyManager()
        self.collector = (
            FeedbackCollector(feedback_path) if feedback_path else FeedbackCollector()
        )
        self.analyzer = ErrorAnalyzer(window=window)
        self.policies = PolicyEngine(safety=self.safety)
        self.retrainer = RetrainingManager(safety=self.safety)

    # ------------------------------------------------------------- per run

    def on_run_recorded(self, background: bool = True) -> Optional[Dict]:
        """
        FR1 hook — call at the end of CentralManager.record_feedback(), after
        the record is written to manager_feedback.jsonl. Bumps a counter and
        runs a full learning cycle every CYCLE_EVERY_N_RUNS runs (learning
        happens between runs, retraining in a background thread).
        """
        self._bump_runs_since_cycle()
        cs = self._load_cycle_state()
        if cs.get("runs_since_cycle", 0) < CYCLE_EVERY_N_RUNS:
            return None
        return self.run_cycle(background_retrain=background)

    # ---------------------------------------------------------------- cycle

    def run_cycle(self, background_retrain: bool = True) -> Dict:
        """One full learning cycle. Returns a human-readable report."""
        records = self.collector.load_records()
        metrics = self.analyzer.analyze(records)                     # FR2

        # Phase 5's rollback review needs per-record timestamps to tell
        # which records were logged after a policy change took effect.
        # Passed via a private key rather than changing evaluate_and_apply's
        # public signature; stripped out of everything we return below.
        metrics_with_records = {**metrics, "_records": records}
        policy_report = self.policies.evaluate_and_apply(metrics_with_records)  # FR4/FR5

        current_policies = self.policies.load()
        decision = self.retrainer.should_retrain(metrics, current_policies)  # FR3

        retrain_report: Dict = {"triggered": False, **decision}
        if decision["retrain"]:
            if background_retrain:
                started = self.retrainer.retrain_async(records)
                retrain_report = {"triggered": started.get("started", False),
                                  "mode": "background", **decision}
            else:
                retrain_report = {"triggered": True, "mode": "sync",
                                  **self.retrainer.retrain_sync(records)}
            self.policies._log({  # improvement log entry (FR6)
                "type": "retrain_triggered",
                "reason": decision["reason"],
            })

        resource_drift_flags = self.policies.check_resource_agent_drift()
        if resource_drift_flags:
            for flag in resource_drift_flags:
                self.policies._log({"type": "resource_agent_drift_flag", **flag})

        self._reset_cycle_counter()
        report = {
            "timestamp": time.time(),
            "metrics": metrics,  # the clean version, no _records key
            "policy_updates": policy_report.get("changes", []),
            "policy_review_events": policy_report.get("review_events", []),
            "policy_skip_reason": policy_report.get("reason"),
            "retraining": retrain_report,
            "flagged_signatures": current_policies.get("flagged_signatures", {}),
            "resource_headroom_factors": current_policies.get("resource_headroom_factors", {}),
            "resource_agent_drift_flags": resource_drift_flags,
        }
        return report

    # ---------------------------------------------------------------- status

    def get_status(self) -> Dict:
        records = self.collector.load_records()
        return {
            "total_runs_recorded": len(records),
            "metrics": self.analyzer.analyze(records),
            "policies": self.policies.load(),
            "retraining": self.retrainer.status(),
            "versions": self.safety.list_versions()[-10:],
            "runs_since_last_cycle": self._load_cycle_state().get("runs_since_cycle", 0),
            "cycle_every_n_runs": CYCLE_EVERY_N_RUNS,
        }

    def get_log(self, limit: int = 100):
        return self.policies.get_log(limit)

    def rollback(self, version_id: str) -> Dict:
        result = self.safety.rollback(version_id)
        self.policies._log({"type": "manual_rollback", "version_id": version_id})
        return result

    # ------------------------------------------------------- cycle counter

    def _load_cycle_state(self) -> Dict:
        if os.path.exists(_CYCLE_STATE):
            try:
                with open(_CYCLE_STATE) as f:
                    return json.load(f)
            except json.JSONDecodeError:
                pass
        return {"runs_since_cycle": 0}

    def _save_cycle_state(self, cs: Dict):
        os.makedirs(os.path.dirname(_CYCLE_STATE), exist_ok=True)
        with open(_CYCLE_STATE, "w") as f:
            json.dump(cs, f)

    def _bump_runs_since_cycle(self):
        cs = self._load_cycle_state()
        cs["runs_since_cycle"] = cs.get("runs_since_cycle", 0) + 1
        self._save_cycle_state(cs)

    def _reset_cycle_counter(self):
        self._save_cycle_state({"runs_since_cycle": 0})


# ── Shared singleton ─────────────────────────────────────────────────────────
# The Manager and the FastAPI router must share one instance so the retrain
# lock and cycle counter are consistent.
_shared_agent: Optional[LearningPolicyAgent] = None


def get_learning_agent() -> LearningPolicyAgent:
    global _shared_agent
    if _shared_agent is None:
        _shared_agent = LearningPolicyAgent()
    return _shared_agent