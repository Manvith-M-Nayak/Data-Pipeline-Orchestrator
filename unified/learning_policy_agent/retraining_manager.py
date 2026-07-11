"""
Phase 4 — Model Retraining (the hard part).

This agent is the TRAINER, not the trainee: it runs the retraining process
for the Performance Prediction Agent's models when their error crosses the
policy threshold.

Flow (all safety-gated):

  1. should_retrain()   duration_mape > retrain_error_threshold, evidence
                        sufficient, and not already retraining.
  2. export_real_runs() writes the real run history to
                        performance_prediction_agent/data/real_runs.csv so
                        run_training.py can blend it with the synthetic set
                        (README §9: blend real + synthetic; diversity > epochs).
  3. snapshot           SafetyManager copies models/ BEFORE training.
  4. retrain            runs run_training.py as a subprocess *in the same
                        Python environment as the server* (sklearn .pkl files
                        are version-tied — never retrain in Colab/Kaggle).
  5. compare + deploy   reads models/metrics.json before vs after. The new
                        model stays ONLY if held-out MAE improved; otherwise
                        the snapshot is rolled back automatically.

Runs in a background thread — never blocks live pipeline execution.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import threading
import time
from typing import Dict, List, Optional

from .safety import SafetyManager

_AGENT_DIR = os.path.dirname(os.path.abspath(__file__))
_UNIFIED_DIR = os.path.dirname(_AGENT_DIR)
_PERF_AGENT_DIR = os.path.join(_UNIFIED_DIR, "performance_prediction_agent")
_PERF_MODELS_DIR = os.path.join(_PERF_AGENT_DIR, "models")
_PERF_METRICS = os.path.join(_PERF_MODELS_DIR, "metrics.json")
_REAL_RUNS_CSV = os.path.join(_PERF_AGENT_DIR, "data", "real_runs.csv")
_STATE_PATH = os.path.join(_AGENT_DIR, "data", "retrain_state.json")


class RetrainingManager:
    # If a retrain has been "running" longer than this, treat the lock as
    # stuck rather than trust it forever — a real training run on this
    # project's dataset takes ~2 minutes; 30 minutes is generous headroom
    # before assuming something crashed without releasing the lock (defense
    # in depth alongside the finally-block hardening below — this catches
    # ANY cause of a stuck lock, not just a state-save failure).
    STALE_LOCK_TIMEOUT_S = 1800

    def __init__(self, safety: Optional[SafetyManager] = None):
        self.safety = safety or SafetyManager()
        self._lock = threading.Lock()
        self._running = False
        self._started_at: Optional[float] = None

    def _lock_is_stuck(self) -> bool:
        return (
            self._running
            and self._started_at is not None
            and (time.time() - self._started_at) > self.STALE_LOCK_TIMEOUT_S
        )

    def _clear_stale_lock_if_any(self):
        if self._lock_is_stuck():
            print(f"[RetrainingManager] lock has been held for over "
                  f"{self.STALE_LOCK_TIMEOUT_S}s — treating as stuck and releasing it")
            self._running = False
            self._started_at = None

    # ------------------------------------------------------------ state file

    def _load_state(self) -> Dict:
        if os.path.exists(_STATE_PATH):
            try:
                with open(_STATE_PATH) as f:
                    return json.load(f)
            except json.JSONDecodeError:
                pass
        return {"last_retrain": None, "history": []}

    def _save_state(self, state: Dict):
        os.makedirs(os.path.dirname(_STATE_PATH), exist_ok=True)
        with open(_STATE_PATH, "w") as f:
            json.dump(state, f, indent=2)

    def status(self) -> Dict:
        self._clear_stale_lock_if_any()
        state = self._load_state()
        return {"retraining_now": self._running, **state}

    # -------------------------------------------------------------- decision

    def should_retrain(self, metrics: Dict, policies: Dict) -> Dict:
        """
        FR3 — trigger retraining when prediction error crosses threshold.

        Uses metrics["by_source"]["ml_model"] exclusively — retraining only
        touches the ML model, so it must be triggered by the ML path's own
        error, not a blended figure contaminated by the (already
        self-correcting, unrelated) formula path. See error_analyzer.py and
        policy_engine.py for the same reasoning applied to
        duration_correction_factor.
        """
        self._clear_stale_lock_if_any()
        threshold = policies.get("retrain_error_threshold", 0.20)
        min_runs = policies.get("min_runs_for_update", 10)

        ml_stats = (metrics.get("by_source") or {}).get("ml_model") or {}
        ml_runs = ml_stats.get("runs", 0)
        mape = ml_stats.get("duration_mape")

        cooldown_s = 6 * 3600  # don't retrain more than every 6 hours
        last = self._load_state().get("last_retrain")

        if self._running:
            return {"retrain": False, "reason": "retraining already in progress"}
        if ml_runs < min_runs:
            return {"retrain": False,
                    "reason": f"only {ml_runs} ml_model run(s) in window; need {min_runs} "
                              f"before considering a retrain"}
        if mape is None:
            return {"retrain": False, "reason": "no ml_model prediction-vs-actual pairs available"}
        if mape <= threshold:
            return {"retrain": False,
                    "reason": f"ml_model duration_mape {mape:.0%} <= threshold {threshold:.0%}"}
        if last and (time.time() - last.get("finished", 0)) < cooldown_s:
            return {"retrain": False, "reason": "cooldown active since last retrain"}
        return {"retrain": True,
                "reason": f"ml_model duration_mape {mape:.0%} > threshold {threshold:.0%} "
                          f"over {ml_runs} run(s)"}

    # ------------------------------------------------------ real-data export

    @staticmethod
    def export_real_runs(records: List[Dict]) -> Optional[str]:
        """
        Write real run outcomes to a CSV inside the Performance Prediction
        Agent's data/ dir so run_training.py can blend them in. Only rows
        with both predicted and actual duration are useful.
        """
        rows = [r for r in records
                if r.get("actual_duration_s") and r.get("predicted_duration_s")]
        if not rows:
            return None
        os.makedirs(os.path.dirname(_REAL_RUNS_CSV), exist_ok=True)
        fields = ["run_id", "timestamp", "pipeline_signature", "stage_count",
                  "complexity", "success", "actual_duration_s",
                  "predicted_duration_s", "prediction_source"]
        with open(_REAL_RUNS_CSV, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        return _REAL_RUNS_CSV

    # -------------------------------------------------------------- retrain

    @staticmethod
    def _read_perf_metrics() -> Optional[Dict]:
        if os.path.exists(_PERF_METRICS):
            try:
                with open(_PERF_METRICS) as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return None
        return None

    @staticmethod
    def _extract_mae(metrics: Optional[Dict]) -> Optional[float]:
        """
        Deploy-gate metric. The actual metrics.json written by run_training.py
        nests it as duration_regressor.mae_seconds; older/other shapes are
        tolerated as fallbacks.
        """
        if not metrics:
            return None
        reg = metrics.get("duration_regressor") or metrics.get("regressor") or {}
        for key in ("mae_seconds", "mae", "MAE"):
            if isinstance(reg, dict) and key in reg:
                try:
                    return float(reg[key])
                except (TypeError, ValueError):
                    pass
        for key in ("mae_seconds", "mae", "MAE", "duration_mae"):
            if key in metrics:
                try:
                    return float(metrics[key])
                except (TypeError, ValueError):
                    pass
        return None

    def retrain_async(self, records: List[Dict], on_done=None) -> Dict:
        """Kick off retraining in a background thread (never blocks a run)."""
        self._clear_stale_lock_if_any()
        with self._lock:
            if self._running:
                return {"started": False, "reason": "already running"}
            self._running = True
            self._started_at = time.time()
        t = threading.Thread(target=self._retrain, args=(records, on_done), daemon=True)
        t.start()
        return {"started": True}

    def retrain_sync(self, records: List[Dict]) -> Dict:
        """Blocking version, used by the API endpoint / tests."""
        self._clear_stale_lock_if_any()
        with self._lock:
            if self._running:
                return {"ok": False, "reason": "already running"}
            self._running = True
            self._started_at = time.time()
        return self._retrain(records, on_done=None)

    def _retrain(self, records: List[Dict], on_done) -> Dict:
        result: Dict = {"ok": False, "started_at": time.time()}
        try:
            # 1. export real data for blending
            exported = self.export_real_runs(records)
            result["real_runs_exported"] = exported

            # 2. snapshot current models BEFORE touching anything
            version_id = self.safety.snapshot(
                [_PERF_MODELS_DIR], label="perf_models",
                reason="pre-retrain snapshot of performance prediction models",
            )
            result["snapshot_version"] = version_id
            before_mae = self._extract_mae(self._read_perf_metrics())
            result["mae_before"] = before_mae

            # 3. run the existing training script in THIS python env
            #    (sklearn .pkl files are tied to the env's sklearn version)
            proc = subprocess.run(
                [sys.executable, "run_training.py"],
                cwd=_PERF_AGENT_DIR,
                capture_output=True, text=True, timeout=1800,
            )
            result["returncode"] = proc.returncode
            result["stdout_tail"] = (proc.stdout or "")[-2000:]
            if proc.returncode != 0:
                result["stderr_tail"] = (proc.stderr or "")[-2000:]
                raise RuntimeError("run_training.py exited non-zero")

            # 4. compare on held-out metrics; deploy only if better
            after_mae = self._extract_mae(self._read_perf_metrics())
            result["mae_after"] = after_mae

            if before_mae is not None and after_mae is not None and after_mae > before_mae:
                if version_id:
                    self.safety.rollback(version_id)
                result["deployed"] = False
                result["rolled_back"] = True
                result["reason"] = (f"new model worse on held-out set "
                                    f"(MAE {after_mae:.1f}s > {before_mae:.1f}s) — rolled back")
            else:
                result["deployed"] = True
                result["rolled_back"] = False
                result["reason"] = "new model kept (held-out MAE improved or no baseline)"
            result["ok"] = True

        except Exception as e:  # noqa: BLE001 — must never crash the server
            result["error"] = str(e)
            # best-effort rollback if we got past the snapshot
            vid = result.get("snapshot_version")
            if vid:
                try:
                    self.safety.rollback(vid)
                    result["rolled_back"] = True
                except Exception as rb:  # noqa: BLE001
                    result["rollback_error"] = str(rb)
        finally:
            # The lock release (self._running = False) must happen no matter
            # what — if state-saving throws (disk issue, permission issue,
            # concurrent write, anything) and that line were reached only
            # after a successful save, the retrain lock would get stuck on
            # True permanently, silently blocking every future retrain with
            # "already running" until the server is restarted. So the save
            # is wrapped in its own try/except; the lock release is
            # unconditional regardless of whether the save succeeded.
            result["finished"] = time.time()
            try:
                state = self._load_state()
                state["last_retrain"] = result
                state["history"] = (state.get("history") or [])[-19:] + [
                    {k: result.get(k) for k in
                     ("started_at", "finished", "ok", "deployed", "rolled_back",
                      "mae_before", "mae_after", "reason", "error")}
                ]
                self._save_state(state)
            except Exception as save_exc:  # noqa: BLE001
                print(f"[RetrainingManager] failed to persist retrain state "
                      f"(non-fatal, lock still releases): {save_exc}")
            self._running = False
            self._started_at = None
            if on_done:
                try:
                    on_done(result)
                except Exception:  # noqa: BLE001
                    pass
        return result