"""
ML-backed Performance Prediction — primary path.

Primary:  trained models loaded locally from models/
Fallback: formula in performance_agent.py (if models missing or fail)

v3 feature set adds 4 derived features:
  - data_complexity_score      : log(file_size) * log(row_count) / 100
  - pipeline_risk_index        : pre-computed weighted risk from plan features
  - correction_uncertainty     : mean absolute deviation of both corrections from 1.0
  - stage_parallelism_efficiency: 1 - (n_groups / stage_count)

IMPORTANT: always retrain inside the project venv, never copy .pkl files
across machines. Run python3 run_training.py to regenerate.
"""

import os
from typing import Dict, List, Optional

import joblib
import numpy as np

_MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")

_DURATION_MODEL_PATH = os.path.join(_MODEL_DIR, "duration_regressor.pkl")
_OUTCOME_MODEL_PATH  = os.path.join(_MODEL_DIR, "outcome_classifier.pkl")
_ENCODER_PATH        = os.path.join(_MODEL_DIR, "feature_encoder.pkl")

FEATURE_COLS = [
    "stage_count", "copy_stages", "notebook_stages",
    "file_size_mb", "row_count",
    "n_execution_groups", "parallel_ratio",
    "transform_count", "agg_count",
    "copy_correction", "notebook_correction",
    "resource_estimate_s", "baseline_s",
    "network_quality",
    "complexity_encoded",
    # Derived features added in v3
    "data_complexity_score",
    "pipeline_risk_index",
    "correction_uncertainty",
    "stage_parallelism_efficiency",
]


class MLNotAvailable(Exception):
    pass


class MLPredictor:
    _duration_model = None
    _outcome_model  = None
    _encoder        = None
    _load_attempted = False
    _load_error     = None

    @classmethod
    def _ensure_loaded(cls):
        if cls._load_attempted:
            if cls._duration_model is None:
                raise MLNotAvailable(cls._load_error or "Models not loaded")
            return

        cls._load_attempted = True
        try:
            cls._duration_model = joblib.load(_DURATION_MODEL_PATH)
            cls._outcome_model  = joblib.load(_OUTCOME_MODEL_PATH)
            cls._encoder        = joblib.load(_ENCODER_PATH)
        except Exception as exc:
            cls._load_error = str(exc)
            cls._duration_model = None
            cls._outcome_model  = None
            cls._encoder        = None
            raise MLNotAvailable(f"Failed to load model files: {exc}")

    @classmethod
    def is_available(cls) -> bool:
        try:
            cls._ensure_loaded()
            return True
        except MLNotAvailable:
            return False

    @classmethod
    def _build_feature_row(
        cls,
        resource_plan: dict,
        predictions: dict,
        plan: dict,
    ) -> np.ndarray:
        stages = plan.get("stages", [])
        allocations = resource_plan.get("allocations", [])
        execution_groups = resource_plan.get("execution_groups", [])

        stage_count = len(stages) or len(allocations)
        copy_stages = sum(1 for s in stages if s.get("type") == "copy") \
            or sum(1 for a in allocations if a.get("stage_type") == "copy")
        notebook_stages = stage_count - copy_stages

        file_size_mb = float(predictions.get("file_size_mb", 0) or 0)
        row_count    = int((plan.get("schema") or {}).get("row_count", 0) or 0)

        complexity = predictions.get("complexity", "medium")
        try:
            complexity_encoded = int(cls._encoder.transform([complexity])[0])
        except Exception:
            complexity_encoded = int(cls._encoder.transform(["medium"])[0])

        n_groups = len(execution_groups) or 1
        parallel_ratio = round(1 - (n_groups / max(stage_count, 1)), 3)

        transform_count = sum(len(s.get("transformations", []) or []) for s in stages)
        agg_count = sum(
            len((s.get("aggregations") or {}).get("agg_exprs", []))
            for s in stages if isinstance(s.get("aggregations"), dict)
        )

        corr = resource_plan.get("correction_factors", {}) or {}
        copy_correction     = float(corr.get("copy", 1.0))
        notebook_correction = float(corr.get("notebook", 1.0))

        resource_estimate_s = float(resource_plan.get("estimated_total_s", 0) or 0)
        baseline_s = resource_estimate_s

        # network_quality: no real telemetry yet, default to 0.7 (dataset mean)
        network_quality = float(predictions.get("network_quality", 0.7))

        # ── Derived features (must match run_training.py exactly) ─────────
        data_complexity_score = (
            np.log1p(file_size_mb) * np.log1p(row_count) / 100.0
        )

        copy_share = copy_stages / max(stage_count, 1)
        correction_blend = (
            copy_correction * copy_share
            + notebook_correction * (1 - copy_share)
        )
        correction_deviation = np.clip(abs(correction_blend - 1.0) / 0.4, 0, 1)

        pipeline_risk_index = (
            0.3 * np.clip(file_size_mb / 2000, 0, 1)
            + 0.2 * np.clip(stage_count / 12, 0, 1)
            + 0.2 * (1 - parallel_ratio)
            + 0.15 * correction_deviation
            + 0.15 * (1 - network_quality)
        )

        correction_uncertainty = (
            abs(copy_correction - 1.0) + abs(notebook_correction - 1.0)
        ) / 2

        stage_parallelism_efficiency = 1 - (n_groups / max(stage_count, 1))

        row = [
            stage_count, copy_stages, notebook_stages,
            file_size_mb, row_count,
            n_groups, parallel_ratio,
            transform_count, agg_count,
            copy_correction, notebook_correction,
            resource_estimate_s, baseline_s,
            network_quality,
            complexity_encoded,
            data_complexity_score,
            pipeline_risk_index,
            correction_uncertainty,
            stage_parallelism_efficiency,
        ]
        return np.array(row, dtype=float).reshape(1, -1)

    @classmethod
    def predict(
        cls,
        resource_plan: dict,
        predictions: dict,
        plan: dict,
    ) -> dict:
        cls._ensure_loaded()

        try:
            X = cls._build_feature_row(resource_plan, predictions, plan)

            log_pred = cls._duration_model.predict(X)[0]
            predicted_total_s = max(60, int(np.expm1(log_pred)))

            outcome = cls._outcome_model.predict(X)[0]
            proba   = cls._outcome_model.predict_proba(X)[0]
            classes = cls._outcome_model.classes_
            confidence = float(max(proba))

            return {
                "predicted_total_s": predicted_total_s,
                "outcome": str(outcome),
                "confidence": round(confidence, 3),
                "class_probabilities": {
                    str(c): round(float(p), 3) for c, p in zip(classes, proba)
                },
                "source": "ml_model",
            }
        except Exception as exc:
            raise MLNotAvailable(f"ML inference failed: {exc}")