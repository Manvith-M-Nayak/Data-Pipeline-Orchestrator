"""
ML-backed Performance Prediction — primary path.

Mirrors the Planner Agent's pattern exactly:
  - Primary:  trained model (RandomForest classifier + GradientBoosting
              regressor) loaded locally from .pkl files
  - Fallback: the original transparent formula (performance_agent.py),
              used only if the model files are missing or model inference
              throws an exception

This module does NOT replace performance_agent.py — it sits in front of
it. performance_agent.py remains untouched and fully functional as the
fallback, exactly the same role Groq plays for the Planner.

Models trained on a synthetic dataset (5000 simulated runs) because real
run history (manager_feedback.jsonl) currently has too few rows (<10) to
train anything meaningful. See training/generate_synthetic_data.py and
training/train_model.py for how the dataset and models were produced.
"""

import os
from typing import Dict, List, Optional

import joblib
import numpy as np
import pandas as pd

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
]


class MLNotAvailable(Exception):
    """Raised when model files are missing or fail to load — triggers fallback."""
    pass


class MLPredictor:
    """
    Loads the trained models once (on first use) and exposes a single
    predict() method that takes the same kind of inputs the formula-based
    agent already uses, so the caller (performance_agent.py / manager.py)
    doesn't need to know which path actually ran.
    """

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
    ) -> "pd.DataFrame":
        """
        Translate the same RunState-derived inputs the formula agent uses
        into the flat feature vector the model was trained on.

        Returned as a named-column DataFrame (not a bare ndarray) so the
        columns line up with the feature names the estimators were fitted
        with — otherwise sklearn warns ("X does not have valid feature
        names") and, on a version mismatch, can reorder/misread columns.
        """
        stages = plan.get("stages", [])
        allocations = resource_plan.get("allocations", [])
        execution_groups = resource_plan.get("execution_groups", [])

        stage_count = len(stages) or len(allocations)
        copy_stages = sum(1 for s in stages if s.get("type") == "copy") \
            or sum(1 for a in allocations if a.get("stage_type") == "copy")
        notebook_stages = stage_count - copy_stages

        file_size_mb = float(predictions.get("file_size_mb", 0) or 0)
# Keep full precision — don't round here, the model needs the raw value
        row_count    = int((plan.get("schema") or {}).get("row_count", 0) or 0)

        complexity = predictions.get("complexity", "medium")
        try:
            complexity_encoded = int(cls._encoder.transform([complexity])[0])
        except Exception:
            # Unknown complexity label at inference time — fall back to "medium"
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
        baseline_s = resource_estimate_s  # same critical-path number the formula agent computes

        # network_quality: the live system doesn't measure real Azure network/
        # cluster conditions at prediction time (that data doesn't exist yet).
        # Default to 0.7 — the dataset's mean-ish value — rather than 1.0,
        # so the model doesn't silently assume "perfect conditions" on every
        # real prediction. This is an honest placeholder until real telemetry
        # (e.g. from Monitor Agent) can feed this feature live.
        network_quality = float(predictions.get("network_quality", 0.7))

        row = [
            stage_count, copy_stages, notebook_stages,
            file_size_mb, row_count,
            n_groups, parallel_ratio,
            transform_count, agg_count,
            copy_correction, notebook_correction,
            resource_estimate_s, baseline_s,
            network_quality,
            complexity_encoded,
        ]
        return pd.DataFrame([row], columns=FEATURE_COLS).astype(float)

    @classmethod
    def predict(
        cls,
        resource_plan: dict,
        predictions: dict,
        plan: dict,
    ) -> dict:
        """
        Returns the same shape of result the formula agent produces for
        the fields the model can predict: predicted_total_s, outcome,
        confidence. Stage-level forecasts and rationale are NOT produced
        by the model (it predicts the plan as a whole) — the caller is
        expected to merge this with stage_forecasts from the formula
        agent's critical-path breakdown.

        Raises MLNotAvailable if models aren't loaded or inference fails.
        """
        cls._ensure_loaded()

        try:
            X = cls._build_feature_row(resource_plan, predictions, plan)

            # Duration: model was trained on log1p(duration), so back-transform
            log_pred = cls._duration_model.predict(X)[0]
            predicted_total_s = max(60, int(np.expm1(log_pred)))

            # Outcome + confidence (max class probability)
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