"""
ML-backed resource sizing — the Resource Agent's primary recommendation path.

Mirrors the Performance Prediction Agent's ml_predictor pattern exactly:
  - Primary:  a trained model bundle (multi-target HistGradientBoosting) loaded
              locally from models/resource_models.pkl
  - Fallback: the Resource Agent's transparent heuristic (resource_agent.py),
              used whenever the bundle is missing or inference fails.

The model recommends compute SETTINGS only (workers / DIU / peak memory /
shuffle partitions / node type). Duration, runtime and SLA prediction live in
the Performance Prediction Agent — see RESPONSIBILITIES.md.
"""

import os
from typing import Optional

import numpy as np

from .ml.feature_spec import (
    FEATURE_COLS, BOUNDS, snap_shuffle, stage_features, features_to_vector,
)
from .resource_agent import NODE_SPECS, DEFAULT_NODE, MAX_WORKERS, MAX_DIU

_MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")
_BUNDLE_PATH = os.path.join(_MODEL_DIR, "resource_models.pkl")


class MLNotAvailable(Exception):
    """Raised when the model bundle is missing or inference fails — triggers fallback."""
    pass


class ResourceMLPredictor:
    """
    Loads the trained bundle once and recommends settings for a single stage.
    Kept API-compatible with the Resource Agent's heuristic so the caller never
    needs to know which path ran.
    """

    _bundle = None
    _load_attempted = False
    _load_error: Optional[str] = None

    @classmethod
    def _ensure_loaded(cls):
        if cls._load_attempted:
            if cls._bundle is None:
                raise MLNotAvailable(cls._load_error or "bundle not loaded")
            return
        cls._load_attempted = True
        try:
            import joblib
            cls._bundle = joblib.load(_BUNDLE_PATH)
            # Minimal contract check: feature order must match what we build.
            if list(cls._bundle.get("feature_cols", [])) != FEATURE_COLS:
                raise ValueError("model feature_cols do not match feature_spec.FEATURE_COLS")
        except Exception as exc:
            cls._bundle = None
            cls._load_error = str(exc)
            raise MLNotAvailable(f"failed to load {_BUNDLE_PATH}: {exc}")

    @classmethod
    def is_available(cls) -> bool:
        try:
            cls._ensure_loaded()
            return True
        except MLNotAvailable:
            return False

    @classmethod
    def predict_settings(
        cls,
        stage: dict,
        schema: dict,
        csv_size_bytes: int = 0,
        stage_index: int = 0,
        n_stages: int = 1,
    ) -> dict:
        """
        Recommend compute settings for one stage.

        Returns {workers, diu, memory_gb, shuffle_partitions, node_type, source}.
        Raises MLNotAvailable if the model can't be used (caller falls back).
        """
        cls._ensure_loaded()
        try:
            feat = stage_features(stage, schema, csv_size_bytes, stage_index, n_stages)
            X = np.array(features_to_vector(feat), dtype=float).reshape(1, -1)

            reg = cls._bundle["regressors"]
            workers = int(np.clip(round(float(reg["rec_workers"].predict(X)[0])), *BOUNDS["rec_workers"]))
            diu     = int(np.clip(round(float(reg["rec_diu"].predict(X)[0])), *BOUNDS["rec_diu"]))
            memory  = float(np.clip(reg["rec_memory_gb"].predict(X)[0], *BOUNDS["rec_memory_gb"]))
            shuffle = snap_shuffle(float(np.clip(
                reg["rec_shuffle_partitions"].predict(X)[0], *BOUNDS["rec_shuffle_partitions"])))
            node = str(cls._bundle["node_classifier"].predict(X)[0])
            if node not in NODE_SPECS:
                node = DEFAULT_NODE

            # Gate targets by stage type so a copy never gets workers and a
            # notebook never gets DIU, regardless of tiny model wobble.
            is_copy = feat["stage_is_copy"] == 1
            if is_copy:
                workers, node, shuffle = 0, DEFAULT_NODE, BOUNDS["rec_shuffle_partitions"][0]
                diu = max(1, diu)
                memory = round(diu * 1.5, 2)
            else:
                diu = 0

            return {
                "workers": workers,
                "diu": diu,
                "memory_gb": round(memory, 2),
                "shuffle_partitions": int(shuffle),
                "node_type": node,
                "source": "ml_model",
            }
        except Exception as exc:
            raise MLNotAvailable(f"inference failed: {exc}")
