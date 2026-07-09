"""
ML-backed cost-optimal configuration prediction.

Primary path: trained model bundle (cost_models.pkl) loaded locally.
Fallback: rule-based heuristics in cost_optimizer.py.

The model recommends cost-optimal compute settings (workers, diu, memory,
shuffle, node_type) for a given stage profile.

Mirrors the Resource Agent's ml_predictor.py pattern exactly.
"""

import os
from typing import Optional

import numpy as np

from cost_optimization_agent.ml.feature_spec import (
    FEATURE_COLS,
    BOUNDS,
    snap_shuffle,
    NODE_HOURLY_RATES,
)
from resource_agent.ml.feature_spec import stage_features, features_to_vector
from resource_agent.resource_agent import NODE_SPECS, DEFAULT_NODE, MAX_WORKERS, MAX_DIU

_MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")
_BUNDLE_PATH = os.path.join(_MODEL_DIR, "cost_models.pkl")


class MLNotAvailable(Exception):
    pass


class CostMLPredictor:
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
            if list(cls._bundle.get("feature_cols", [])) != FEATURE_COLS:
                raise ValueError(
                    "model feature_cols do not match feature_spec.FEATURE_COLS"
                )
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
    def predict_optimal_config(
        cls,
        stage: dict,
        schema: dict,
        csv_size_bytes: int = 0,
        stage_index: int = 0,
        n_stages: int = 1,
    ) -> dict:
        """
        Predict the cost-optimal compute configuration for one stage.

        Returns {workers, diu, memory_gb, shuffle_partitions, node_type, source}.
        Raises MLNotAvailable if the model can't be used.
        """
        cls._ensure_loaded()
        try:
            feat = stage_features(stage, schema, csv_size_bytes, stage_index, n_stages)
            X = np.array(features_to_vector(feat), dtype=float).reshape(1, -1)

            reg = cls._bundle["regressors"]
            workers = int(
                np.clip(
                    round(float(reg["opt_workers"].predict(X)[0])),
                    *BOUNDS["opt_workers"],
                )
            )
            diu = int(
                np.clip(round(float(reg["opt_diu"].predict(X)[0])), *BOUNDS["opt_diu"])
            )
            memory = float(
                np.clip(reg["opt_memory_gb"].predict(X)[0], *BOUNDS["opt_memory_gb"])
            )
            shuffle = snap_shuffle(
                float(
                    np.clip(
                        reg["opt_shuffle_partitions"].predict(X)[0],
                        *BOUNDS["opt_shuffle_partitions"],
                    )
                )
            )
            node = str(cls._bundle["node_classifier"].predict(X)[0])
            if node not in NODE_SPECS:
                node = DEFAULT_NODE

            is_copy = feat["stage_is_copy"] == 1
            if is_copy:
                workers, node, shuffle = (
                    0,
                    DEFAULT_NODE,
                    BOUNDS["opt_shuffle_partitions"][0],
                )
                diu = max(1, diu)
                memory = round(diu * 1.5, 2)
            else:
                diu = 0
                workers = min(workers, MAX_WORKERS)

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
