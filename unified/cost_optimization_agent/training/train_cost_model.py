"""
Train the Cost Optimization Agent's cost-optimal configuration models.

Multi-target design (mirrors Resource Agent's train_resource_model.py):
  * 4 x HistGradientBoostingRegressor -> opt_workers, opt_diu, opt_memory_gb,
    opt_shuffle_partitions
  * 1 x HistGradientBoostingClassifier -> opt_node_type

All models share the FEATURE_COLS contract and save into models/cost_models.pkl.

Usage:
    python -m cost_optimization_agent.training.generate_cost_dataset --rows 200000
    python -m cost_optimization_agent.training.train_cost_model
"""

import json
import os
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
)
from sklearn.metrics import balanced_accuracy_score, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from cost_optimization_agent.ml.feature_spec import (
    FEATURE_COLS,
    TARGET_COLS,
    NODE_TARGET,
)

_HERE = os.path.dirname(__file__)
_MODEL_DIR = os.path.abspath(os.path.join(_HERE, "..", "models"))
_DEFAULT_CSV = os.path.join(_HERE, "cost_training.csv")


def train(csv_path: str = _DEFAULT_CSV, model_dir: str = _MODEL_DIR) -> dict:
    print(f"[train] loading {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"[train] {len(df):,} rows | sklearn {sklearn.__version__}")

    X = df[FEATURE_COLS].values
    idx_train, idx_test = train_test_split(
        np.arange(len(df)), test_size=0.2, random_state=42
    )
    Xtr, Xte = X[idx_train], X[idx_test]

    regressors, metrics = (
        {},
        {"sklearn_version": sklearn.__version__, "rows": int(len(df)), "targets": {}},
    )

    for tgt in TARGET_COLS:
        y = df[tgt].values
        reg = HistGradientBoostingRegressor(
            max_iter=400,
            max_depth=8,
            learning_rate=0.06,
            l2_regularization=1.0,
            random_state=42,
        )
        reg.fit(Xtr, y[idx_train])
        pred = reg.predict(Xte)
        yte = y[idx_test]
        mae, r2 = mean_absolute_error(yte, pred), r2_score(yte, pred)
        regressors[tgt] = reg
        entry = {"mae": round(float(mae), 3), "r2": round(float(r2), 4)}
        if tgt in ("opt_workers", "opt_diu"):
            rp = np.clip(np.round(pred), 0, None)
            entry["exact_acc"] = round(float((rp == yte).mean()), 4)
            entry["within1_acc"] = round(float((np.abs(rp - yte) <= 1).mean()), 4)
        metrics["targets"][tgt] = entry
        print(
            f"[train] {tgt:<24} MAE={entry['mae']:<8} R2={entry['r2']}"
            + (
                f"  within1={entry.get('within1_acc')}"
                if "within1_acc" in entry
                else ""
            )
        )

    node_clf = HistGradientBoostingClassifier(
        max_iter=300,
        max_depth=8,
        learning_rate=0.08,
        random_state=42,
    )
    node_y = df[NODE_TARGET].values
    node_clf.fit(Xtr, node_y[idx_train])
    node_pred = node_clf.predict(Xte)
    bal_acc = balanced_accuracy_score(node_y[idx_test], node_pred)
    metrics["node_type"] = {"balanced_accuracy": round(float(bal_acc), 4)}
    print(f"[train] {NODE_TARGET:<24} balanced_accuracy={bal_acc:.4f}")

    bundle = {
        "feature_cols": FEATURE_COLS,
        "targets": TARGET_COLS,
        "node_target": NODE_TARGET,
        "regressors": regressors,
        "node_classifier": node_clf,
        "node_classes": [str(c) for c in node_clf.classes_],
        "sklearn_version": sklearn.__version__,
        "trained_rows": int(len(df)),
    }
    os.makedirs(model_dir, exist_ok=True)
    bundle_path = os.path.join(model_dir, "cost_models.pkl")
    joblib.dump(bundle, bundle_path)
    with open(os.path.join(model_dir, "cost_metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"[train] saved bundle -> {bundle_path}")
    return metrics


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=_DEFAULT_CSV)
    ap.add_argument("--model-dir", default=_MODEL_DIR)
    args = ap.parse_args()
    train(args.csv, args.model_dir)
