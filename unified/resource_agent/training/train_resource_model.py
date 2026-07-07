#!/usr/bin/env python3
"""
Train the Resource Agent's resource-sizing models from the generated CSV.

Multi-target design (one light model per target so each can be tuned/inspected):
  * 4 × HistGradientBoostingRegressor → rec_workers, rec_diu, rec_memory_gb,
    rec_shuffle_partitions  (HistGB is fast and accurate on 500k tabular rows)
  * 1 × HistGradientBoostingClassifier → rec_node_type

All models share the FEATURE_COLS contract and are saved into a single bundle
(models/resource_models.pkl) alongside models/metrics.json.

    # generate the data first, then train:
    python -m resource_agent.training.generate_resource_dataset --rows 500000
    python -m resource_agent.training.train_resource_model

IMPORTANT: joblib pickles are tied to the scikit-learn version that wrote them.
Train with the SAME scikit-learn the FastAPI process uses (currently 1.7.0) or
the agent will fail to load the bundle and silently fall back to the heuristic.
The Kaggle notebook pins scikit-learn==1.7.0 for exactly this reason.
"""

import json
import os
import sys

try:  # Windows consoles default to cp1252 and choke on non-latin glyphs.
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import balanced_accuracy_score, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from resource_agent.ml.feature_spec import FEATURE_COLS, TARGET_COLS, NODE_TARGET  # noqa: E402

_HERE = os.path.dirname(__file__)
_MODEL_DIR = os.path.abspath(os.path.join(_HERE, "..", "models"))
_DEFAULT_CSV = os.path.join(_HERE, "resource_training.csv")


def train(csv_path: str = _DEFAULT_CSV, model_dir: str = _MODEL_DIR) -> dict:
    print(f"[train] loading {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"[train] {len(df):,} rows | sklearn {sklearn.__version__}")

    # Fit on plain numpy (no column names) so runtime inference on numpy feature
    # vectors doesn't emit sklearn "X has no valid feature names" warnings.
    X = df[FEATURE_COLS].values
    idx_train, idx_test = train_test_split(np.arange(len(df)), test_size=0.2, random_state=42)
    Xtr, Xte = X[idx_train], X[idx_test]

    regressors, metrics = {}, {"sklearn_version": sklearn.__version__, "rows": int(len(df)), "targets": {}}

    for tgt in TARGET_COLS:
        y = df[tgt].values
        reg = HistGradientBoostingRegressor(
            max_iter=400, max_depth=8, learning_rate=0.06,
            l2_regularization=1.0, random_state=42,
        )
        reg.fit(Xtr, y[idx_train])
        pred = reg.predict(Xte)
        yte = y[idx_test]
        mae, r2 = mean_absolute_error(yte, pred), r2_score(yte, pred)
        regressors[tgt] = reg
        entry = {"mae": round(float(mae), 3), "r2": round(float(r2), 4)}
        # For the integer targets, also report exact / within-1 accuracy.
        if tgt in ("rec_workers", "rec_diu"):
            rp = np.clip(np.round(pred), 0, None)
            entry["exact_acc"] = round(float((rp == yte).mean()), 4)
            entry["within1_acc"] = round(float((np.abs(rp - yte) <= 1).mean()), 4)
        metrics["targets"][tgt] = entry
        print(f"[train] {tgt:<24} MAE={entry['mae']:<8} R2={entry['r2']}"
              + (f"  within1={entry.get('within1_acc')}" if "within1_acc" in entry else ""))

    # Node-type classifier
    node_clf = HistGradientBoostingClassifier(
        max_iter=300, max_depth=8, learning_rate=0.08, random_state=42,
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
    bundle_path = os.path.join(model_dir, "resource_models.pkl")
    joblib.dump(bundle, bundle_path)
    with open(os.path.join(model_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"[train] saved bundle -> {bundle_path}")
    print(f"[train] saved metrics -> {os.path.join(model_dir, 'metrics.json')}")
    return metrics


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=_DEFAULT_CSV)
    ap.add_argument("--model-dir", default=_MODEL_DIR)
    args = ap.parse_args()
    train(args.csv, args.model_dir)
