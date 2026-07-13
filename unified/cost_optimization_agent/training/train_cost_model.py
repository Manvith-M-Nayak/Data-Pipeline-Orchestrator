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
    print(f"[split] train={len(idx_train):,}  test={len(idx_test):,}")

    regressors, metrics = (
        {},
        {"sklearn_version": sklearn.__version__, "rows": int(len(df)), "targets": {}},
    )

    for tgt in TARGET_COLS:
        y = df[tgt].values
        ytr, yte = y[idx_train], y[idx_test]

        reg = HistGradientBoostingRegressor(
            max_iter=400,
            max_depth=6,
            learning_rate=0.06,
            l2_regularization=2.0,
            random_state=42,
        )
        reg.fit(Xtr, ytr)
        train_pred = reg.predict(Xtr)
        test_pred = reg.predict(Xte)

        train_mae = mean_absolute_error(ytr, train_pred)
        train_r2 = r2_score(ytr, train_pred)
        test_mae = mean_absolute_error(yte, test_pred)
        test_r2 = r2_score(yte, test_pred)

        regressors[tgt] = reg
        entry = {
            "train_mae": round(float(train_mae), 3),
            "train_r2": round(float(train_r2), 4),
            "test_mae": round(float(test_mae), 3),
            "test_r2": round(float(test_r2), 4),
        }
        r2_gap = abs(train_r2 - test_r2)
        entry["r2_gap"] = round(float(r2_gap), 4)
        entry["overfitting_flag"] = bool(r2_gap > 0.05)

        if tgt in ("opt_workers", "opt_diu"):
            train_rp = np.clip(np.round(train_pred), 0, None)
            test_rp = np.clip(np.round(test_pred), 0, None)
            entry["train_exact"] = round(float((train_rp == ytr).mean()), 4)
            entry["test_exact"] = round(float((test_rp == yte).mean()), 4)
            entry["train_within1"] = round(
                float((np.abs(train_rp - ytr) <= 1).mean()), 4
            )
            entry["test_within1"] = round(float((np.abs(test_rp - yte) <= 1).mean()), 4)
        metrics["targets"][tgt] = entry

        flag = " ⚠ OVERFIT" if entry["overfitting_flag"] else ""
        exact_str = (
            f"  exact train={entry.get('train_exact')} test={entry.get('test_exact')}"
            if "train_exact" in entry
            else ""
        )
        within1_str = (
            f"  within1 train={entry.get('train_within1')} test={entry.get('test_within1')}"
            if "train_within1" in entry
            else ""
        )
        print(
            f"[train] {tgt:<24} "
            f"MAE train={entry['train_mae']:<8} test={entry['test_mae']:<8} "
            f"R2 train={entry['train_r2']} test={entry['test_r2']} "
            f"gap={r2_gap:.4f}{exact_str}{within1_str}{flag}"
        )

    node_clf = HistGradientBoostingClassifier(
        max_iter=300,
        max_depth=6,
        learning_rate=0.08,
        l2_regularization=1.0,
        random_state=42,
    )
    node_y = df[NODE_TARGET].values
    node_clf.fit(Xtr, node_y[idx_train])

    train_node_pred = node_clf.predict(Xtr)
    test_node_pred = node_clf.predict(Xte)
    train_bal_acc = balanced_accuracy_score(node_y[idx_train], train_node_pred)
    test_bal_acc = balanced_accuracy_score(node_y[idx_test], test_node_pred)

    node_r2_gap = abs(train_bal_acc - test_bal_acc)
    metrics["node_type"] = {
        "train_balanced_accuracy": round(float(train_bal_acc), 4),
        "test_balanced_accuracy": round(float(test_bal_acc), 4),
        "r2_gap": round(float(node_r2_gap), 4),
        "overfitting_flag": bool(node_r2_gap > 0.05),
    }
    flag = " ⚠ OVERFIT" if node_r2_gap > 0.05 else ""
    print(
        f"[train] {NODE_TARGET:<24} "
        f"balanced_accuracy train={train_bal_acc:.4f} test={test_bal_acc:.4f} "
        f"gap={node_r2_gap:.4f}{flag}"
    )

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
