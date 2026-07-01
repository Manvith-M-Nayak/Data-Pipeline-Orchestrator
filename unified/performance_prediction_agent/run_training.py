"""
Improved training script — v2.

Key fixes vs v1:
  1. File size minimum lowered from 0.1 MB to 0.0001 MB (100 bytes).
     The lognormal generator previously clipped at 0.1 MB, meaning real
     pipelines with tiny test files (< 100 KB) were completely outside
     the training distribution. Now explicitly adds a 'tiny file' tier.
  2. Added 2000 dedicated tiny-file rows (< 0.01 MB) so the model has
     real signal for small files like yours (1.1 KB = 0.001 MB).
  3. baseline_s for tiny files correctly uses a smaller floor (30s)
     since ADF copy of a 1KB file takes ~30s, not the 60s+ floor
     that made sense for larger files.
  4. Kept everything else identical to v1 so the fix is targeted,
     not a wholesale rewrite.

Run this from inside your performance_prediction_agent/ folder:
    python3 run_training.py
"""

import json
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestClassifier
from sklearn.metrics import (
    balanced_accuracy_score, classification_report,
    mean_absolute_error, r2_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

np.random.seed(42)
os.makedirs("models", exist_ok=True)

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


# ── Dataset generation ────────────────────────────────────────────────────────
def _make_row(file_size_mb: float, rng_stage_max: int = 13) -> dict:
    """Generate one synthetic run row given a file size."""
    stage_count = np.random.randint(1, rng_stage_max)
    copy_stages = np.random.randint(0, stage_count + 1)
    notebook_stages = stage_count - copy_stages

    row_count = int(file_size_mb * np.random.uniform(800, 1500))

    if file_size_mb > 100 or stage_count > 5:
        complexity = "high"
    elif file_size_mb > 10 or stage_count > 2:
        complexity = "medium"
    else:
        complexity = "low"

    n_groups = np.random.randint(1, stage_count + 1)
    parallel_ratio = round(1 - (n_groups / stage_count), 3)

    copy_time = copy_stages * np.random.uniform(
        # Tiny files copy faster — ADF startup dominates
        20 if file_size_mb < 0.01 else 40,
        50 if file_size_mb < 0.01 else 90,
    )
    transform_count = np.random.randint(0, 6)
    agg_count = np.random.randint(0, 4)
    notebook_time = notebook_stages * (
        120 + transform_count * 3 + agg_count * 10 + (row_count / 50000)
    )

    # Smaller floor for tiny files — 30s is realistic for a 1KB CSV
    floor_s = 30 if file_size_mb < 0.01 else 60
    baseline_s = max(floor_s, (copy_time + notebook_time) * (1 - parallel_ratio * 0.4))

    copy_correction = np.random.normal(1.0, 0.18)
    notebook_correction = np.random.normal(1.0, 0.22)
    copy_share = copy_stages / max(stage_count, 1)
    correction_blend = (copy_correction * copy_share) + (notebook_correction * (1 - copy_share))
    resource_estimate_s = baseline_s * np.random.normal(1.0, 0.1)
    corrected_baseline_s = baseline_s * correction_blend

    network_quality = np.random.beta(a=5, b=2)
    network_penalty = (1 - network_quality) * 0.25
    correction_deviation = np.clip(abs(correction_blend - 1.0) / 0.4, 0, 1)

    risk_score = (
        0.28 * np.clip(file_size_mb / 2000, 0, 1)
        + 0.18 * np.clip(stage_count / 12, 0, 1)
        + 0.14 * (1 - parallel_ratio)
        + 0.18 * correction_deviation
        + 0.22 * network_penalty / 0.25
    )
    risk_score = np.clip(risk_score + np.random.normal(0, 0.06), 0, 1)

    actual_duration_s = max(
        floor_s,
        corrected_baseline_s
        * np.random.lognormal(0, 0.15)
        * (1.0 + risk_score * np.random.uniform(2.0, 5.0)),
    )

    outcome = (
        "failure" if risk_score >= 0.58
        else "slowdown" if risk_score >= 0.38
        else "success"
    )

    return {
        "stage_count": stage_count,
        "copy_stages": copy_stages,
        "notebook_stages": notebook_stages,
        "file_size_mb": round(file_size_mb, 6),  # keep precision for tiny files
        "row_count": row_count,
        "complexity": complexity,
        "n_execution_groups": n_groups,
        "parallel_ratio": parallel_ratio,
        "transform_count": transform_count,
        "agg_count": agg_count,
        "copy_correction": round(copy_correction, 3),
        "notebook_correction": round(notebook_correction, 3),
        "resource_estimate_s": round(resource_estimate_s, 1),
        "baseline_s": round(baseline_s, 1),
        "network_quality": round(network_quality, 3),
        "actual_duration_s": round(actual_duration_s, 1),
        "outcome": outcome,
    }


def generate_dataset() -> pd.DataFrame:
    rows = []

    # ── Main distribution: 20,000 rows (same as before) ──────────────────
    for _ in range(20000):
        file_size_mb = np.clip(
            np.random.lognormal(mean=2.7, sigma=2.1), 0.1, 20000
        )
        rows.append(_make_row(file_size_mb))

    # ── Tiny file tier: 2,000 dedicated rows (NEW) ────────────────────────
    # Covers the range 0.0001 MB (100 bytes) to 0.1 MB (100 KB)
    # so the model has real signal for test/sample files like yours (0.001 MB).
    for _ in range(2000):
        file_size_mb = np.random.uniform(0.0001, 0.1)
        rows.append(_make_row(file_size_mb, rng_stage_max=5))  # tiny files = simpler pipelines

    return pd.DataFrame(rows)


# ── Generate ──────────────────────────────────────────────────────────────────
print("Generating dataset...")
df = generate_dataset()
print(f"Total rows: {len(df)}")
print("\nOutcome distribution:")
print(df["outcome"].value_counts())
print("\nFile size coverage:")
print(f"  < 0.01 MB:  {(df.file_size_mb < 0.01).sum()} rows")
print(f"  < 0.1 MB:   {(df.file_size_mb < 0.1).sum()} rows")
print(f"  0.1–10 MB:  {((df.file_size_mb >= 0.1) & (df.file_size_mb < 10)).sum()} rows")
print(f"  > 10 MB:    {(df.file_size_mb >= 10).sum()} rows")


# ── Train ─────────────────────────────────────────────────────────────────────
encoder = LabelEncoder()
df["complexity_encoded"] = encoder.fit_transform(df["complexity"])

X = df[FEATURE_COLS]
y_dur = df["actual_duration_s"]
y_dur_log = np.log1p(y_dur)
y_out = df["outcome"]

X_train, X_test, yd_train, yd_test, ydl_train, ydl_test, yo_train, yo_test = train_test_split(
    X, y_dur, y_dur_log, y_out,
    test_size=0.2, random_state=42, stratify=y_out,
)

print(f"\nTrain: {len(X_train)} rows | Test: {len(X_test)} rows")

print("\nTraining duration regressor (log-target GradientBoosting)...")
reg = GradientBoostingRegressor(
    n_estimators=300, max_depth=4, learning_rate=0.05,
    subsample=0.8, random_state=42,
)
reg.fit(X_train, ydl_train)
yd_pred = np.expm1(reg.predict(X_test))
mae = mean_absolute_error(yd_test, yd_pred)
r2 = r2_score(yd_test, yd_pred)
print(f"  MAE: {mae:.1f}s   R²: {r2:.3f}")

# Spot check: what does the model predict for a tiny file like yours?
import warnings
warnings.filterwarnings("ignore")
tiny_sample = pd.DataFrame([{
    "stage_count": 2, "copy_stages": 1, "notebook_stages": 1,
    "file_size_mb": 0.0011,  # your actual file: 1.1 KB
    "row_count": 15,
    "n_execution_groups": 2, "parallel_ratio": 0.0,
    "transform_count": 2, "agg_count": 0,
    "copy_correction": 1.0, "notebook_correction": 1.0,
    "resource_estimate_s": 213.0, "baseline_s": 213.0,
    "network_quality": 0.7,
    "complexity_encoded": encoder.transform(["low"])[0],
}])
pred_log = reg.predict(tiny_sample[FEATURE_COLS])[0]
pred_s = int(np.expm1(pred_log))
print(f"\n  Spot check — your actual pipeline (0.0011 MB, 2 stages, 15 rows):")
print(f"  Model predicts: {pred_s}s   (actual was 144s)")

print("\nTraining outcome classifier (balanced class weights)...")
clf = RandomForestClassifier(
    n_estimators=300, max_depth=10, min_samples_leaf=3,
    class_weight="balanced", random_state=42, n_jobs=-1,
)
clf.fit(X_train, yo_train)
yo_pred = clf.predict(X_test)
bal_acc = balanced_accuracy_score(yo_test, yo_pred)
print(f"  Balanced accuracy: {bal_acc:.3f}")
print(classification_report(yo_test, yo_pred))

# ── Save ──────────────────────────────────────────────────────────────────────
joblib.dump(reg, "models/duration_regressor.pkl")
joblib.dump(clf, "models/outcome_classifier.pkl")
joblib.dump(encoder, "models/feature_encoder.pkl")

metrics = {
    "mae_seconds": round(mae, 2),
    "r2_score": round(r2, 4),
    "balanced_accuracy": round(bal_acc, 4),
    "spot_check_tiny_file": {
        "file_size_mb": 0.0011,
        "predicted_s": pred_s,
        "actual_was_s": 144,
    },
}
with open("models/metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)

print(f"\nDone. Models saved to models/")
print(f"sklearn version: {__import__('sklearn').__version__}")