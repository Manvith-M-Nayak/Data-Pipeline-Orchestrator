"""
Performance Prediction Model — Training Script v3

What's improved vs v2:
  1. 100,000 rows (was 22,000) — more coverage, especially for edge cases
  2. 5,000 dedicated tiny-file rows (was 2,000) — better small-file predictions
  3. Failure threshold lowered (risk >= 0.48, was 0.58) — failures now ~5% of
     data instead of 1.4%, giving the model ~5,000 failure examples to learn
     from instead of ~286. This is the main driver of better failure recall.
  4. Richer feature set — added 4 new derived features that give the model
     more signal: data_complexity_score, pipeline_risk_index,
     correction_uncertainty, stage_parallelism_efficiency
  5. 1,000 estimators (was 300) — more trees = better ensemble, slower training
  6. 5-fold cross-validation on the classifier — proper evaluation, adds time,
     gives confidence intervals on balanced accuracy
  7. GradientBoostingRegressor with 500 estimators (was 300) for duration

Expected training time: 3-6 minutes on a Mac M-series.
Expected metrics improvement: failure recall ~0.60-0.70 (was 0.43)

Run from inside performance_prediction_agent/:
    python3 run_training.py
"""

import json
import os
import time

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestClassifier
from sklearn.metrics import (
    balanced_accuracy_score,
    classification_report,
    f1_score,
    mean_absolute_error,
    r2_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
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
    "data_complexity_score",
    "pipeline_risk_index",
    "correction_uncertainty",
    "stage_parallelism_efficiency",
]


def _make_row(file_size_mb: float, stage_max: int = 13) -> dict:
    stage_count = np.random.randint(1, stage_max)
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

    is_tiny = file_size_mb < 0.01
    copy_time = copy_stages * np.random.uniform(20 if is_tiny else 40, 50 if is_tiny else 90)
    transform_count = np.random.randint(0, 6)
    agg_count = np.random.randint(0, 4)
    notebook_time = notebook_stages * (
        120 + transform_count * 3 + agg_count * 10 + (row_count / 50000)
    )

    floor_s = 30 if is_tiny else 60
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

    data_complexity_score = np.log1p(file_size_mb) * np.log1p(row_count) / 100.0
    pipeline_risk_index = (
        0.3 * np.clip(file_size_mb / 2000, 0, 1)
        + 0.2 * np.clip(stage_count / 12, 0, 1)
        + 0.2 * (1 - parallel_ratio)
        + 0.15 * correction_deviation
        + 0.15 * (1 - network_quality)
    )
    correction_uncertainty = (abs(copy_correction - 1.0) + abs(notebook_correction - 1.0)) / 2
    stage_parallelism_efficiency = 1 - (n_groups / stage_count)

    risk_score = (
        0.28 * np.clip(file_size_mb / 2000, 0, 1)
        + 0.18 * np.clip(stage_count / 12, 0, 1)
        + 0.14 * (1 - parallel_ratio)
        + 0.18 * correction_deviation
        + 0.22 * network_penalty / 0.25
    )
    risk_score = np.clip(risk_score + np.random.normal(0, 0.06), 0, 1)

    if is_tiny:
        actual_duration_s = max(floor_s, corrected_baseline_s * np.random.lognormal(0, 0.10))
    else:
        actual_duration_s = max(
            floor_s,
            corrected_baseline_s
            * np.random.lognormal(0, 0.15)
            * (1.0 + risk_score * np.random.uniform(2.0, 5.0)),
        )

    if is_tiny:
        outcome = (
            "failure" if network_quality < 0.25
            else "slowdown" if network_quality < 0.45
            else "success"
        )
    else:
        outcome = (
            "failure" if risk_score >= 0.48
            else "slowdown" if risk_score >= 0.32
            else "success"
        )

    return {
        "stage_count": stage_count,
        "copy_stages": copy_stages,
        "notebook_stages": notebook_stages,
        "file_size_mb": round(file_size_mb, 6),
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
        "data_complexity_score": round(data_complexity_score, 4),
        "pipeline_risk_index": round(pipeline_risk_index, 4),
        "correction_uncertainty": round(correction_uncertainty, 4),
        "stage_parallelism_efficiency": round(stage_parallelism_efficiency, 3),
        "actual_duration_s": round(actual_duration_s, 1),
        "outcome": outcome,
    }


def generate_dataset() -> pd.DataFrame:
    rows = []
    print("  Generating 100,000 main rows...")
    for i in range(100000):
        file_size_mb = np.clip(np.random.lognormal(mean=2.7, sigma=2.1), 0.1, 20000)
        rows.append(_make_row(file_size_mb))
        if (i + 1) % 20000 == 0:
            print(f"    {i+1}/100000 rows done...")
    print("  Generating 5,000 tiny-file rows...")
    for _ in range(5000):
        file_size_mb = np.random.uniform(0.0001, 0.1)
        rows.append(_make_row(file_size_mb, stage_max=5))
    return pd.DataFrame(rows)


t_start = time.time()
print("\n" + "="*60)
print("STEP 1: Generating synthetic dataset (105,000 rows)...")
print("="*60)
df = generate_dataset()
print(f"\nTotal rows: {len(df)}")
print("\nOutcome distribution:")
print(df["outcome"].value_counts())
print(f"\nFailure rate: {(df.outcome=='failure').mean()*100:.1f}%  (target: ~5%)")
print("\nFile size coverage:")
print(f"  < 0.01 MB:  {(df.file_size_mb < 0.01).sum():,} rows")
print(f"  < 0.1 MB:   {(df.file_size_mb < 0.1).sum():,} rows")
print(f"  0.1-10 MB:  {((df.file_size_mb >= 0.1) & (df.file_size_mb < 10)).sum():,} rows")
print(f"  > 10 MB:    {(df.file_size_mb >= 10).sum():,} rows")

print("\n" + "="*60)
print("STEP 2: Verifying logical correctness...")
print("="*60)
df["is_risky"] = (df["outcome"] != "success").astype(int)
df["correction_deviation_check"] = (
    (df["copy_correction"] * df["copy_stages"] + df["notebook_correction"] * df["notebook_stages"])
    / df["stage_count"].clip(lower=1)
).sub(1.0).abs()

print("\nCorrelation of features with risk (all should match expected sign):")
checks = {
    "stage_count": "+", "file_size_mb": "+", "parallel_ratio": "-",
    "network_quality": "-", "pipeline_risk_index": "+",
    "correction_deviation_check": "+", "data_complexity_score": "+",
}
all_pass = True
for col, expected in checks.items():
    corr = df[col].corr(df["is_risky"])
    actual_sign = "+" if corr > 0 else "-"
    status = "OK" if actual_sign == expected else "FAIL"
    if actual_sign != expected:
        all_pass = False
    print(f"  {col:30s}: {corr:+.3f}  {status}")
print(f"\nAll correctness checks passed: {all_pass}")
df.drop(columns=["is_risky", "correction_deviation_check"], inplace=True)

print("\n" + "="*60)
print("STEP 3: Preparing features and train/test split...")
print("="*60)
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
print(f"Train: {len(X_train):,} rows | Test: {len(X_test):,} rows")
print(f"Failure examples in train: {(yo_train=='failure').sum():,}")
print(f"Failure examples in test:  {(yo_test=='failure').sum():,}")

print("\n" + "="*60)
print("STEP 4: Training duration regressor (GradientBoosting, 500 trees)...")
print("="*60)
t1 = time.time()
reg = GradientBoostingRegressor(
    n_estimators=500, max_depth=4, learning_rate=0.05,
    subsample=0.8, random_state=42,
)
reg.fit(X_train, ydl_train)
yd_pred = np.expm1(reg.predict(X_test))
mae = mean_absolute_error(yd_test, yd_pred)
r2 = r2_score(yd_test, yd_pred)
t_reg = time.time() - t1
print(f"  MAE:  {mae:.1f}s")
print(f"  R2:   {r2:.3f}")
print(f"  Time: {t_reg:.1f}s")

print("\nTop 7 features for duration prediction:")
importances = sorted(zip(FEATURE_COLS, reg.feature_importances_), key=lambda x: -x[1])
for name, imp in importances[:7]:
    print(f"  {name:35s}: {imp:.4f}")

import warnings
warnings.filterwarnings("ignore")
tiny_sample = pd.DataFrame([{
    "stage_count": 2, "copy_stages": 1, "notebook_stages": 1,
    "file_size_mb": 0.0011, "row_count": 15,
    "n_execution_groups": 2, "parallel_ratio": 0.0,
    "transform_count": 2, "agg_count": 0,
    "copy_correction": 1.0, "notebook_correction": 1.0,
    "resource_estimate_s": 203.0, "baseline_s": 203.0,
    "network_quality": 0.7,
    "data_complexity_score": np.log1p(0.0011) * np.log1p(15) / 100.0,
    "pipeline_risk_index": 0.2 * np.clip(2/12, 0, 1) + 0.15 * 0.3,
    "correction_uncertainty": 0.0,
    "stage_parallelism_efficiency": 0.0,
    "complexity_encoded": encoder.transform(["low"])[0],
}])
pred_s = int(np.expm1(reg.predict(tiny_sample[FEATURE_COLS])[0]))
print(f"\n  Spot check (your 1.1KB file, 2 stages, 15 rows):")
print(f"  Model predicts: {pred_s}s  |  Real runs: ~126-144s")

print("\n" + "="*60)
print("STEP 5: Training outcome classifier (RandomForest, 1000 trees)...")
print("="*60)
t2 = time.time()
clf = RandomForestClassifier(
    n_estimators=1000, max_depth=12, min_samples_leaf=2,
    class_weight="balanced",
    random_state=42, n_jobs=-1,
)
clf.fit(X_train, yo_train)
yo_pred = clf.predict(X_test)
bal_acc = balanced_accuracy_score(yo_test, yo_pred)
macro_f1 = f1_score(yo_test, yo_pred, average="macro")
report = classification_report(yo_test, yo_pred, output_dict=True)
t_clf = time.time() - t2
print(f"  Balanced accuracy: {bal_acc:.3f}")
print(f"  Macro F1:          {macro_f1:.3f}")
print(f"  Time: {t_clf:.1f}s")
print()
print(classification_report(yo_test, yo_pred))

print("Top 7 features for outcome classification:")
importances_clf = sorted(zip(FEATURE_COLS, clf.feature_importances_), key=lambda x: -x[1])
for name, imp in importances_clf[:7]:
    print(f"  {name:35s}: {imp:.4f}")

print("\n" + "="*60)
print("STEP 6: 5-fold cross-validation on outcome classifier...")
print("="*60)
t3 = time.time()
clf_cv = RandomForestClassifier(
    n_estimators=200, max_depth=12, min_samples_leaf=2,
    class_weight="balanced", random_state=42, n_jobs=-1,
)
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
cv_scores = cross_val_score(clf_cv, X_train, yo_train, cv=cv, scoring="balanced_accuracy", n_jobs=-1)
t_cv = time.time() - t3
print(f"  CV balanced accuracy: {cv_scores.mean():.3f} +/- {cv_scores.std():.3f}")
print(f"  Per-fold scores: {[round(s,3) for s in cv_scores]}")
print(f"  Time: {t_cv:.1f}s")

print("\n" + "="*60)
print("STEP 7: Saving models...")
print("="*60)
joblib.dump(reg, "models/duration_regressor.pkl")
joblib.dump(clf, "models/outcome_classifier.pkl")
joblib.dump(encoder, "models/feature_encoder.pkl")

metrics = {
    "duration_regressor": {"mae_seconds": round(mae, 2), "r2_score": round(r2, 4), "training_time_s": round(t_reg, 1)},
    "outcome_classifier": {
        "balanced_accuracy": round(bal_acc, 4), "macro_f1": round(macro_f1, 4),
        "cv_balanced_accuracy_mean": round(cv_scores.mean(), 4),
        "cv_balanced_accuracy_std": round(cv_scores.std(), 4),
        "classification_report": report, "training_time_s": round(t_clf, 1),
    },
    "feature_columns": FEATURE_COLS,
    "n_training_samples": len(X_train),
    "n_test_samples": len(X_test),
    "failure_rate_pct": round((df.outcome=="failure").mean()*100, 2),
    "spot_check_tiny_file": {"file_size_mb": 0.0011, "predicted_s": pred_s, "real_runs_s": "126-144"},
    "total_training_time_s": round(time.time() - t_start, 1),
}
with open("models/metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)

print("  models/duration_regressor.pkl  saved")
print("  models/outcome_classifier.pkl  saved")
print("  models/feature_encoder.pkl     saved")
print("  models/metrics.json            saved")
print("\n" + "="*60)
print(f"DONE. Total training time: {time.time()-t_start:.1f}s")
print(f"sklearn version: {__import__('sklearn').__version__}")
print("="*60)