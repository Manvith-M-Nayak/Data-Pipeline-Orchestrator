"""
Performance Prediction Model — Training Script v4 (correct real-run blending)

What's improved vs v3:
  1. Oversample weight is now DYNAMIC, not a fixed magic number. v3 used a
     flat 20x regardless of how much real data existed or how large the
     synthetic set was — with ~20 real rows against 105,000 synthetic rows,
     that put real data at under 0.4% of the training set, too dilute to
     move the model (this is why MAE barely moved: 223.94 -> 223.12 after
     blending 18 real runs). Now the multiplier is computed to target a
     fixed fraction of the FINAL training set (TARGET_REAL_FRACTION, default
     10%), capped at MAX_OVERSAMPLE to avoid the opposite failure mode —
     duplicating a handful of rows thousands of times, which would let the
     model memorize a few specific runs instead of learning anything
     general.
  2. FIXED A REAL TRAIN/TEST LEAKAGE BUG. v3 oversampled real rows BEFORE
     the train/test split, so identical duplicate copies of the same real
     run could land in both train and test — meaning "held-out" evaluation
     on those rows wasn't testing generalization, it was testing recall of
     rows the model had already seen verbatim. Now: synthetic data is split
     first, real rows are split independently (80/20) BEFORE oversampling,
     oversampling only touches the real-train portion, and real-test is
     added to the held-out set exactly once, unweighted. Evaluation is now
     honest.
  3. Requires MIN_REAL_ROWS_FOR_BLEND unique real rows before blending kicks
     in at all (default 15) — below that, a handful of noisy real rows at
     high oversample weight would let the model overfit to their specific
     quirks rather than learn anything generalizable. Below the minimum,
     training falls back to synthetic-only, same as before real data existed.

Carried over from v3:
  - 100,000 main synthetic rows + 5,000 dedicated tiny-file rows
  - Failure threshold at risk >= 0.48 (~5% failure rate, was 1.4%)
  - Richer derived feature set (data_complexity_score, pipeline_risk_index,
    correction_uncertainty, stage_parallelism_efficiency)
  - GradientBoostingRegressor (500 trees) + RandomForestClassifier (1000
    trees) + 5-fold CV on the classifier

Honest limitation, unchanged from v3: manager_feedback.jsonl only logs a
handful of coarse fields (stage_count, complexity, actual_duration_s, the
perf forecast) — not row_count, transform_count, agg_count, network_quality,
or the correction factors the synthetic generator uses. Real rows still get
those fields imputed from synthetic medians (see _build_real_feature_rows).
Once the Manager logs the full feature set, delete the imputation block and
pass those fields through directly.

Expected training time: 3-6 minutes on a Mac M-series.

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

# Real-run CSV exported by the Learning & Policy Update Agent before a
# retrain (see learning_policy_agent/retraining_manager.py: export_real_runs).
REAL_RUNS_CSV = os.path.join("data", "real_runs.csv")

# ── Dynamic real-data blending knobs ─────────────────────────────────────
# Below this many usable real rows, skip blending entirely — too few rows
# at any oversample weight risks memorizing their specific quirks instead
# of learning something general.
MIN_REAL_ROWS_FOR_BLEND = 15
# Target fraction of the FINAL training set that should be real-derived.
# 10% is enough to meaningfully pull the model without letting a small,
# lower-fidelity (imputed) sample dominate training.
TARGET_REAL_FRACTION = 0.10
# Hard ceiling on the oversample multiplier regardless of how few real rows
# exist, so a tiny real-train split (e.g. 12 rows) can't get duplicated into
# thousands of copies of the same few runs.
MAX_OVERSAMPLE = 500
# Fraction of real rows held out for testing, split BEFORE oversampling.
REAL_TEST_FRACTION = 0.2


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


def _build_real_feature_rows(synthetic_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map real_runs.csv (exported by the Learning Agent from
    manager_feedback.jsonl) onto FEATURE_COLS + actual_duration_s + outcome.

    real_runs.csv columns actually available:
        run_id, timestamp, pipeline_signature, stage_count, complexity,
        success, actual_duration_s, predicted_duration_s, prediction_source

    Everything else in FEATURE_COLS is imputed from synthetic_df's medians,
    grouped by complexity where possible (better than a single global
    median — a "high" complexity real run gets high-complexity synthetic
    medians for the fields we don't have, not low-complexity ones).

    Returns the RAW (not yet split, not yet oversampled) set of usable real
    rows — splitting and oversampling happen later, after the synthetic
    train/test split, to avoid leaking duplicate rows across the boundary.
    """
    if not os.path.exists(REAL_RUNS_CSV):
        return pd.DataFrame(columns=list(synthetic_df.columns))

    real = pd.read_csv(REAL_RUNS_CSV)
    real = real.dropna(subset=["actual_duration_s"])
    if real.empty:
        return pd.DataFrame(columns=list(synthetic_df.columns))

    # Fields we actually have from real runs
    KNOWN = {"stage_count", "complexity", "actual_duration_s"}
    IMPUTE_COLS = [c for c in FEATURE_COLS if c not in KNOWN and c != "complexity_encoded"]

    medians_by_complexity = synthetic_df.groupby("complexity")[IMPUTE_COLS].median()
    global_medians = synthetic_df[IMPUTE_COLS].median()

    built_rows = []
    for _, r in real.iterrows():
        complexity = r.get("complexity") if r.get("complexity") in ("low", "medium", "high") else "low"
        base = (
            medians_by_complexity.loc[complexity]
            if complexity in medians_by_complexity.index
            else global_medians
        ).to_dict()

        row = dict(base)
        row["complexity"] = complexity
        row["stage_count"] = int(r["stage_count"]) if not pd.isna(r.get("stage_count")) else int(base.get("stage_count", 2))
        row["actual_duration_s"] = float(r["actual_duration_s"])

        # copy/notebook split unknown for real rows — assume an even split,
        # consistent with how most 2-stage student pipelines are shaped.
        row["copy_stages"] = max(1, row["stage_count"] // 2)
        row["notebook_stages"] = max(0, row["stage_count"] - row["copy_stages"])

        # outcome: prefer the Manager's own success flag over a re-derived
        # risk score, since we don't have the fields the risk score needs.
        if "success" in r and not pd.isna(r["success"]):
            success = bool(r["success"]) if not isinstance(r["success"], str) else r["success"].lower() in ("true", "1")
            row["outcome"] = "success" if success else "failure"
        else:
            row["outcome"] = "success"

        built_rows.append(row)

    real_df = pd.DataFrame(built_rows)
    # column order must match synthetic_df for a clean concat
    return real_df.reindex(columns=synthetic_df.columns)


t_start = time.time()
print("\n" + "="*60)
print("STEP 1: Generating synthetic dataset (105,000 rows)...")
print("="*60)
synthetic_df = generate_dataset()
print(f"\nTotal rows: {len(synthetic_df)}")
print("\nOutcome distribution:")
print(synthetic_df["outcome"].value_counts())
print(f"\nFailure rate: {(synthetic_df.outcome=='failure').mean()*100:.1f}%  (target: ~5%)")
print("\nFile size coverage:")
print(f"  < 0.01 MB:  {(synthetic_df.file_size_mb < 0.01).sum():,} rows")
print(f"  < 0.1 MB:   {(synthetic_df.file_size_mb < 0.1).sum():,} rows")
print(f"  0.1-10 MB:  {((synthetic_df.file_size_mb >= 0.1) & (synthetic_df.file_size_mb < 10)).sum():,} rows")
print(f"  > 10 MB:    {(synthetic_df.file_size_mb >= 10).sum():,} rows")

print("\n" + "="*60)
print("STEP 2: Verifying logical correctness (synthetic data only)...")
print("="*60)
synthetic_df["is_risky"] = (synthetic_df["outcome"] != "success").astype(int)
synthetic_df["correction_deviation_check"] = (
    (synthetic_df["copy_correction"] * synthetic_df["copy_stages"]
     + synthetic_df["notebook_correction"] * synthetic_df["notebook_stages"])
    / synthetic_df["stage_count"].clip(lower=1)
).sub(1.0).abs()

print("\nCorrelation of features with risk (all should match expected sign):")
checks = {
    "stage_count": "+", "file_size_mb": "+", "parallel_ratio": "-",
    "network_quality": "-", "pipeline_risk_index": "+",
    "correction_deviation_check": "+", "data_complexity_score": "+",
}
all_pass = True
for col, expected in checks.items():
    corr = synthetic_df[col].corr(synthetic_df["is_risky"])
    actual_sign = "+" if corr > 0 else "-"
    status = "OK" if actual_sign == expected else "FAIL"
    if actual_sign != expected:
        all_pass = False
    print(f"  {col:30s}: {corr:+.3f}  {status}")
print(f"\nAll correctness checks passed: {all_pass}")
synthetic_df.drop(columns=["is_risky", "correction_deviation_check"], inplace=True)

print("\n" + "="*60)
print("STEP 3: Encoding + splitting synthetic data...")
print("="*60)
encoder = LabelEncoder()
synthetic_df["complexity_encoded"] = encoder.fit_transform(synthetic_df["complexity"])

X_syn = synthetic_df[FEATURE_COLS]
yd_syn = synthetic_df["actual_duration_s"]
ydl_syn = np.log1p(yd_syn)
yo_syn = synthetic_df["outcome"]

X_train, X_test, yd_train, yd_test, ydl_train, ydl_test, yo_train, yo_test = train_test_split(
    X_syn, yd_syn, ydl_syn, yo_syn,
    test_size=0.2, random_state=42, stratify=yo_syn,
)
print(f"Synthetic train: {len(X_train):,} rows | Synthetic test: {len(X_test):,} rows")

print("\n" + "="*60)
print("STEP 3.5: Blending real runs (Learning & Policy Update Agent)...")
print("="*60)
real_feature_df = _build_real_feature_rows(synthetic_df)
n_real = len(real_feature_df)
real_blend_info = {
    "real_rows_available": n_real,
    "real_rows_blended_train": 0,
    "real_rows_held_out_test": 0,
    "oversample_weight_used": 0,
    "skipped_reason": None,
}

if n_real == 0:
    real_blend_info["skipped_reason"] = f"no usable rows found at {REAL_RUNS_CSV}"
    print(f"  No usable real runs found at {REAL_RUNS_CSV} — training on synthetic data only "
          "(this is normal before the Learning Agent's first retrain trigger).")
elif n_real < MIN_REAL_ROWS_FOR_BLEND:
    real_blend_info["skipped_reason"] = (
        f"only {n_real} real row(s), need {MIN_REAL_ROWS_FOR_BLEND} minimum to blend safely"
    )
    print(f"  Found {n_real} real run(s), but need at least {MIN_REAL_ROWS_FOR_BLEND} before "
          "blending — too few rows would let the model overfit to their specific quirks "
          "instead of learning something general. Training on synthetic data only for now.")
else:
    # Encode complexity on the real rows using the SAME encoder fit on
    # synthetic data (never re-fit it — that would shift the label mapping).
    real_feature_df["complexity_encoded"] = encoder.transform(real_feature_df["complexity"])

    # Split real rows BEFORE oversampling — this is the fix for the v3 leak.
    # Duplicating first and splitting after let identical copies of the same
    # real run land in both train and test.
    real_train_df, real_test_df = train_test_split(
        real_feature_df, test_size=REAL_TEST_FRACTION, random_state=42,
    )

    # Dynamic oversample weight: solve for the multiplier that makes
    # real_train represent TARGET_REAL_FRACTION of the FINAL training set
    # (synthetic train + oversampled real train), capped at MAX_OVERSAMPLE.
    #   target = (w * n_real_train) / (n_synthetic_train + w * n_real_train)
    #   =>  w = target * n_synthetic_train / (n_real_train * (1 - target))
    n_real_train = len(real_train_df)
    n_syn_train = len(X_train)
    ideal_weight = (
        TARGET_REAL_FRACTION * n_syn_train
        / (n_real_train * (1 - TARGET_REAL_FRACTION))
    )
    oversample_weight = int(round(min(max(ideal_weight, 1), MAX_OVERSAMPLE)))

    real_train_oversampled = pd.concat(
        [real_train_df] * oversample_weight, ignore_index=True
    )

    # Fold the (oversampled) real train rows into the synthetic train split,
    # and the (unweighted, exactly-once) real test rows into the synthetic
    # test split.
    X_train = pd.concat([X_train, real_train_oversampled[FEATURE_COLS]], ignore_index=True)
    yd_train = pd.concat([yd_train, real_train_oversampled["actual_duration_s"]], ignore_index=True)
    ydl_train = pd.concat([ydl_train, np.log1p(real_train_oversampled["actual_duration_s"])], ignore_index=True)
    yo_train = pd.concat([yo_train, real_train_oversampled["outcome"]], ignore_index=True)

    X_test = pd.concat([X_test, real_test_df[FEATURE_COLS]], ignore_index=True)
    yd_test = pd.concat([yd_test, real_test_df["actual_duration_s"]], ignore_index=True)
    ydl_test = pd.concat([ydl_test, np.log1p(real_test_df["actual_duration_s"])], ignore_index=True)
    yo_test = pd.concat([yo_test, real_test_df["outcome"]], ignore_index=True)

    real_blend_info.update({
        "real_rows_blended_train": n_real_train,
        "real_rows_held_out_test": len(real_test_df),
        "oversample_weight_used": oversample_weight,
    })
    final_real_frac = (oversample_weight * n_real_train) / (n_syn_train + oversample_weight * n_real_train)
    print(f"  Found {n_real} real run(s): {n_real_train} for training, {len(real_test_df)} held out for testing")
    print(f"  Oversample weight: {oversample_weight}x -> real rows are ~{final_real_frac:.1%} of the final training set")
    print(f"  NOTE: real rows are feature-imputed (see _build_real_feature_rows "
          "docstring) — row_count/transform_count/agg_count/network_quality/"
          "correction factors are NOT from real telemetry, only stage_count, "
          "complexity, and actual_duration_s are.")

print(f"\nFinal train: {len(X_train):,} rows | Final test: {len(X_test):,} rows")
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
    "failure_rate_pct": round((yo_train == "failure").mean() * 100, 2),
    "spot_check_tiny_file": {"file_size_mb": 0.0011, "predicted_s": pred_s, "real_runs_s": "126-144"},
    "real_data_blend": real_blend_info,
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