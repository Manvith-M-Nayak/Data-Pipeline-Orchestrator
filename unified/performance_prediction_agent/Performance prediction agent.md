# Performance Prediction Agent — README

---

## 1. What This Agent Does

The Performance Prediction Agent sits in **Phase 2 (pre-checks)** of the Central Manager's run lifecycle. It runs **before any Azure resources are touched** and answers:

1. How long will this pipeline take in total?
2. Which stage is the bottleneck?
3. Will it succeed, slow down, or fail — and how confident are we?
4. Will it breach the SLA target (default: 15 minutes)?
5. What is the expected throughput in MB/s?

The agent has **two prediction paths**:

- **Primary:** a trained ML model (GradientBoosting regressor for duration + RandomForest classifier for outcome), loaded locally from `.pkl` files
- **Fallback:** a transparent formula based on critical-path duration and historical run data from `manager_feedback.jsonl`

This mirrors the Planner Agent's architecture exactly — model primary, API/formula fallback.

---

## 2. Complete File List

### New files added to `unified/`

```
unified/
└── performance_prediction_agent/
    ├── __init__.py
    ├── performance_agent.py       ← all agent logic (formula + ML wiring)
    ├── ml_predictor.py            ← ML model loader and inference
    ├── router.py                  ← FastAPI endpoints
    ├── run_training.py            ← run this to regenerate .pkl files
    ├── models/
    │   ├── duration_regressor.pkl ← GradientBoosting (log-target) for duration
    │   ├── outcome_classifier.pkl ← RandomForest for success/slowdown/failure
    │   ├── feature_encoder.pkl    ← LabelEncoder for the 'complexity' column
    │   └── metrics.json           ← training metrics (MAE, R², balanced accuracy)
    └── data/
        └── synthetic_runs.csv     ← 22,000-row training dataset (synthetic)
```

### Modified files

| File | What changed |
|---|---|
| `central_manager_agent/manager.py` | Added `performance_prediction` field to `RunState`; added `predict_performance()` method; wired into `execute_run()` Phase 2; changed `round(mb, 2)` → `round(mb, 6)` in `predict_resources()` |
| `main.py` | Imported and registered the performance prediction router at `/api/performance-prediction/` |
| `frontend/src/api.js` | Added `perfPrediction` export with `predict()` and `history()` calls |
| `frontend/src/App.jsx` | Added `PerformancePredictionTab` import, nav tab (`/performance`), and route |
| `frontend/src/pages/PerformancePredictionTab.jsx` | New file — full dashboard tab |

---

## 3. How It Works

### 3.1 Where it runs in the Manager lifecycle

```
Phase 2 (pre_checks):
  analyze_parallelism()
  predict_resources()         ← Resource Agent
  estimate_cost()
  predict_performance()       ← THIS AGENT
      │
      ├── Try ML model first
      │     └── success → return ML result
      │
      └── MLNotAvailable raised → fall through to formula
```

If `outcome == "failure"`, the Manager **aborts the run immediately** before touching any Azure resources.

### 3.2 Primary path — ML model

**What it uses as input** (built from `RunState` fields already populated by Phase 2a):

| Feature | Source |
|---|---|
| `stage_count`, `copy_stages`, `notebook_stages` | `state.plan` |
| `file_size_mb` | `state.predictions` (from `csv_size_bytes / 1024 / 1024`, rounded to 6 decimal places) |
| `row_count` | `state.plan` schema |
| `complexity` | `state.predictions` (low/medium/high) |
| `n_execution_groups`, `parallel_ratio` | `state.resource_plan` execution_groups |
| `transform_count`, `agg_count` | counted from stage definitions in `state.plan` |
| `copy_correction`, `notebook_correction` | `state.resource_plan` correction_factors |
| `resource_estimate_s`, `baseline_s` | `state.resource_plan` estimated_total_s |
| `network_quality` | defaults to 0.7 (neutral assumption — no real telemetry yet) |

**What it produces:**
- `predicted_total_s` — duration model output (back-transformed from log space)
- `outcome` — classifier output: `success` / `slowdown` / `failure`
- `confidence` — max class probability from the classifier (0.0–1.0)
- `class_probabilities` — all three class probabilities
- `stage_forecasts` — per-stage durations distributed proportionally from the total prediction
- `bottleneck_stage` — stage with highest proportional share
- `sla_breach_risk` — `predicted_total_s > 900s`
- `throughput_mb_per_s` — `file_size_mb / predicted_total_s` (None if file size unknown)
- `prediction_source` — `"ml_model"` (tells dashboard which path ran)

### 3.3 Fallback path — formula

Used when model files are missing or sklearn raises an error. Computes:

1. **Critical-path duration** — sum of slowest stage per parallel execution group
2. **History adjustment** — loads `manager_feedback.jsonl`, computes `mean(actual/predicted)` for recent runs, dampens by 40%: `adj = 1.0 + (mean_ratio - 1.0) * 0.4`. Requires minimum 5 runs to activate; otherwise uses `1.0` (no correction)
3. **Outcome** — ratio of `predicted_total_s` to Resource Agent's `estimated_total_s`: ≥3.0× → failure, ≥1.6× → slowdown, else success. Also checks historical failure rate from `manager_feedback.jsonl`
4. **Confidence** — starts at 0.50, grows to max 0.90 as history accumulates: `0.50 + min(history_runs/10, 1.0) * 0.40`
5. `prediction_source` → `"formula"`

---

## 4. The ML Model — Honest Details

### What it is

Two scikit-learn models trained from scratch on synthetic data:

- `duration_regressor.pkl` — `GradientBoostingRegressor(n_estimators=300, max_depth=4, learning_rate=0.05, subsample=0.8)`, trained on `log1p(actual_duration_s)` because durations are right-skewed
- `outcome_classifier.pkl` — `RandomForestClassifier(n_estimators=300, max_depth=10, min_samples_leaf=3, class_weight="balanced")`, predicts success/slowdown/failure
- `feature_encoder.pkl` — `LabelEncoder` for the `complexity` categorical column

**This is NOT a fine-tuned LLM.** It's classical ML trained from scratch on tabular data. The task (predict a number from structured features) is a regression/classification problem, not a language problem — classical ML is the right tool.

### Training data — what it is and why it's synthetic

Real run history (`manager_feedback.jsonl`) currently has ~6 rows. A model needs hundreds of examples minimum. The synthetic dataset (22,000 rows) simulates realistic pipeline runs by encoding the same physics the formula agent uses, with added noise.

**Dataset composition:**
- 20,000 rows: general distribution, `file_size_mb` from 0.1 MB to 20,000 MB (log-normal)
- 2,000 rows: dedicated tiny-file tier, `file_size_mb` from 0.0001 MB to 0.1 MB

The tiny-file tier was added specifically because the general distribution clipped at 0.1 MB minimum, meaning real test pipelines (1–2 KB CSV files) were completely outside the training distribution and got wildly overestimated.

**Logical correctness checks (verified, not assumed):**

Every time `run_training.py` runs, it prints correlation signs. These were verified to be correct:

```
stage_count           : +0.270   (more stages = riskier)
file_size_mb          : +0.189   (bigger files = riskier)
parallel_ratio        : -0.179   (more parallel = safer)
correction_deviation  : +0.337   (uncertain estimates = riskier)
network_quality       : -0.266   (bad network = riskier)
raw copy_correction   : ~0.000   (correct — only deviation matters)
row count vs file size: 0.988    (near-linear, correct)
```

### Actual training metrics (sklearn 1.9.0, your venv)

```
Duration regressor:
  MAE:  224.7s
  R²:   0.868

Outcome classifier:
  Balanced accuracy: 0.656
  failure:   41% precision, 47% recall
  slowdown:  52% precision, 71% recall
  success:   90% precision, 78% recall
```

### What "network_quality = 0.7" means at inference time

The training data includes a `network_quality` feature (simulating Azure network/cluster variance). At inference time, the live system has no real network telemetry, so it defaults to 0.7 (the dataset mean). This is an honest placeholder — it means the model always assumes "slightly-better-than-average" conditions, which is a reasonable prior but not grounded in real Azure metrics.

### Prediction accuracy on real runs so far

| Run | Predicted | Actual | Ratio |
|---|---|---|---|
| First ML run | 425s | 144s | 2.95× (before tiny-file fix) |
| After fix | 199s | 126s | 1.58× |

The remaining 1.58× gap is expected — the model predicts for the average of the training distribution, and your real pipelines consistently finish faster than average (all 6 historical runs show actual < predicted). This gap will close as real run data accumulates and the model is retrained on it.

**Important: the formula fallback's history adjustment** (`adj_factor`) would normally correct this over time, but it only applies to the formula path, not the ML path. The ML path's predictions are fixed at training time — you must retrain to incorporate real run feedback.

---

## 5. Known Gaps and Honest Limitations


### The history adjustment doesn't apply to the ML path

The formula fallback's `adj_factor` reads `manager_feedback.jsonl` and corrects for real-world variance. The ML model does not — it predicts entirely from plan features. So even though you now have 6 runs showing a consistent 0.65× ratio, the ML model doesn't know about them. This is by design (ML should learn from retraining, not from a runtime correction factor), but it means the ML model will keep overestimating until it's retrained on real data.

### `throughput_mb_per_s` shows 0 or near-0

This is because `file_size_mb` for a 1.1 KB file is 0.000001 MB — the throughput comes out essentially zero even after the rounding fix. It's not a bug; it's just that throughput is meaningless at this file scale. It will show a real value once you run larger files.



---

## 6. API Endpoints

Both registered at `/api/performance-prediction/` in `main.py`.

### `POST /api/performance-prediction/predict`

Runs prediction directly without a Manager run. Useful for testing.

```json
Request:
{
  "resource_plan": { ... },   // ResourceAgent.analyze() output
  "predictions":   { ... },   // state.predictions dict
  "plan":          { ... },   // raw Planner plan
  "sla_target_s":  900        // optional, default 900
}

Response:
{
  "predicted_total_s": 199,
  "bottleneck_stage": "Transform_Bronze_To_Silver",
  "outcome": "success",
  "confidence": 0.86,
  "sla_breach_risk": false,
  "sla_target_s": 900,
  "throughput_mb_per_s": 0.0,
  "throughput_rows_per_s": null,
  "stage_forecasts": [...],
  "history_runs_used": 0,
  "adjustment_factor": 1.0,
  "rationale": "ML model prediction...",
  "prediction_source": "ml_model"
}
```

### `GET /api/performance-prediction/history`

Returns last 50 entries from `manager_feedback.jsonl` that have both `actual_duration_s` and `predicted_duration_s`.

---

## 7. Dashboard Tab

Located at `frontend/src/pages/PerformancePredictionTab.jsx`. Route: `/performance`.

On load makes two API calls:
1. `GET /api/performance-prediction/history`
2. `GET /api/manager/runs` → then `GET /api/manager/status/<latest_run_id>` to pull `performance_prediction` block

Shows: outcome banner with rationale, 4 stat cards (runtime, outcome+confidence, SLA risk, adjustment factor), 2 throughput cards, per-stage bar chart with bottleneck tagged, key findings, prediction history table, and a "How Predictions Are Made" reference card.

The rationale text is the quickest way to tell which path ran:
- `"ML model prediction (RandomForest classifier..."` → ML path ran
- `"Critical-path baseline from Resource Agent..."` → formula fallback ran

---

## 8. How to Retrain the Model

**Always retrain inside the project venv, not in Colab/Kaggle.** `.pkl` files are tied to the sklearn version they were trained with. Retraining in a different environment breaks them with `No module named '_loss'` or similar errors.

```bash
cd unified/performance_prediction_agent
source ../venv/bin/activate    # or however you activate your venv
python3 run_training.py
```

Then restart the server:
```bash
cd ..
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The script prints a spot-check prediction for your actual pipeline (0.0011 MB, 2 stages, 15 rows) so you can sanity-check the model before deploying it.

---

## 9. 

### Most important: retrain on real data once you have enough

Once `data/manager_feedback.jsonl` has 50-100+ real runs, retrain the model on those instead of (or blended with) the synthetic data. Real runs will correct the systematic overestimate because they reflect actual Azure/Databricks behavior on your infrastructure, not a simulation.

To blend real + synthetic:
```python
# In run_training.py, after generating synthetic df:
import json, os
real_records = []
with open('../data/manager_feedback.jsonl') as f:
    for line in f:
        r = json.loads(line.strip())
        if r.get('actual_duration_s') and r.get('predicted_duration_s'):
            real_records.append(r)
# Map real records to feature columns and append to df before training
```

### Connect `network_quality` to real Monitor Agent data

Right now `network_quality` defaults to 0.7 at inference. The Monitor Agent already tracks pipeline run times and anomalies. A simple improvement: compute `network_quality` from recent ADF pipeline run variance (low variance = high quality) and pass it into `MLPredictor._build_feature_row()` via `predictions["network_quality"]`.

### If the ML model keeps overestimating after retraining

Check `models/metrics.json` spot-check value. If the spot-check prediction is still > 2× actual, the issue is that `baseline_s` (the Resource Agent's estimate) is too high — the ML model's strongest feature is `baseline_s` (importance ~0.91), so if the Resource Agent overestimates, the ML model will too. Fix: improve the Resource Agent's duration constants for your specific Azure tier.

### To change SLA target

Pass `sla_target_s` when calling `predict_performance(state, sla_target_s=<value>)` in `manager.py`.

### To change slowdown/failure thresholds (formula path)

Edit `SLOWDOWN_RATIO` and `FAILURE_RATIO` at the top of `performance_agent.py`.

### To add the Cost Agent

`state.performance_prediction["predicted_total_s"]` is already on `RunState` after Phase 2. The Cost Agent reads it directly — no changes needed to this agent.

---

## 10. Dependency to Add to `requirements.txt`

```
scikit-learn>=1.9.0
joblib
```

These are needed to load the `.pkl` files. If they're already in your venv they're probably already in `requirements.txt` — check before adding duplicates.
