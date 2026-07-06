# Performance Prediction Agent — README
<<<<<<< HEAD
 
**Milestone:** Current (formula-based agent, no ML yet)  
**Status:** Complete for the formula-first phase. ML model is future work.
=======
>>>>>>> main

---

## 1. What This Agent Does

<<<<<<< HEAD
The Performance Prediction Agent sits in **Phase 2 (pre-checks)** of the Central Manager's run lifecycle. It runs **before any Azure resources are touched** and answers three questions:
=======
The Performance Prediction Agent sits in **Phase 2 (pre-checks)** of the Central Manager's run lifecycle. It runs **before any Azure resources are touched** and answers:
>>>>>>> main

1. How long will this pipeline take in total?
2. Which stage is the bottleneck?
3. Will it succeed, slow down, or fail — and how confident are we?
<<<<<<< HEAD

It also flags whether the predicted runtime will breach the SLA target (default: 15 minutes), and reports throughput (MB/s and rows/s) when file size data is available.

**It does not use a trained ML model.** It uses a transparent formula built on the Resource Agent's estimates, corrected by historical run data. The ML upgrade is the next milestone.

---

## 2. Files Created / Modified

### New files (all inside `unified/`)
=======
4. Will it breach the SLA target (default: 15 minutes)?
5. What is the expected throughput in MB/s?

The agent has **two prediction paths**:

- **Primary:** a trained ML model (GradientBoosting regressor for duration + RandomForest classifier for outcome), loaded locally from `.pkl` files
- **Fallback:** a transparent formula based on critical-path duration and historical run data from `manager_feedback.jsonl`

This mirrors the Planner Agent's architecture exactly — model primary, API/formula fallback.

---

## 2. Complete File List

### New files added to `unified/`
>>>>>>> main

```
unified/
└── performance_prediction_agent/
<<<<<<< HEAD
    ├── __init__.py              # package exports
    ├── performance_agent.py     # all agent logic
    └── router.py                # FastAPI endpoints
=======
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
>>>>>>> main
```

### Modified files

| File | What changed |
|---|---|
<<<<<<< HEAD
| `central_manager_agent/manager.py` | Added `performance_prediction` field to `RunState`; added `predict_performance()` method; wired into `execute_run()` Phase 2 |
| `main.py` | Imported and registered the performance prediction router |
| `frontend/src/api.js` | Added `perfPrediction` export with `predict()` and `history()` calls |
| `frontend/src/App.jsx` | Added `PerformancePredictionTab` import, nav tab, and route |
=======
| `central_manager_agent/manager.py` | Added `performance_prediction` field to `RunState`; added `predict_performance()` method; wired into `execute_run()` Phase 2; changed `round(mb, 2)` → `round(mb, 6)` in `predict_resources()` |
| `main.py` | Imported and registered the performance prediction router at `/api/performance-prediction/` |
| `frontend/src/api.js` | Added `perfPrediction` export with `predict()` and `history()` calls |
| `frontend/src/App.jsx` | Added `PerformancePredictionTab` import, nav tab (`/performance`), and route |
>>>>>>> main
| `frontend/src/pages/PerformancePredictionTab.jsx` | New file — full dashboard tab |

---

<<<<<<< HEAD
## 3. How It Works — Step by Step

### 3.1 When it runs

Central Manager `execute_run()` Phase 2 calls agents in this order:

```
analyze_parallelism()
predict_resources()          ← Resource Agent
estimate_cost()
predict_performance()        ← THIS AGENT  ← runs here
```

### 3.2 Inputs

Everything it needs is already on `RunState` from earlier phases:

| Input | Source | Used for |
|---|---|---|
| `state.resource_plan` | Resource Agent | Per-stage `duration_s`, `execution_groups`, `estimated_total_s`, `correction_factors` |
| `state.predictions` | Manager Phase 2a | `complexity`, `file_size_mb`, `correction_factors` |
| `state.plan` | Planner Agent | Stage list (for schema / row count lookup) |
| `data/manager_feedback.jsonl` | Written by Manager Phase 5 after every run | Historical actual vs predicted durations |

### 3.3 Computation steps

**Step 1 — Critical path duration**

Does not naively sum all stage durations. Respects parallelism from `execution_groups`:

```
For each execution group → take the MAX duration stage in that group
Sum those maximums across all groups → baseline_s
```

Example: two sequential groups `[ingest=120s]` and `[transform=300s]` → baseline = 420s.  
If they were parallel → baseline = 300s (slowest wins).

**Step 2 — History adjustment factor**

Loads `manager_feedback.jsonl`. Filters to runs with matching `complexity` (low/medium/high). Falls back to all history if fewer than 5 complexity-matched runs. If total history is still below 5 runs, uses `1.0` (no correction — trust Resource Agent as-is).

With enough history:
```
ratios         = [actual_duration_s / predicted_duration_s] for last 10 runs
mean_ratio     = average of ratios
adj_factor     = 1.0 + (mean_ratio - 1.0) * 0.4
```

The `0.4` is a damping coefficient — it moves 40% toward observed history, not 100%, to prevent overcorrection from outlier runs.

**Step 3 — Adjusted total**

```
predicted_total_s = max(60, int(baseline_s * adj_factor))
```

**Step 4 — Per-stage forecasts**

Applies `adj_factor` to each stage's `duration_s` individually. Risk labels:
- `ok` → under 300s (5 min)
- `warning` → 300–600s (5–10 min)
- `high` → over 600s (10 min+)

Stage with highest adjusted duration = **bottleneck**.

**Step 5 — Outcome classification**

Compares `predicted_total_s` against the Resource Agent's own `estimated_total_s`:
```
ratio = predicted_total_s / resource_estimate_s

ratio >= 3.0  OR  historical failure rate > 50%  →  "failure"
ratio >= 1.6  OR  historical failure rate > 25%  →  "slowdown"
otherwise                                         →  "success"
```

Historical failure rate = proportion of recent runs where `assurance_passed = False`.

**Step 6 — Confidence score**

```
base             = 0.50   (formula only, no history)
history_boost    = min(history_runs / 10, 1.0) * 0.40
confidence       = min(base + history_boost, 0.90)
```

If Resource Agent correction factors deviate more than 0.4 from 1.0 (meaning the Resource Agent itself is uncertain), confidence drops by 0.10, floored at 0.30.

Max confidence is capped at **0.90** — never 100%, because it will always be a formula.

**Step 7 — Throughput**

```
throughput_mb_per_s   = file_size_mb / predicted_total_s     (if file_size_mb > 0)
throughput_rows_per_s = row_count    / predicted_total_s     (if row_count > 0)
```

`file_size_mb` comes from `state.predictions`. `row_count` comes from the plan schema. Both show `None` (displayed as `—`) if the data isn't available — this is normal on runs that fail before completion.

**Step 8 — SLA check**

```
sla_breach_risk = predicted_total_s > sla_target_s   (default: 900s = 15 min)
```

### 3.4 What happens after prediction

| Outcome | What Manager does |
|---|---|
| `"success"` | Run proceeds to Phase 3 (Execute) |
| `"slowdown"` | Run proceeds, warning logged in decisions |
| `"failure"` | Run **aborted immediately**. No Azure resources used. `state.error` set with rationale. |

Result stored on `state.performance_prediction` — available in the Manager state dict for the dashboard and for future agents (Cost Agent will read `predicted_total_s`).

---

## 4. API Endpoints

Both registered under `/api/performance-prediction/` in `main.py`.

### `POST /api/performance-prediction/predict`

Runs a prediction directly (without going through a full Manager run). Useful for testing.

**Request body:**
```json
{
  "resource_plan": { ... },   // output of ResourceAgent.analyze()
  "predictions":   { ... },   // Manager's state.predictions dict
  "plan":          { ... },   // raw Planner plan
  "sla_target_s":  900        // optional, default 900
}
```

**Response:**
```json
{
  "predicted_total_s":    420,
  "bottleneck_stage":     "Transform_Bronze_To_Silver",
  "outcome":              "success",
  "confidence":           0.54,
  "sla_breach_risk":      false,
  "sla_target_s":         900,
  "throughput_mb_per_s":  0.119,
  "throughput_rows_per_s": null,
  "stage_forecasts": [
    { "name": "Ingest_Raw_To_Bronze",       "predicted_s": 50,  "risk_level": "ok",  "is_bottleneck": false },
    { "name": "Transform_Bronze_To_Silver", "predicted_s": 151, "risk_level": "ok",  "is_bottleneck": true  }
  ],
  "history_runs_used":  1,
  "adjustment_factor":  1.0,
  "rationale":          "..."
=======
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
>>>>>>> main
}
```

### `GET /api/performance-prediction/history`

Returns last 50 entries from `manager_feedback.jsonl` that have both `actual_duration_s` and `predicted_duration_s`.

---

<<<<<<< HEAD
## 5. Dashboard Tab

Located at `frontend/src/pages/PerformancePredictionTab.jsx`. Accessible at `/performance` in the nav.

On load it makes two API calls:
1. `GET /api/performance-prediction/history` — for the history table
2. `GET /api/manager/runs` → then `GET /api/manager/status/<latest_run_id>` — to pull `performance_prediction` from the most recent Manager run

**What it shows:**

- **Outcome banner** — green/amber/red with the full rationale text
- **4 stat cards** — predicted runtime, outcome + confidence, SLA risk, adjustment factor
- **2 throughput cards** — MB/s and total data processed
- **Stage forecasts** — bar chart per stage, bottleneck tagged, risk-coloured
- **Key findings** — bottleneck stage, confidence, history runs used, adjustment factor
- **Prediction history table** — each past run with actual vs predicted ratio and pass/fail
- **How predictions are made** — static reference card explaining the methodology

---

## 6. Constants and Thresholds

All in `performance_agent.py`, easy to tune:

| Constant | Value | Meaning |
|---|---|---|
| `DEFAULT_SLA_TARGET_S` | 900 | 15 min SLA default |
| `SLOWDOWN_RATIO` | 1.6 | Prediction > 1.6× estimate → slowdown |
| `FAILURE_RATIO` | 3.0 | Prediction > 3.0× estimate → failure |
| `MIN_HISTORY_FOR_ML` | 5 | Runs needed before history correction activates |
| `DAMPING` | 0.4 | How aggressively history shifts the estimate |
| `STAGE_RISK_WARN_S` | 300 | Per-stage warning threshold (5 min) |
| `STAGE_RISK_HIGH_S` | 600 | Per-stage high-risk threshold (10 min) |

---

## 7. Data Flow Diagram

```
Planner Agent
     │
     ▼
Central Manager — execute_run()
     │
     ├── Phase 1: validate_plan()
     │
     ├── Phase 2: pre_checks
     │     ├── analyze_parallelism()
     │     ├── predict_resources()        ← Resource Agent
     │     │         │
     │     │         └── resource_plan → allocations, execution_groups
     │     │
     │     ├── estimate_cost()
     │     │
     │     └── predict_performance()     ← THIS AGENT
     │               │
     │               ├── reads: resource_plan, predictions, manager_feedback.jsonl
     │               ├── computes: critical path, adj_factor, outcome, confidence
     │               └── writes: state.performance_prediction
     │                         (used by dashboard + future Cost Agent)
     │
     ├── Phase 3: execute_with_retry()   ← only reached if outcome != "failure"
     ├── Phase 4: run_assurance()
     └── Phase 5: record_feedback()      ← writes to manager_feedback.jsonl
                                            (improves future predictions)
```

---

## 8. Learning Loop

Every completed run (pass or fail) writes to `data/manager_feedback.jsonl`:

```json
{
  "run_id":              "...",
  "actual_duration_s":   131.0,
  "predicted_duration_s": 201,
  "complexity":          "low",
  "assurance_passed":    true
}
```

The Performance Prediction Agent reads this file on every call. After **5 runs**, the adjustment factor starts moving away from 1.0. After **10 runs**, the history component reaches full weight. This means predictions improve automatically with use — no manual retraining required at the formula stage.

---

## 9. What Is NOT Done (Honest)

| Item | Status | Notes |
|---|---|---|
| ML model | Not started | Spec says "start with formula, then train model". Formula is done. ML is the next phase of this agent. |
| Throughput when run fails mid-way | Shows `—` | `file_size_mb` only propagates cleanly on successful runs. Code is correct; data just isn't there on failures. |
| Statistical confidence intervals | Not done | Confidence is a heuristic (0.5 base + history boost), not a statistically rigorous interval. |
| Throughput metric | Done | MB/s and rows/s computed and displayed. Shows `—` until a full successful run completes. |

---

## 10. What the Next Person Should Know

**To upgrade to an ML model:**  
Replace `_compute_adjustment()` in `performance_agent.py` with a trained model (sklearn, etc.) that takes `complexity`, `stage_count`, `file_size_mb`, `correction_factors` as features and predicts `actual/predicted` ratio. The training data is already accumulating in `data/manager_feedback.jsonl`. Everything else in the agent stays the same.

**To feed the Cost Agent:**  
`state.performance_prediction["predicted_total_s"]` and `state.performance_prediction["throughput_mb_per_s"]` are already on `RunState` after Phase 2. The Cost Agent just reads them.

**To change SLA target:**  
Pass `sla_target_s` as a parameter when calling `predict_performance(state, sla_target_s=<value>)` in `manager.py`.

**To change slowdown/failure thresholds:**  
Edit `SLOWDOWN_RATIO` and `FAILURE_RATIO` at the top of `performance_agent.py`.
=======
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
>>>>>>> main
