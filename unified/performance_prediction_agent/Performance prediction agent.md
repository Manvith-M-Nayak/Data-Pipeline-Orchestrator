# Performance Prediction Agent — README
 
**Milestone:** Current (formula-based agent, no ML yet)  
**Status:** Complete for the formula-first phase. ML model is future work.

---

## 1. What This Agent Does

The Performance Prediction Agent sits in **Phase 2 (pre-checks)** of the Central Manager's run lifecycle. It runs **before any Azure resources are touched** and answers three questions:

1. How long will this pipeline take in total?
2. Which stage is the bottleneck?
3. Will it succeed, slow down, or fail — and how confident are we?

It also flags whether the predicted runtime will breach the SLA target (default: 15 minutes), and reports throughput (MB/s and rows/s) when file size data is available.

**It does not use a trained ML model.** It uses a transparent formula built on the Resource Agent's estimates, corrected by historical run data. The ML upgrade is the next milestone.

---

## 2. Files Created / Modified

### New files (all inside `unified/`)

```
unified/
└── performance_prediction_agent/
    ├── __init__.py              # package exports
    ├── performance_agent.py     # all agent logic
    └── router.py                # FastAPI endpoints
```

### Modified files

| File | What changed |
|---|---|
| `central_manager_agent/manager.py` | Added `performance_prediction` field to `RunState`; added `predict_performance()` method; wired into `execute_run()` Phase 2 |
| `main.py` | Imported and registered the performance prediction router |
| `frontend/src/api.js` | Added `perfPrediction` export with `predict()` and `history()` calls |
| `frontend/src/App.jsx` | Added `PerformancePredictionTab` import, nav tab, and route |
| `frontend/src/pages/PerformancePredictionTab.jsx` | New file — full dashboard tab |

---

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
}
```

### `GET /api/performance-prediction/history`

Returns last 50 entries from `manager_feedback.jsonl` that have both `actual_duration_s` and `predicted_duration_s`.

---

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