# Resource Agent — ML Sizing Model

The Resource Agent recommends the best compute **settings** for each pipeline stage
(`workers`, `DIU`, `peak memory`, `shuffle partitions`, `node type`) with a supervised
regression model, falling back to a transparent heuristic when the model is absent.

> Duration / runtime / SLA prediction is **not** here — it belongs to the Performance
> Prediction Agent. See `../../docs/RESPONSIBILITIES.md`.

## Layout

```
resource_agent/
├── ml/
│   ├── feature_spec.py   # SINGLE SOURCE OF TRUTH: FEATURE_COLS, TARGET_COLS, stage_features()
│   └── calibration.py    # demand→settings label policy, anchored to real telemetry
├── training/
│   ├── generate_resource_dataset.py   # writes the 500k-row training CSV
│   ├── train_resource_model.py        # trains locally (use inside the project venv)
│   └── kaggle_train_resource_model.ipynb   # self-contained Kaggle notebook (gen + train)
├── models/
│   ├── resource_models.pkl   # the trained bundle the agent loads at runtime
│   └── metrics.json          # training metrics
├── ml_predictor.py       # runtime inference (ResourceMLPredictor) + heuristic fallback
└── resource_agent.py     # the agent; predict_stage() overlays ML settings when available
```

## How the model is grounded in real data

Labels are produced by `ml/calibration.py`, whose constants come from the real telemetry in
`Datasets/Cleaned/` (see the module docstring):

- **job_runs_cleaned.csv** (779k Databricks runs) → per-core throughput + the CRON-vs-
  CONTINUOUS intensity spread.
- **pipeline_runs_cleaned.csv** (ADF copies) → target copy time per DIU.
- **queries_cleaned.csv** (60k SQL runs) → operation-cost ordering (`group_by` > `join` >
  `aggregation`).
- **dbquery_statistics_cleaned.csv** → CPU-seconds & I/O magnitudes → memory/shuffle blow-up.

We deliberately do **not** train on the existing `synthetic_resource_dataset.json`: its
labels are the Resource Agent's own heuristic output, so a model trained on it would just
clone the heuristic.

## Reproduce

### Option A — Kaggle (recommended for the 500k train)

1. (Optional) Upload `Datasets/Cleaned/*.csv` as a Kaggle dataset so the generator can read
   the real trigger mix.
2. Open `training/kaggle_train_resource_model.ipynb` in Kaggle, Run All.
3. From the notebook **Output**, download `resource_models.pkl` and `metrics.json` into
   `resource_agent/models/`.

### Option B — locally (inside the project venv)

```bash
cd unified
python -m resource_agent.training.generate_resource_dataset --rows 500000
python -m resource_agent.training.train_resource_model
```

## ⚠️ scikit-learn version pinning

joblib pickles are tied to the scikit-learn version that wrote them. The FastAPI app runs
**scikit-learn 1.6.1** (pinned in `requirements.txt`), and the Kaggle notebook installs the
same version. If the training and serving versions differ, `ResourceMLPredictor` raises
`MLNotAvailable` on load and the agent silently falls back to the heuristic — check
`GET /api/resource/model-info` to see which path is live.

## Verify

```bash
python -m resource_agent.examples.run_examples   # exercises ML path + all invariants
curl localhost:8000/api/resource/model-info       # {"ml_available": true, ...}
```
