# Cost Optimization Agent

Answers one question: **"Can we run this pipeline for less money and still get it done properly and on time?"**

Primary path: a trained HistGradientBoosting model proposes compute configurations per stage. A conservative validator checks estimated runtime, existing memory requirements, aggregate limits and savings before recommending or applying a change. If validation fails or the model is unavailable, the agent keeps the original plan and returns no recommendations. The retained legacy rule helpers are not used by the public optimization paths.

## Safety rebuild (2026-09-29)

The bundled model was retrained on 30,000 feasible synthetic examples with 17 inputs, including `deadline_s` (0 means unconstrained). Impossible deadlines are excluded instead of mislabeled; integer targets no longer receive Gaussian noise. Copy labels now account for DIU-dependent runtime and an approximate DIU-hour charge. These assumptions are not validated production billing rates.

Use the model and code from the same revision: the former 16-feature bundle is incompatible. Verified runtime: Python 3.13, NumPy 2.2.6, scikit-learn 1.6.1. The Python 3.9 NumPy 2.0.2 compatibility branch in requirements has not been tested.

From the repository root, run `python retrain_cost_model.py`, `python test_cost_model_safety.py`, and `python audit_cost_model.py` using the compatible environment. Thirteen regression tests cover deadlines, memory, missing workload data, unchanged input, empty plans, pricing, copy DIUs, model loading, safe fallback and a valid saving candidate. The independent 5,000-example synthetic audit measured worker R² 0.962 and node balanced accuracy 99.39%; this is not production runtime validation. The broader pipeline integration test stopped at a missing FastAPI dependency in the isolated environment; full-pipeline compatibility remains unverified.

Runtime estimates are recalculated after changes. Missing stage timings cause rejection. Without a deadline, changes that increase estimated runtime are rejected. Aggregate resource validation assumes all stages might run concurrently, so it can reject otherwise valid sequential plans. Automatic heuristic application remains disabled pending equivalent validation.

## Layout

```
cost_optimization_agent/
├── ml/
│   └── feature_spec.py       # SINGLE SOURCE OF TRUTH: feature/target contract, cost/duration estimators, brute-force labeler
├── training/
│   ├── generate_cost_dataset.py   # writes the 200k-row training CSV (brute-force cost-minimization labels)
│   └── train_cost_model.py        # trains 4 regressors + 1 classifier, saves cost_models.pkl
├── models/
│   ├── cost_models.pkl       # trained bundle the agent loads at runtime
│   └── cost_metrics.json     # holdout metrics for each target
├── ml_predictor.py           # runtime inference: CostMLPredictor.predict_optimal_config()
├── cost_optimizer.py         # core agent: ML-first optimize() with 5 rule-based fallbacks
├── router.py                 # FastAPI endpoints: POST /optimize, POST /estimate, GET /node-rates
└── __init__.py               # exports CostOptimizationAgent, CostMLPredictor, etc.
```

## How it works

### Architecture

The agent receives the **Resource Agent's allocation plan** (what resources each stage needs), the **Performance Prediction Agent's forecast** (runtime, throughput), and the original **pipeline plan**, then:

1. **Cost Model** — estimates dollar cost from resource-hours using node pricing + DBU markup + ADF activity fee.
2. **ML Optimization (primary)** — loads `cost_models.pkl`, predicts cost-optimal (workers, node, shuffle) per stage, compares against current plan, emits aggregate saving.
3. **Legacy rule helpers (disabled in public paths)** — retained for reference; the public fallback makes no changes:
   - Cluster downsize (reduce workers when utilization is low)
   - Node downgrade (cheaper VM when memory headroom exists)
   - Off-peak scheduling (30% discount for non-critical, no-deadline jobs)
   - Merge tiny stages (<60s each, startup overhead dominates)
   - Shuffle tuning (target ~12 tasks per worker)
4. **Constraint Enforcement** — validates numerical candidate timing, memory and aggregate resources before emitting a recommendation.
5. **Ranking** — value-score orders recommendations (savings / risk / trade-off).

```
Resource Plan ─┐
Performance ───┼──→ CostOptimizationAgent.optimize() ──→ { estimated_cost, recommendations }
Plan ──────────┘
```

### ML Model

5 separate **HistGradientBoosting** models sharing 17 input features (16 workload features plus deadline):

| Model | Type | Predicts |
|---|---|---|
| `opt_workers` | Regressor | Optimal Databricks worker count (0–4) |
| `opt_diu` | Regressor | Optimal ADF DIUs (0–8) |
| `opt_memory_gb` | Regressor | Peak memory in GB |
| `opt_shuffle_partitions` | Regressor | Optimal shuffle partitions (snapped to tiers: 8, 16, 32, 64, 128, 200) |
| `opt_node_type` | Classifier | Cheapest adequate VM (6 SKUs) |

**Training labels** come from a **brute-force search**: for each feature vector, evaluate every feasible (workers, node, shuffle) combo against a calibrated throughput model, compute dollar cost, pick the cheapest. This is deterministic — same features always produce the same optimal config.

**Metrics** (on 6,000 held-out rows from the 30,000-row rebuild):

| Target | MAE | R² | Accuracy |
|---|---|---|---|
| `opt_workers` | 0.027 | 0.9672 | 99.27% exact |
| `opt_diu` | 0.008 | 0.9923 | 99.63% exact |
| `opt_memory_gb` | 1.061 | 0.9738 | — |
| `opt_shuffle_partitions` | 0.282 | 0.9807 | — |
| `opt_node_type` | — | — | 98.86% balanced |

These metrics measure agreement with a deterministic synthetic labeler. They do not demonstrate real-world feasibility or cost savings; runtime validation is still required for every candidate.

### Cost Model Assumptions

| Parameter | Value |
|---|---|
| Node rates | $0.14 – $0.56/hr per node (Azure pay-as-you-go) |
| DBU multiplier | 1.5 DBU/worker/hr × $0.55/DBU |
| ADF activity | $0.001 per copy stage |
| Storage | $0.018/GB/month, prorated by runtime |
| Off-peak discount | 30% |

All costs are **estimates**. No real billing data was available at build time. See `COST_MODEL_ASSUMPTIONS` in `cost_optimizer.py`.

## Reproduce

### Train the model

```bash
cd unified
python -m cost_optimization_agent.training.generate_cost_dataset --rows 200000
python -m cost_optimization_agent.training.train_cost_model
```

The trained bundle lands in `models/cost_models.pkl`. The training CSV is gitignored.

### Run the integration test

```bash
cd unified
python scripts/integration_test.py
```

Exercises the full pre-execution pipeline: Resource Agent → Performance Prediction → Cost Optimization (ML path) → Central Manager wiring. Exits non-zero on failure.

## Usage

### Library

```python
from cost_optimization_agent import CostOptimizationAgent

agent = CostOptimizationAgent()
result = agent.optimize(
    plan=plan,
    performance_prediction=perf_pred,
    resource_plan=resource_plan,
    constraints={"deadline_s": 900, "priority": "normal"},
)

print(f"Estimated cost: ${result['estimated_cost']['total_usd']:.4f}")
for r in result['recommendations']:
    print(f"  {r['change']}  save {r['estimated_saving']}  risk={r['risk_level']}  source={r['source']}")
```

### HTTP (FastAPI)

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/cost-optimization/optimize` | Run full optimization on a plan |
| POST | `/api/cost-optimization/estimate` | Cost estimate only (no recommendations) |
| GET | `/api/cost-optimization/node-rates` | Current node hourly rates |

## Output contract

### `POST /api/cost-optimization/optimize`

```jsonc
{
  "estimated_cost": {
    "compute_usd": 0.1142,
    "databricks_dbu_usd": 0.3364,
    "adf_usd": 0.001,
    "storage_usd": 0.00001,
    "total_usd": 0.4516
  },
  "recommendations": [
    {
      "change": "ingest: 2DIU->1DIU; transform: 1w->0w",
      "estimated_saving": "~74.8%",
      "trade_off": "negligible runtime impact",
      "reason": "ML model predicted cost-optimal config: ...",
      "new_cost": { "total_usd": 0.1137, ... },
      "risk_level": "low",
      "value_score": 24.93,
      "source": "ml"
    }
  ],
  "optimization_source": "ml_model"
}
```

## Integration with other agents

The Central Manager calls the Cost Optimization Agent as **Phase 2c** (the last pre-check before execution):

```
Planner
  → Assurance Agent (validate plan)
  → Resource Agent (predict resource needs)
  → Performance Prediction Agent (forecast runtime)
  → Cost Optimization Agent (suggest savings)    ← here
  → Executor Agent (run pipeline)
```

Boundary discipline: the **Resource Agent** decides *what's needed*. The **Cost Optimization Agent** decides *how to make it cheaper*. Neither steps on the other's territory. Recommendations are advisory — the Central Manager logs them alongside the cost estimate for audit.
