# Logic review — findings (2026-09-30)

Full read-through of `unified/` looking for logic errors, not lint or style. The
review followed the data flow: planner → assurance → manager → resource →
performance → cost → executor/notebooks → monitor/anomaly → learning → frontend.
Each finding says how it was verified:

- **Reproduced**: a script showed the wrong behaviour.
- **Simulated**: the maths was run numerically.
- **Code-read**: follows directly from the code, no runtime proof.

Line numbers are as of commit `6f8904c` plus the uncommitted scan fixes
(`calibration.py`, `semantic.py`, `planner_agent/__init__.py`).

**Nothing below has been fixed yet.**

## High

| # | Finding | Where | Evidence |
|---|---|---|---|
| H1 | **Container names are injected into notebook source unescaped.** `SOURCE_CONTAINER = "{source_container}"` (same for sink and checkpoint). `/api/manager/run` accepts the plan JSON from the client, so the planner's name sanitising is bypassed, and assurance and `validate_plan` don't check names. `/api/manager/stream/start` doesn't validate at all. The earlier filter-injection fix (#1) missed this path. | `executor_agent/notebook_builder.py:462-463, 713-715` | **Reproduced**: a crafted `source_container` produced valid Python running `__import__("os").system(...)`, and assurance returned `pass`. |
| H2 | **The plan never contains `schema` or `csv_size_bytes`, but three consumers read them from it.** The UI sends the schema as a separate form field. Effects: the Performance model always gets `row_count=0`, and the Cost Optimization ML candidate is always rejected (`feat["row_count"] <= 0`). So in real runs the cost agent never recommends or applies anything; the teammate's tests pass only because they add `schema` to the plan. | `performance_prediction_agent/ml_predictor.py:109`, `performance_agent.py:159`, `cost_optimization_agent/cost_optimizer.py:162,169-170` | **Reproduced** with a real plan from `adf_monitor.db` put through the manager's own pre-check steps: `('heuristic', 0 recommendations, not applied)`. |
| H3 | **The frontend sends a flat `{column: type}` map as the run schema**, not `{columns, row_count, size_hint}`, because `localStorage.last_csv_schema = result.columns`. Effects: the Resource ML model always gets `row_count=0, column_count=0`, and the anomaly detector never stores a schema, so `schema_drift` can never fire. That includes the per-pipeline drift work done earlier this session. | `frontend/src/pages/PlannerTab.jsx:169`, `ManagerTab.jsx:621`, `ExecutorTab.jsx:215` | **Reproduced**: `stage_features` gave `row_count=0, column_count=0` for the flat map vs `120000, 3` for the intended shape; two runs with different columns raised no drift event. |
| H4 | **Aggregation count is always 0 for real plans.** Performance and Resource (heuristic path) read `stage["aggregations"]["agg_exprs"]`, but the planner emits `stage["aggregation"]["aggregations"]`. The Performance model was trained on counts 0–3, so aggregation-heavy stages are under-predicted. The Resource ML `feature_spec` handles both shapes; these two places don't. | `performance_prediction_agent/ml_predictor.py:122-123`, `resource_agent/resource_agent.py:253-254` | **Reproduced**: a 2-aggregation stage gave `agg_count=0` in both. |
| H5 | **Failed and aborted runs pollute the Resource Agent's correction factor.** `record_feedback` runs on every exit path and calls `_record_resource_feedback` whenever a resource plan exists, including runs aborted before execution and runs that failed after retries. `get_correction_factor` has no success filter. (The learning agent's analyzer does filter these; the Resource Agent doesn't.) | `central_manager_agent/manager.py:1124, 1158`; `resource_agent/resource_agent.py:740-760` | **Reproduced on real data**: 11 of 49 `resource_feedback.jsonl` rows are from failed runs, with ratios 0.005 (aborted) and about 2.9 (retried). |
| H6 | **"Actual cost" doesn't depend on actual duration** (since teammate commit `ae954ec`). `_estimate_cost` bills each allocation by its *predicted* `duration_s`; `override_duration_s` is only used when that is missing. So `actual_cost_usd == estimate`, and cost learning is meaningless. The stored `estimated_cost_usd` includes the learned cost factor while "actual" doesn't, so the learner only sees 1/factor. | `cost_optimization_agent/cost_optimizer.py:350, 409` | **Reproduced**: actual durations of 100 s, 210 s and 5,000 s all gave `$0.055375`. |
| H7 | **No isolation between concurrent runs.** Batch runs purge every container, and container names default to the fixed `raw`/`bronze`/`silver`. There is one ADF pipeline (`Orchestrator_Copy_Pipeline`) and its datasets are overwritten per run. Notebooks live at `/Shared/unified_orchestrator/<stage name>`, and every stream stage is named `Stream_Ingest_Transform`. Two runs, or two stream ticks, at once can wipe each other's data or run each other's notebook, and container names are baked into the notebook. Nothing serialises runs. | `executor_agent/executor.py:749, 774-775`; `planner_agent/planner_common.py:164` | **Code-read** (no concurrent cloud run executed). |
| H8 | **The Resource Agent's sizing and cost auto-apply never reach execution.** The executor reads only each plan stage's `diu` and `shuffle_partitions`. Cluster size is an existing cluster or serverless, and `num_workers`/`node_type` are ignored. The "COST AUTO-APPLY: resource_plan updated" log and the post-apply cost estimates describe a configuration that isn't used. | `executor_agent/executor.py` (only `stage["diu"]`, `stage["shuffle_partitions"]` used); `central_manager_agent/manager.py:776-843` | **Code-read**: grep shows no read of `resource_plan`, `allocations`, `num_workers` or `node_type` in the executor. |

## Medium

| # | Finding | Where | Evidence |
|---|---|---|---|
| M1 | **Resource correction converges to the wrong value.** The ratio is measured against the *already-corrected* prediction (`alloc.duration_s = raw × cf`), but the new factor is applied to the raw heuristic. | `resource_agent/resource_agent.py:740-760` + `manager.py:1158` | **Simulated**: true 0.5 → converges to 0.809; true 2.0 → 1.281. |
| M2 | **The learning agent's correction factors converge to √(true ratio).** The target is actual ÷ `perf_predicted_total_s`, which is already corrected, but the factor is applied to raw ML output. Same for the cost factor. `perf_uncorrected_total_s` and `cost_uncorrected_estimated_usd` are already logged and should be used instead. | `learning_policy_agent/feedback_collector.py:144`, `policy_engine.py:248` + update site | **Simulated** with `PolicyEngine._gradual`: 0.5 → 0.707, 2.0 → 1.414. |
| M3 | **"Actual duration" is the whole managed run**: validation, the semantic LLM check (≤120 s), pre-checks, retry backoff (10 s + 30 s) and post-assurance. It is compared with execution-only predictions in feedback, learning, assurance timing, monitor duration and anomaly baselines. | `central_manager_agent/manager.py:1200` (`t0`) and every `time.time() - t0` | **Code-read**. |
| M4 | **Cost ML rejects every copy-stage DIU reduction.** Copy "memory" is just `diu × 1.5`, a derived number rather than a real requirement, so fewer DIUs always gives capacity below current "memory". Even with H2 fixed, copy stages can't be optimised. | `cost_optimization_agent/cost_optimizer.py:179-181` | **Reproduced**: traced with `settrace` → rejected at the capacity check (model suggested DIU 1: capacity 1.5 < 3.0). |
| M5 | **ML models are cached per process with no invalidation.** A retrained Performance model marked "deployed" isn't used until restart, and a failed load (e.g. during a retrain write) is cached for good, so the model stays on the formula fallback until restart. Same pattern in the Resource and Cost predictors. | `performance_prediction_agent/ml_predictor.py:54-64`; `resource_agent/ml_predictor.py:41-59`; `cost_optimization_agent/ml_predictor.py:35-40` | **Code-read**: nothing resets `_load_attempted`. |
| M6 | **Frontend 404/410 handling never triggers.** It checks `msg.startsWith("404")`, but `api.js` `req()` throws the backend `detail` (`"Run not found"`). A missing run polls forever and the UI stays "running". | `frontend/src/pages/ManagerTab.jsx:558`, `ExecutorTab.jsx:179` | **Code-read** against `api.js` `req()` and `router.py` `HTTPException(404, "Run not found")`. |
| M7 | **`/api/schema/detect` returns 500 on a ragged CSV** (a row with fewer fields than the header). `DictReader` fills `None`, and `_infer_type` calls `v.strip()` on it. | `main.py:220` | **Reproduced**: HTTP 500. |
| M8 | **Streaming endpoints skip every validation.** `stream/start` sends the client config straight to the executor (no `validate_plan`, no assurance), which widens H1. Stream ticks are never recorded to the monitor or feedback. | `central_manager_agent/router.py` stream routes; `stream_manager.py` | **Code-read**. |
| M9 | **The formula-path performance prediction aborts runs based on other pipelines.** Outcome is "failure" (so the manager aborts before executing) if more than half of the last 10 runs of *any* pipeline failed post-assurance, and the history-adjustment factor includes aborted and retried runs. It clears after about one aborted run. | `performance_prediction_agent/performance_agent.py:333-361, 444-458` | **Simulated**: lock lasted 1 abort. |

## Low

- **Monitor run-id collisions**: `run_tag = int(time.time())` uses seconds, so `dbx-<tag>` ids collide for runs or ticks starting in the same second. That id is the primary key in the monitor and `run_metrics`. (`executor_agent/executor.py:683`)
- **Stream drops can overwrite each other**: `stream_manager.add_data` blob names use seconds too, so two drops of the same filename in the same second overwrite.
- **Assurance column check can miss errors**: references are checked against columns created anywhere in the stage (order-insensitive), and an aggregation alias counts as "known" before the stage's filter is checked. (`assurance_agent/structural.py`)
- **Combined analytics numbers are skewed**:
  - the duration ratio includes failed or aborted runs;
  - `feasible_plans` counts any run with a `complexity` value;
  - `anomalies_detected` counts the legacy `anomaly_log`, not `anomaly_events`;
  - manager runs are capped by `limit` while feedback isn't.
  
  (`central_manager_agent/combined.py`)
- **`validate_plan` checks a flag that's never there**: it reads `plan.used_fallback`, but the planner returns `used_fallback` outside the config, so it's never set.
- **Deterministic failures are retried**: e.g. "Plan cannot be compiled to a notebook" still waits through 10 s + 30 s of backoff.
- **Contention spill ignores dependencies**: `resolve_contention` merges a spilled stage into the next group without checking dependencies. The executor doesn't use these groups, so it only affects predictions.
- **Duplicate and failing monitor analyses**:
  - `sync_historical` (after each run) and the poll loop can analyse the same run twice;
  - startup backfill calls ADF for `dbx-` ids, which always fails, so their analysis is never retried.
- **Assumes a single worker**: `mark_interrupted_manager_runs` would fail other workers' in-flight runs under `uvicorn --workers N`.
- **Learning cycle blocks the event loop**: it runs synchronously inside async `record_feedback`, and the `runs_since_cycle` counter is a non-atomic read-modify-write.
- **Deploy gate compares different test sets**: the retrain gate compares MAE across held-out sets that differ, because the real-run split changes.
- **Ollama planner ignores names without a count**: `ollama_planner` ignores `container_names` when `num_containers` is missing (the Groq planner applies them). The UI always sends both, so only direct API calls hit this.
- **Container creation failure isn't fatal**: `create_blob_container` only prints on failure; the run fails later with a less clear error.
- **Dead rule code in the cost agent**: `_apply_cluster_downsize`, `_apply_node_downgrade`, `_apply_shuffle_tuning` and `_enforce_constraints_single` are unused since the fail-closed change.

## Checked and found correct

- **Planner**:
  - redistribution preserves the transforms → filter → aggregation order;
  - `extract_prompt_stage_filters` indexing is right;
  - `size_hint` matching works with the real strings.
- **Assurance**: streaming plans are allowed (`stream` is in both configs); the orchestrator's pass/fail logic is right.
- **Notebooks**: run transforms → filter → aggregation → write. `group_by`, aliases and aggregation columns are identifier-validated. The filter fix (#1) holds.
- **Learning analyzer**: excludes failed runs (`success is False`), unlike the Resource Agent.
- **Frontend**: no page passes updater functions to the `AppContext` setters; the display pages guard their numeric formatting.

## Reviewed only lightly

- Planner dataset tools (`planner_agent/training/*`): executed and validated, not line-by-line reviewed.
- Resource/Cost dataset generators and trainers: executed successfully.
- `frontend` display components (charts, cards): checked for API field and number-formatting issues only.
