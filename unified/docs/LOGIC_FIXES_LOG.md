# Logic fixes log

Running record of fixes for the issues in
[LOGIC_REVIEW_FINDINGS.md](LOGIC_REVIEW_FINDINGS.md) (ids H1–H8, M1–M9, and
the Low list). The work is done in stages, one commit per stage. Each stage
records:
- what changed and why;
- how it was verified;
- anything left open or decided differently from the review.

| Stage | Scope | Findings | Status |
|---|---|---|---|
| 1 | Plan safety — name validation at every entry point | H1, M8 (security part) | Done |
| 2 | Schema plumbing — get the real schema to every agent | H2, H3, H4 | Done |
| 3 | Learning loops — what feeds the correction factors | H5, H6, M1, M2, M3, M9 | Done |
| 4 | Execution — concurrent-run isolation, executor issues | H7, H8, M4, run-id collisions, retries | Done |
| 5 | Learned expected duration — replaces the fixed time limit ("SLA") everywhere | design change (user request) | Done |
| 6 | User-facing bugs — model reload, missing-run detection, ragged CSV, streams, planner | M5, M6, M7, M8 (rest), 3 Low | Done |
| 7 | Remaining low-severity items | Low list (analytics, assurance order, contention, monitor duplicates, learning cycle, deploy gate, dead code, single-worker) | Done |

---

## Stage 1 — Plan safety

**Commit message:** `fix(security): validate container/stage/dataset names at every plan entry point`

### Problem (H1, and part of M8)

- **Unescaped names in generated code.** The notebook builder wrote container
  names straight into generated Python (`SOURCE_CONTAINER = "{name}"`).
- **Planner sanitising could be bypassed.** The Planner cleans names, but
  `POST /api/manager/run` and `POST /api/manager/stream/start` accept the plan
  JSON from the client, so a crafted plan skipped that step.
- **Nothing downstream checked names.** Neither the Assurance Agent nor
  `validate_plan` looked at them.
- **The review reproduced it.** A container name like
  `x"; __import__("os").system("id"); y = "` produced valid Python that would
  run on Databricks with the storage key in scope, and assurance returned `pass`.
- **Stage and dataset names were also unchecked.** They become Databricks
  workspace paths, job names and ADF REST URL segments.
- **The earlier filter-injection fix (#1) only covered filters.** This path was
  missed.

### Changes

| File | Change | Why |
|---|---|---|
| `executor_agent/plan_safety.py` *(new)* | `plan_safety_issues(config)` and `is_valid_container(name)`. Container: valid Azure name (3–63 chars, lowercase letters/digits, single hyphens, alphanumeric at both ends). Stage: `[A-Za-z0-9_-]`, ≤100 chars. Dataset: starts with a letter, then `[A-Za-z0-9_]`. Covers `containers_to_create`, `datasets[].name/container`, and each stage's `source/sink/checkpoint_container` and `source/sink_dataset`. | One definition of "safe name", shared by every entry point, so the checks can't drift apart. |
| `executor_agent/notebook_builder.py` | `_require_container()` guards source, sink and checkpoint names in both the batch and streaming builders; an invalid name raises `UnsupportedTransformError`. | The last line of defence: the builder can no longer produce code from an unsafe name, whoever called it. The executor already turns this error into a clean "Plan cannot be compiled" failure. |
| `executor_agent/executor.py` | `execute_pipeline` rejects unsafe plans before any cloud call. | Fail fast: no containers created, nothing uploaded. |
| `central_manager_agent/manager.py` | `validate_plan` adds any safety issues to its blocking issues. | A managed run fails in Phase 1 with a clear reason, and it shows in the run's decision log. |
| `central_manager_agent/stream_manager.py` | `start()` raises `ValueError` for unsafe plans; the router already maps that to HTTP 422. | Streams skipped all validation before (M8). |
| `planner_agent/planner_common.py` | New `_normalize_identifiers()`, called by `_structural_validate`: cleans stage and dataset names and updates `execution_order`, `execution_groups` and copy-stage dataset references. | The Planner must always produce plans that pass the new checks, even when the LLM puts spaces or punctuation in names. |

### Verification

- **The review's reproduction is rejected everywhere:**
  - both notebook builders;
  - `execute_pipeline` (before any cloud call);
  - `validate_plan` (`ok: False`);
  - stream `start()`.
- **Nothing valid is broken:** all 13 real plans in `data/adf_monitor.db` pass
  the new checks.
- **Messy LLM names are normalized:** `"Filter Rows!"` → `Filter_Rows` and
  `"DS Raw-Data"` → `DS_Raw_Data`, with references updated, and the result
  passes. The streaming conversion of that plan also passes.
- **Regressions:**
  - `scripts/integration_test.py` passes;
  - the teammate's `test_cost_model_safety.py` passes (13 tests);
  - the backend starts with no tracebacks, and all 25 GET endpoints return 200;
  - `ruff` (F, E9) is clean.

### Notes and decisions

- **Strict validation, not escaping.** Escaping names would still allow names
  Azure rejects, and they would then fail later with a vaguer error.
- **The rest of M8 is not in this stage.** Streams still skip Assurance, and
  stream ticks aren't recorded in the monitor. That is planned for stage 5.
- **A long sink name can make an invalid checkpoint name.** The streaming
  checkpoint name is `{sink}-chk`, so a user-chosen sink longer than 59
  characters produces a checkpoint name that is too long. That is now rejected
  with a clear error instead of failing inside Azure.

---

## Stage 2 — Schema plumbing

**Commit message:** `fix(schema): pass the real input schema to every agent and count aggregations consistently`

### Problems

- **H2: agents read a schema the plan never has.** The Performance and Cost
  agents read `plan["schema"]` and `plan["csv_size_bytes"]`, but the plan never
  contains them: the UI sends the schema as a separate form field. As a result:
  - the Performance model always got `row_count=0`;
  - the Cost ML candidate was always rejected (`row_count <= 0`), so in real
    runs the Cost agent never recommended or applied anything.
- **H3: the UI sent a bare column map.** It sent `{col: type}` (from
  `localStorage.last_csv_schema = result.columns`), not
  `{columns, row_count, size_hint}`. As a result:
  - the Resource model always saw 0 rows and 0 columns;
  - the anomaly detector never stored a schema, so `schema_drift` could never
    fire.
- **H4: aggregation counts were always 0.** The Performance model features and
  the Resource heuristic counted aggregations from
  `stage["aggregations"]["agg_exprs"]`, but the planner emits
  `stage["aggregation"]["aggregations"]`.

### Changes

| File | Change | Why |
|---|---|---|
| `schema_utils.py` *(new)* | `normalize_run_schema(raw, contents, filename)` always returns `{columns: {col: type}, row_count, size_hint, ...}`. It accepts the full shape, a flat `{col: type}` map, or a `columns` list. With the uploaded bytes it **measures** `row_count` (`count_rows`: CSV via `csv.reader`, JSON array, NDJSON) and `size_hint`. Also `size_hint_for(nbytes)`. | One shape for every consumer. Numbers measured from the file beat whatever a client sends, and older clients or saved browser state still work. |
| `central_manager_agent/router.py` | `/run` normalizes the schema using the uploaded file; `/stream/start` normalizes the given schema. | Every run and stream starts from the canonical shape. |
| `central_manager_agent/manager.py` | `RunState` gains `schema` and `csv_size_bytes`, set in `execute_run`. New `_agent_plan(state)` gives a **copy** of the plan with `schema` / `csv_size_bytes` added; used for Performance prediction, cost optimize, auto-apply and actual-cost estimation. | Satisfies the agents' existing contract (the one the teammate's tests use) without changing `state.plan`, which goes to the semantic LLM (more context) and the executor (doesn't need it). |
| `monitor_agent/services/runtime_predictor.py` | Passes the stored run schema and size to the Performance agent. | Monitor predictions get the same inputs as the manager's. |
| `main.py` | `/api/schema/detect` uses `size_hint_for`. | The size buckets are defined in one place. |
| `resource_agent/ml/feature_spec.py` | New public `stage_agg_count(stage)`, built on the existing helpers that accept both shapes. | One aggregation counter. |
| `resource_agent/resource_agent.py`, `performance_prediction_agent/ml_predictor.py` | Use `stage_agg_count`. | H4. |
| `frontend/src/pages/PlannerTab.jsx` | Saves `{columns, row_count, size_hint, file_format}` to `last_csv_schema` instead of just `columns`. | H3 at the source. The Manager and Executor tabs send whatever is stored. |

### Verification

- **Normalizer:**
  - a flat map plus a CSV with a quoted comma gives the right columns, `row_count 3` and `small (< 5MB)`;
  - the detect shape and the list shape are handled;
  - row counts from a JSON array, NDJSON and CSV are right.
- **H4:** a 2-aggregation stage now counts 2 in both the Performance features and the Resource heuristic (was 0).
- **H2/H3, using a real plan from `adf_monitor.db` with a flat-map schema, as a client sends it:**
  - the Resource ML model sees `(50000 rows, 3 columns)` (was `(0, 0)`);
  - Performance `row_count` is 50000 (was 0);
  - Cost features get `row_count 50000, csv_size_mb 0.69` (was 0);
  - `state.plan` is unchanged.
- **H3 drift:** with a temp DB, two runs of the same pipeline with different columns give `[[], ['schema_drift']]`. Drift never fired before.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - the backend has no tracebacks, and all 25 GET endpoints return 200;
  - `/api/schema/detect` and monitor predictions still work.

### Notes and decisions

- **The plan isn't changed.** The agents' `plan["schema"]` contract is kept and
  satisfied with a copy, because putting the schema into `state.plan` would also
  send it to the semantic LLM and persist it twice.
- **The Cost ML path still rarely applies anything.** Its inputs are right now,
  but the copy-stage DIU capacity check (M4) still rejects most candidates. That
  is fixed in stage 4.
- **Old runs are unchanged.** Runs saved before this change have no `schema` on
  their state, so monitor predictions for them fall back to `row_count=0`, as
  before.

---

## Stage 3 — Learning loops

**Commit message:** `fix(learning): train correction factors on raw predictions and executed runs only`

### Problems

| Id | Problem |
|---|---|
| H5 | Resource feedback was recorded for **every** run, including aborts and failures. `get_correction_factor` had no success filter. Real data: 11 of 49 rows came from failed runs, with ratios 0.005 and about 2.9. |
| M1 | The Resource ratio was measured against the **already-corrected** prediction, but the new factor was applied to the raw one, so it settled halfway (true 0.5 → 0.81). The fixed 50% damping was recomputed on every call, so it permanently under-corrected. |
| M2 | Same problem in the learning agent: the duration and cost factors targeted actual ÷ corrected prediction, so they converged to √(true ratio) (0.5 → 0.707). |
| M3 | "Actual duration" was the whole managed run (validation, semantic LLM ≤120 s, pre-checks, retry backoff), compared against execution-only predictions. |
| H6 | `estimate_actual_cost` ignored the actual duration: "actual" always equalled the estimate. |
| M9 | The formula-path "failure" verdict (which aborts the run) used post-run assurance results from **any** pipeline, and the history adjustment included aborted and retried runs. |

### Changes

| File | Change | Why |
|---|---|---|
| `central_manager_agent/manager.py` | **Execution time and feedback records:** <ul><li>Each Executor attempt is timed; a successful attempt sets `RunState.execution_s`.</li><li>`record_feedback(state, total_elapsed_s, ...)` logs `actual_duration_s = execution_s` (or elapsed time if the run never executed), plus `total_elapsed_s`, `executed` and `pipeline_key`.</li><li>Post-run assurance timing uses `execution_s`.</li></ul> **Resource feedback:** <ul><li>Recorded **only** for `completed` runs, using `execution_s`.</li><li>Passes the correction factor that was applied.</li></ul> | M3, H5. `pipeline_key` (the anomaly detector's identity) lets history checks compare the same pipeline. |
| `central_manager_agent/router.py` | The monitor's Databricks record and the anomaly detector use `execution_s` when the pipeline ran. | Same measure everywhere a duration is compared. |
| `resource_agent/resource_agent.py` | **Recording:** `record_actual(..., correction_factor=1.0)` stores `correction_factor` and `raw_predicted_duration_s`. **`get_correction_factor`:** <ul><li>skips rows with `success False`, and legacy rows whose run the manager logged as failed (cross-checked against `manager_feedback.jsonl`, so no data is deleted);</li><li>uses the ratio against the **raw** prediction;</li><li>returns the **median** of the last 10 ratios, bounded to [0.33, 3.0].</li></ul> The accuracy report uses the same function. | H5, M1. With raw ratios, the median is itself the right multiplier; permanent damping only under-corrects, and the median plus bounds resist outliers. |
| `learning_policy_agent/feedback_collector.py` | `normalize()` adds `raw_predicted_duration_s` (from `perf_uncorrected_total_s`) and `raw_estimated_cost_usd` (from `cost_uncorrected_estimated_usd`). Both fields were already logged. | M2 |
| `learning_policy_agent/error_analyzer.py` | `duration_ratio` and `cost_ratio` (what the factors target) use the **raw** values. `duration_ape` and `cost_ape` (MAPE) stay on the **corrected** values. | The factors converge to the true ratio, while MAPE still measures the error users actually saw. That is what the policy review compares before and after a change, so rollbacks keep working. |
| `cost_optimization_agent/cost_optimizer.py` | `estimate_actual_cost` scales each allocation's `duration_s` by actual ÷ `resource_plan.estimated_total_s` before costing. | H6. Allocation durations add up to the Resource estimate, so scaling them makes actual cost follow the real duration. |
| `performance_prediction_agent/performance_agent.py` | **History:** the formula path uses only executed runs (not `failed`, `executed` not false), and this pipeline's runs when at least 5 exist. **Failure rate:** uses only this pipeline's runs, and needs at least 3. New `_pipeline_key(plan)`. | M9 |

### Verification

- **M1, simulated with the real `ResourceAgent`** (temp feedback log): raw estimates 2× too long converge to **0.5**, and 2× too short to **2.0**. They were 0.809 and 1.281 before.
- **H5:** 6 legacy failed-run rows with ratio 0.01 alongside 4 good rows → factor 1.0; the bad rows are ignored.
- **M2, through the real `normalize` → `per_run_errors` → `PolicyEngine._gradual`:** converges to **0.5** and **2.0**. It was 0.707 and 1.414 before.
- **H6:** actual cost at 105 s, 210 s and 420 s gives $0.028, $0.055 and $0.110; the estimate is $0.055. Before, all three gave $0.055.
- **Manager flow, mocked executor and temp logs:**
  - A successful run logs `actual_duration_s = 0.3` (execution) against `total_elapsed_s = 2.5`. The total includes a simulated 1 s assurance step, and assurance timing uses 0.3.
  - A failed run is logged with `executed: false` and writes **no** Resource feedback rows.
  - New rows include `correction_factor` and `raw_predicted_duration_s`.
- **M9:** 8 failed-assurance runs of **another** pipeline → outcome `success`. The same records for **this** pipeline → `failure`.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean;
  - the backend has no tracebacks, and all 25 GET endpoints return 200;
  - the real feedback logs were not touched by the tests.

### Effect on live behaviour (expected)

- **Resource factors drop sharply.** On existing data they go from copy 1.032 / notebook 0.766 to **0.657 / 0.525**. Real stages run well under the raw heuristic estimates, and the old formula hid that by including failed runs and halving every correction. Duration estimates (and anything derived from them) will be noticeably shorter.
- **History baselines mix old and new measures for a while.** Anomaly `run_metrics` and monitor baselines hold older whole-run durations; new rows use execution time. The mixing fades as new runs replace the old window.
- **Pipeline history starts empty.** Legacy feedback has no `pipeline_key`, so the M9 failure check has no history for any pipeline until new runs are logged. That is the safe default: no aborts based on unrelated runs.

### Not changed

- The formula path's own history adjustment (`DAMPING = 0.4`) is left as is. It now acts only on the residual left after the (now correct) Resource correction, so its effect is small.

---

## Stage 4 — Execution

**Commit message:** `fix(executor): isolate concurrent runs and apply resource settings the executor controls`

### Problems

| Id | Problem |
|---|---|
| H7 | **No isolation between concurrent runs.** <ul><li>Batch runs purge every container, and container names default to fixed values (`raw`/`bronze`/`silver`).</li><li>One ADF pipeline, `Orchestrator_Copy_Pipeline`, is redefined by every run.</li><li>Notebooks sit at fixed `/Shared/unified_orchestrator/<stage>` paths, and every stream stage is `Stream_Ingest_Transform`.</li></ul> Two runs or stream ticks at once could wipe each other's data or run each other's notebook. |
| Low | `run_tag = int(time.time())`: runs starting in the same second shared `dbx-<tag>` (the monitor's primary key) and job names. |
| H8 | The Resource Agent's sizing and cost auto-apply never reached execution: the executor only reads each plan stage's `diu` / `shuffle_partitions`, but was handed the original plan. |
| M4 | Cost ML rejected every copy-stage DIU reduction: copy "memory" is just `diu × 1.5`, so a lower DIU always "failed" the capacity check. |
| Low | Deterministic failures (uncompilable or unsafe plan, missing references) were retried with 10 s + 30 s backoff. |
| Low | A failed container creation was only printed; the run failed later with a vaguer error. |

### Changes

| File | Change | Why |
|---|---|---|
| `executor_agent/executor.py` | **Split into two functions:** `execute_pipeline` is now a wrapper around the old body, renamed `_execute_pipeline`. <br>**Run tag:** unique, `"{epoch}{6 hex}"`. <br>**Resource lock:** `_ResourceLocks` holds every container the run touches, plus `adf-pipeline:Orchestrator_Copy_Pipeline` when it has copy stages. Runs sharing any resource wait (logged as "Waiting for another run using …"); disjoint runs, e.g. separate streams, still run in parallel. <br>**Notebooks:** go to a per-run folder `…/run-<tag>/<stage>`, deleted in a `finally` (`delete_workspace_path`) even on failure. | H7 and the id collision. The ADF pipeline name stays fixed because the monitor groups history by pipeline name, so copy runs serialize on it instead. |
| `executor_agent/executor.py` | Deterministic failures return `"retryable": False`: unsafe plan, no containers, missing dataset/container references, undefined datasets, uncompilable notebook. `create_blob_container` raises on any status other than 200/201/409. | Avoid pointless retries; fail with the real reason. |
| `central_manager_agent/manager.py` | `execute_with_retry` stops on `retryable: False` and reports the number of attempts actually made. | Retries only help transient failures. |
| `central_manager_agent/manager.py` | New `_execution_plan(state)`: a copy of the plan with the **final** resource plan applied where the executor can use it — copy-stage `diu` (1..`MAX_DIU`) and notebook/stream `shuffle_partitions`. Settings in the stage's `pinned_settings` are left alone. What was applied goes into `RunState.execution_settings` and a `RESOURCE SETTINGS APPLIED` log line, which says workers/node_type are advisory. The executor gets this copy; `state.plan` is unchanged. | H8: cost auto-apply and Resource sizing now affect DIU and shuffle. Workers and node type can't be set on serverless or an existing cluster (the `DATABRICKS_SPARK_VERSION`/`NODE_TYPE` settings aren't used anywhere), so they're labelled advisory instead of silently ignored. |
| `planner_agent/planner_common.py` | `apply_custom_settings` records `pinned_settings` on each stage it sets. `build_default_config` now goes through it too. | A user's explicit choice in the Planner must not be overridden by an agent's recommendation. |
| `cost_optimization_agent/cost_optimizer.py` | For copy stages, `_validated_ml_candidate` recomputes memory from the candidate DIU instead of comparing the old derived value. | M4 |

### Verification

- **Locks**, with the inner function mocked at 0.4 s per run:

  | Scenario | Time | Meaning |
  |---|---|---|
  | Two runs, same containers | 0.8 s | serialized |
  | Two streams, disjoint containers | 0.4 s | parallel |
  | Two copy runs, disjoint containers | 0.8 s | serialized on the copy pipeline |

- **Run tags and cleanup:**
  - run tags are unique (e.g. `1790756294d706cd`);
  - the per-run notebook folder is deleted after a run that raised.
- **Full executor flow**, cloud calls mocked at the lowest level:
  - run status `ok`, `dbx_run_id` has the unique tag, `stages_completed ['C', 'T']`, `rows_written 42`;
  - notebook at `…/run-<tag>/T`, and the folder was cleaned up;
  - a 403 on container creation raises `Creating container 'raw' failed: 403 …`.
- **Retries:** a non-retryable failure makes **1** executor attempt ("failed after 1 attempt(s)"); an ordinary failure still makes 3.
- **Execution plan:** with the user's `custom_settings = {"diu": 8}`, the copy stage is pinned and keeps DIU 8 despite a recommendation of 2; the notebook stage's shuffle goes 8 → 16 as recommended. `state.plan` is unchanged.
- **M4:** the teammate's `test_copy_deadline_changes_diu` passes (13/13). With a real plan, the candidate is no longer rejected at the capacity check.
- **Regressions:**
  - the integration test passes, and `ruff` is clean;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Notes and decisions

- **Serialize rather than rename.** Container names are user-visible (downloads, the monitor), so batch runs with the same containers now take turns rather than getting unique names. That limits throughput for identical pipelines, but guarantees correctness.
- **Open decision: the Cost agent still rarely applies anything.**
  - Its remaining rejection is deliberate policy: *never trade runtime without an explicit deadline*, enforced by the teammate's `test_no_deadline_does_not_allow_slowdown`.
  - The manager passes no deadline, so candidates that save money but run slower (e.g. DIU 2 → 1) are refused.
  - Passing the SLA target (900 s) as the deadline would enable those savings. That's a product decision, left for the user.
- **Workers and node type** would need Databricks job clusters (`new_cluster`) instead of serverless or an existing cluster. That changes compute cost and workspace requirements, so it's out of scope for a logic fix.

---

## Stage 5 — Learned expected duration (the fixed "SLA" limit is removed)

**Commit message:** `feat(performance): replace fixed SLA with a per-pipeline expected duration learned from past runs`

### Why

The project had two separate, unconnected "SLA" (time-limit) values:
- **A hard-coded 900 s prediction target** (`DEFAULT_SLA_TARGET_S`; `manager.predict_performance(sla_target_s=900)`). It only drove an "SLA breach risk" warning, and it wasn't configurable for managed runs.
- **An `SLA_SECONDS` env value for a `sla_breach` anomaly.** It was unset, so the anomaly never fired.

One fixed limit makes no sense when pipelines range from 1 KB test files to 200 MB datasets. At the user's request, the concept was removed entirely and replaced by what each pipeline normally does.

This stage also settles the open decision from stage 4: the Cost agent now gets a deadline, but a learned and bounded one.

### Design

`PerformancePredictionAgent._expected_duration(plan, predictions, predicted_total_s)`:

- **Comparable runs:** same `pipeline_key`; `final_status == "completed"`; `executed`; input size within **0.5–2×** of this run's (a 10× bigger file is not "slow"); and **not** a run where the Cost agent accepted a slower configuration (`cost_slowdown_applied`).
- **Expected duration** = 95th percentile of the last 20 comparable runs, needing at least 3 of them. Otherwise `expected_duration_basis = "insufficient_history"` and nothing is flagged.
- **`slower_than_usual`** = prediction > expected duration.
- **`max_acceptable_s`** (the Cost agent's ceiling) = `min(prediction × 1.2, expected)`, never below the prediction:
  - at most +20% extra runtime;
  - and only within the pipeline's usual range;
  - `None` without history, so the Cost agent keeps its fail-closed "never trade runtime" default.
- **Ratchet guard:** runs where the Cost agent accepted a slowdown are excluded from learning, so accepted slowdowns can't raise the target and permit bigger ones.

### Changes

| File | Change |
|---|---|
| `performance_prediction_agent/performance_agent.py` | **Removed:** `DEFAULT_SLA_TARGET_S`, the `sla_target_s` parameter, and the `sla_breach_risk` / `sla_target_s` output fields. **Added:** `_expected_duration()` and the constants `EXPECTED_MIN_RUNS`, `EXPECTED_WINDOW`, `EXPECTED_SIZE_BAND`, `COST_SLOWDOWN_MARGIN`. **Output fields:** `expected_duration_s`, `expected_duration_basis`, `expected_duration_runs`, `slower_than_usual`, `max_acceptable_s`. `predict()` wraps the ML/formula paths (now `_predict_core`) and adds these fields to every result. |
| `performance_prediction_agent/router.py` | `sla_target_s` request field removed. |
| `central_manager_agent/manager.py` | <ul><li>`predict_performance(state)` has no limit parameter, and logs "slower than usual (this pipeline normally takes ≤Xs)".</li><li>After the learning correction changes the prediction, the expected-duration fields are recomputed, so they describe the corrected number.</li><li>New `_cost_constraints(state)` → `{"deadline_s": max_acceptable_s}` or `{}`, used by `optimize_cost` and `auto_apply_cost_optimization`.</li><li>New `RunState.cost_slowdown_applied`, set when auto-apply lengthened any stage.</li><li>Feedback records `file_size_mb` and `cost_slowdown_applied`.</li></ul> |
| `monitor_agent/services/anomaly_detector.py` | `sla_breach` kind and `_sla_seconds()` / `SLA_SECONDS` removed. `slow_runtime` already compares each run with its own pipeline's p95. |
| `monitor_agent/routers/anomalies.py`, `services/db_service.py` | Kind list and comments updated. |
| `frontend/src/pages/ManagerTab.jsx`, `RunInsights.jsx` | "SLA breach risk" → **"vs usual duration"**: `✔ within usual (≤Xs)` / `⚠ slower (usually ≤Xs)` / `still learning (n/3 runs)`; `—` for runs saved before this change. |
| `frontend/src/pages/PerformancePredictionTab.jsx` | The runtime card shows "Usually ≤ X" (or "still learning"). The "SLA breach risk" card becomes **"Slower than usual"**. The reference table explains how the usual duration is learned. |
| `frontend/src/pages/AnomaliesPage.jsx`, `api.js`, `ResourceTab.jsx` | `sla_breach` filter chip removed; `slaTargetS` argument removed; wording updated. |
| Docs and comments | `unified/README.md` (the `SLA_SECONDS` row), `Performance_Prediction_Agent.md` (fields, API example, tuning section), `docs/RESPONSIBILITIES.md`, `PROJECT_STATUS_REPORT.md`, the teammate's `COST_MODEL_AUDIT.md` and `cost_optimization_agent/README.md` ("SLA validation" → "runtime validation"), Resource Agent comments, the Kaggle notebook note, and a `generate_cost_dataset.py` comment. |
| `scripts/seed_anomalies.py` | The demo `sla_breach` event becomes a `slow_runtime` event. |

**Deliberately left:** `Datasets/.../ORIGINAL_DATASETS_OVERVIEW.md` says the *external raw dataset* has "no SLA tiers". That describes third-party data, not a feature of this project. The earlier review and changelog docs mention SLA as history.

### Verification

- **`_expected_duration`**, with a temp feedback log:

  | Case | Result |
  |---|---|
  | No history | `insufficient_history`, no verdict, no ceiling |
  | 3 runs of 100 / 120 / 110 s, prediction 150 s | expected 120 s, **slower**, ceiling 150 (no slack: already above usual) |
  | Same history, prediction 105 s | within usual, ceiling 120 |
  | 3 runs on 10× bigger input | not comparable → insufficient |
  | Plus 5 cost-slowed runs of 900 s | ignored (expected stays 120) |
  | Failed/aborted runs; another pipeline's runs | ignored |

- **Manager flow, mocked executor and temp logs:**
  - First run (no history): the Cost agent gets `{}`, so no trade-offs.
  - After 3 comparable runs (400/420/410 s): prediction 207 s, expected 420 s → Cost gets `{"deadline_s": 248.4}`, i.e. 207 × 1.2, within the usual range.
  - A simulated slower pick sets `cost_slowdown_applied = True`, and the feedback records `file_size_mb` and `cost_slowdown_applied`.
- **API:** `POST /api/performance-prediction/predict` works without the old field and returns no `sla*` keys.
- **No SLA references are left** in project code or docs (repo-wide grep), apart from the history docs and the external dataset description.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Notes

- **Cost savings become possible, but only after history exists.** A new pipeline needs 3 completed runs of similar input size first; until then the Cost agent stays fail-closed.
- **No saved data changes.** Old saved runs still contain `sla_breach_risk`; the UI now ignores it.

---

## Stage 6 — User-facing bugs

**Commit message:** `fix: reload changed models, detect missing runs by status, handle ragged CSVs, assure and record streams`

### Changes

| Id | Problem | Change |
|---|---|---|
| M5 | ML predictors cached models per process forever: a retrained Performance model wasn't used until restart, and a failed load (file missing or half-written) was cached permanently. | New `model_files.files_signature(*paths)` (mtime + size). The Performance, Resource and Cost predictors store the signature they loaded (`_loaded_sig`) and reload when it changes. Unchanged files keep the cached object: one `stat` per file per prediction. |
| M6 | `ManagerTab` / `ExecutorTab` detected a missing run with `msg.startsWith("404")`, but `api.js` `req()` throws the backend `detail` ("Run not found"), so a missing run polled forever. | `req()` sets `err.status = res.status`; both pages check `e?.status === 404` (and 410). |
| M7 | `/api/schema/detect` returned 500 on a CSV row with fewer fields than the header (`DictReader` fills `None`; `_infer_type` called `.strip()` on it). | `_infer_type` skips `None`. Headers come from `reader.fieldnames` (the real header row), excluding the `None` key `DictReader` uses for extra fields. |
| M8 (rest) | Streams skipped assurance entirely, and stream ticks were never recorded in the monitor or anomaly detector. | `StreamManager.start` runs the deterministic structural assurance (no LLM) and rejects failures; the column check is waived only when the client sent no schema. New `_record_tick` gives each tick's result to `_notify_monitor` (a `Databricks_Streaming_Pipeline` record plus AI analysis) and to `detect_and_store` (per-pipeline metrics and anomalies). Failures there are logged, never fatal. |
| Low | Two drops of the same file in the same second overwrote each other (seconds-only blob name). | Blob name includes an 8-hex uuid. |
| Low | `validate_plan`'s "Planner used fallback" warning and the feedback log's `used_fallback` never fired: the flag was returned *next to* the config, and the plan travels alone. | The planner router also sets `config["used_fallback"]`. |
| Low | The Ollama planner ignored `container_names` sent without `num_containers` (Groq applied them). | The count is taken from the names when not given. |

### Verification

- **M5:**

  | Step | Result |
  |---|---|
  | Cost bundle file missing | unavailable |
  | File appears | available (was cached as failed forever before) |
  | File rewritten | new object loaded |
  | File unchanged | cached object reused |
  | Performance model file touched | reloaded |

- **M6:** the real `req()` from `api.js`, run under Node with a mocked 404 response, gives `message "Run not found"` and `status 404`. The new check matches; the old `startsWith("404")` check did not.
- **M7:** `/api/schema/detect` returns **200** for a short row (`id/name/amount`, 3 rows) and for a row with an extra field (columns `id/name`, the extra value ignored). Before: **500**.
- **Streams** (temp DB):
  - an unknown column in the filter → rejected ("Column references: …");
  - no schema but a bad `execution_order` → rejected ("Stage ordering: …");
  - no schema and otherwise valid → starts;
  - a mocked tick creates a monitor row (`Databricks_Streaming_Pipeline`, `Succeeded`) and 1 `run_metrics` row;
  - 3 same-second drops of `same.csv` → 3 distinct blob names.
- **Planner:**
  - the Ollama backend, with its HTTP call mocked, applies `["landing","clean","gold"]` given without a count;
  - `/api/planner/plan` returns `config.used_fallback` matching the top-level flag, both `True` and `False`.
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Mistakes during this stage

- **Wrong attribute name.** My first version of `_structural_failures` read `c.name`, but `assurance_agent.result.CheckResult` calls the field `check`. The stream test raised `AttributeError`. The "unknown column" case had passed only because it failed before reaching that attribute. Fixed and re-tested.
- **Model timestamp touched.** The M5 test updated the timestamp of the real `performance_prediction_agent/models/duration_regressor.pkl` (via `os.utime`) to prove the reload. The contents are unchanged, and `.pkl` files are gitignored.

---

## Stage 7 — Remaining low-severity items

**Commit message:** `fix: order-aware assurance, accurate analytics, dedupe monitor analyses, fair retrain gate, remove dead cost code`

### Changes

| Finding | Change |
|---|---|
| **Combined analytics.** The duration ratio included failed/aborted runs; `feasible_plans` counted any run with a `complexity`; `anomalies_detected` used the legacy Groq log; manager runs were windowed but feedback wasn't. | `combined.py`: feedback is limited to the same run window; the duration ratio uses only successful, executed runs ("completed", or the legacy "feedback" status); `feasible_plans` / `infeasible_plans` count the new `resource_feasible` value, which the manager now records; `anomalies_detected` counts `anomaly_events` and `ai_slow_run_verdicts` counts the legacy log. |
| **Assurance column check ignored order.** A transform could use a column created later in the same stage, and a filter could use an aggregation alias. | `assurance_agent/structural.py` checks in execution order (transforms one at a time → filter → aggregation inputs; aliases become known only after that). Violations say where: `(transformation)`, `(filter)` or `(aggregation)`. |
| **Contention spill.** `resolve_contention` merged a spilled stage into the next group, which could depend on it. | The spilled stage gets its own group right after the current one. |
| **Duplicate monitor analyses.** The poll loop, `sync_historical` and backfill could analyze the same run twice. Backfill asked ADF about `dbx-` Databricks ids, which always failed. | `MonitorService._in_flight` claims a `run_id` before fetching or analyzing (`_analyze` → `_analyze_locked`). Backfill analyzes `dbx-` records from their stored `raw_json` instead of calling ADF. |
| **Learning cycle.** It ran synchronously on the event loop, and the `runs_since_cycle` counter was a non-atomic read-modify-write. | The manager runs `on_run_recorded` via `asyncio.to_thread`; `learning_agent._CYCLE_LOCK` serializes counting and the cycle trigger. |
| **Retrain deploy gate.** It compared MAE across different held-out sets. | `run_training.py` writes its held-out set to `data/holdout_test.csv`. `RetrainingManager._old_model_mae` scores the snapshotted old model on those same rows, and the gate compares like with like (the stored old MAE is kept as `mae_before_stored`). If the old model can't be scored there (different feature set), the new one is kept. |
| **Dead cost code.** Nine methods were unused since the fail-closed change. | Removed `_apply_cluster_downsize`, `_apply_node_downgrade`, `_apply_shuffle_tuning`, `_enforce_constraints_single` and `_suggest_*` ×5, plus the four constants only they used (`UTILIZATION_LOW_THRESHOLD`, `TINY_STAGE_THRESHOLD_S`, `OFF_PEAK_DISCOUNT`, `MERGE_SAVING_FACTOR`). A reference check across `unified/` and the teammate's root scripts showed 0 uses. |
| **Single-worker assumption.** `mark_interrupted_manager_runs` would fail another worker's live runs. | **Documented, not changed:** the `db_service` docstring and `unified/README.md` now say to run one server process. Run state, the executor's resource locks, streams and the monitor poll loop are all per-process, so real multi-worker support would need shared state (a database or Redis), which is a larger design change. |

### Verification

- **Analytics** (real DB, through the API): 10 duration samples, average ratio 0.55, failed runs excluded; `anomalies_detected 2` (events) and `ai_slow_run_verdicts 5`.
- **Assurance:**
  - all real plans still pass the column check;
  - a transform using a column defined later is flagged "unknown column 'z' (transformation)";
  - a filter on an aggregation alias is flagged "unknown column 'total' (filter)";
  - a correctly ordered stage passes.
- **Contention:** groups `[a,b,c,e]` then `[d]` become `[b,a,c]`, `[e]`, `[d]`: the spilled `e` sits alone, before `d`.
- **Monitor** (temp DB, mocked Groq/ADF):
  - three concurrent analyses of one run make 1 Groq call;
  - a `dbx-` record is backfilled and saved without ADF.
- **Learning:** 20 concurrent `on_run_recorded` calls give `runs_since_cycle = 20`.
- **Deploy gate:**
  - the old model's MAE is computed on a new held-out file (125.3 on synthetic rows);
  - a mismatched feature set returns `None` (no comparison);
  - a **copy** of `run_training.py` run in a temp directory (real models untouched) wrote `holdout_test.csv` with 21,000 rows × (19 features + `actual_duration_s`).
- **Regressions:**
  - the integration test passes, and so do the teammate's 13 cost tests;
  - `ruff` is clean, and the frontend builds;
  - the backend has no tracebacks, and all 25 GET endpoints return 200.

### Mistakes during this stage

- **Legacy status missed.** The first analytics filter accepted only `final_status == "completed"`, which dropped every legacy success: older records use `"feedback"`, from before bug #8 was fixed. That gave 0 duration samples. Now both are accepted. I checked the other new filters: `_expected_duration` requires `"completed"` but also needs `pipeline_key`, which legacy records don't have, so it's unaffected.
- **Bad test harness, twice.** One `asyncio.sleep` patch called itself recursively, and one contention scenario never actually spilled. Both were test bugs, fixed before relying on the results.

---

## All findings — where each was fixed

| Finding | Stage | Finding | Stage |
|---|---|---|---|
| H1 container-name injection | 1 | M1 resource factor convergence | 3 |
| H2 schema missing from plan | 2 | M2 learning factor convergence | 3 |
| H3 flat schema from UI | 2 | M3 whole-run duration | 3 |
| H4 aggregation count 0 | 2 | M4 copy-stage memory check | 4 |
| H5 failed runs in Resource feedback | 3 | M5 model cache never refreshed | 6 |
| H6 actual cost ignored duration | 3 | M6 frontend 404 detection | 6 |
| H7 no concurrent-run isolation | 4 | M7 ragged CSV 500 | 6 |
| H8 resource settings not executed | 4 (DIU/shuffle; workers/node advisory) | M8 streams unvalidated/unrecorded | 1 + 6 |
| Fixed "SLA" time limit | 5 (replaced by learned duration) | M9 cross-pipeline aborts | 3 |
| Low items | 4, 6, 7 (multi-worker: documented) | | |

### Honest limits that remain

- **Worker count and node type are still advisory.** Applying them needs Databricks job clusters (stage 4).
- **One server process only.** This is documented, not changed (stage 7).
- **The Cost agent needs history.** It only trades runtime for savings once a pipeline has 3 comparable completed runs (stage 5, by design).
- **Frontend changes were checked by build and by running `req()` under Node, not by clicking through a browser.** Cloud behaviour was tested with mocks; no real Azure/Databricks runs were made during these stages.
